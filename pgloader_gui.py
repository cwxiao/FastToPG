import json
import os
import queue
import re
import subprocess
import threading
import time
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
from collections import deque
from tkinter import ttk, messagebox, filedialog
from urllib.parse import unquote, urlparse

DEFAULT_CONFIG = "pgloader_tool.json"
SOURCE_TYPES = ["mysql", "mssql", "sqlite", "pgsql", "redshift", "file"]


def should_skip_table(table: str, keywords: list[str]) -> bool:
    name = table.lower()
    for keyword in keywords:
        token = keyword.strip().lower()
        if token and token in name:
            return True
    return False


def is_datax_key_log(line: str) -> bool:
    text = line.strip()
    if not text:
        return False
    keep_tokens = [
        "ERROR",
        "WARN",
        "jobContainer starts job",
        "completed successfully",
        "Total ",
        "Percentage",
        "DataX jobId",
    ]
    return any(token in text for token in keep_tokens)


def is_pgloader_error_log(line: str) -> bool:
    text = line.strip()
    if not text:
        return False
    tokens = [
        " FATAL ",
        " ERROR ",
        "KABOOM!",
        "ESRAP-PARSE-ERROR",
        "Failed to create the schema",
    ]
    return any(token in text for token in tokens)


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(path: str, config: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def run_command(cmd: list, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, env=env, capture_output=True, text=True)


def get_total_tables(mysql_container: str, user: str, password: str, db: str) -> int:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
        "-e",
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='{db}';",
    ]
    result = run_command(cmd)
    if result.returncode != 0:
        return 0
    output = (result.stdout or "").strip().splitlines()
    if not output:
        return 0
    try:
        return int(output[0].strip())
    except ValueError:
        return 0


def render_load_file(template_path: str, output_path: str, replacements: dict) -> None:
    with open(template_path, "r", encoding="utf-8") as f:
        content = f.read()

    for key, value in replacements.items():
        content = content.replace(f"{{{{{key}}}}}", value)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)


def build_pgloader_command(
    workspace: str,
    load_file: str,
    image: str,
    env: dict,
) -> list:
    cmd = ["docker", "run", "--rm"]
    for key, value in env.items():
        cmd.extend(["-e", f"{key}={value}"])
    cmd.extend(["-v", f"{workspace}:/pgloader", image])
    cmd.extend([
        "sh",
        "-c",
        f"pgloader --on-error-stop /pgloader/{load_file}",
    ])
    return cmd


def parse_db_uri(uri: str) -> dict:
    parsed = urlparse(uri)
    if not parsed.scheme:
        raise ValueError(f"Invalid URI: {uri}")
    return {
        "scheme": parsed.scheme,
        "host": parsed.hostname or "",
        "port": parsed.port or 0,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": (parsed.path or "").lstrip("/"),
    }


def clear_target_public_tables(db: str, config: dict) -> tuple[int, str]:
    target_cfg = config.get("target", {})
    target_uri = target_cfg.get("uri", "").replace("{{DB_NAME}}", db)
    target = parse_db_uri(target_uri)

    if target.get("scheme") not in ("postgresql", "pgsql", "postgres"):
        return 1, f"Skip clear public: unsupported target scheme {target.get('scheme')}\n"

    sql = (
        "DO $$ "
        "DECLARE r record; "
        "BEGIN "
        "FOR r IN SELECT tablename FROM pg_tables WHERE schemaname='public' LOOP "
        "EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', r.tablename); "
        "END LOOP; "
        "END $$;"
    )

    pg_user = str(target.get("user", ""))
    pg_db = str(target.get("database", ""))
    pg_host = str(target.get("host", ""))
    pg_port = str(target.get("port", 5432) or 5432)
    psql_container = (target_cfg.get("psql_container") or "postgres16").strip()

    container_cmd = [
        "docker",
        "exec",
        "-e",
        f"PGPASSWORD={str(target.get('password', ''))}",
        psql_container,
        "psql",
        "-h",
        pg_host,
        "-p",
        pg_port,
        "-U",
        pg_user,
        "-d",
        pg_db,
        "-v",
        "ON_ERROR_STOP=1",
        "-q",
        "-c",
        sql,
    ]
    container_result = run_command(container_cmd)
    if container_result.returncode == 0:
        return 0, (container_result.stdout or "") + (container_result.stderr or "")

    psql_cmd = target_cfg.get("psql", "psql")
    local_cmd = [
        psql_cmd,
        "-h",
        pg_host,
        "-p",
        pg_port,
        "-U",
        pg_user,
        "-d",
        pg_db,
        "-v",
        "ON_ERROR_STOP=1",
        "-q",
        "-c",
        sql,
    ]

    cmd_env = os.environ.copy()
    password = str(target.get("password", ""))
    if password:
        cmd_env["PGPASSWORD"] = password

    result = run_command(local_cmd, env=cmd_env)
    output = (
        (container_result.stdout or "")
        + (container_result.stderr or "")
        + (result.stdout or "")
        + (result.stderr or "")
    )
    return result.returncode, output


def get_mysql_tables(mysql_container: str, user: str, password: str, db: str) -> list[str]:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
        "-e",
        (
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema='{db}' AND table_type='BASE TABLE' ORDER BY table_name;"
        ),
    ]
    result = run_command(cmd)
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def get_mysql_columns(mysql_container: str, user: str, password: str, db: str, table: str) -> list[str]:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
        "-e",
        (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='{db}' AND table_name='{table}' ORDER BY ordinal_position;"
        ),
    ]
    result = run_command(cmd)
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def get_mysql_split_pk(mysql_container: str, user: str, password: str, db: str, table: str) -> str | None:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
        "-e",
        (
            "SELECT k.column_name, c.data_type FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage k "
            "ON tc.constraint_name = k.constraint_name "
            "AND tc.table_schema = k.table_schema "
            "AND tc.table_name = k.table_name "
            "JOIN information_schema.columns c "
            "ON c.table_schema = k.table_schema "
            "AND c.table_name = k.table_name "
            "AND c.column_name = k.column_name "
            f"WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema='{db}' AND tc.table_name='{table}' "
            "ORDER BY k.ordinal_position;"
        ),
    ]
    result = run_command(cmd)
    if result.returncode != 0:
        return None
    rows = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    if len(rows) != 1:
        return None
    parts = rows[0].split("\t")
    if len(parts) < 2:
        return None
    col_name = parts[0].strip()
    data_type = parts[1].strip().lower()
    numeric_types = {
        "tinyint",
        "smallint",
        "mediumint",
        "int",
        "integer",
        "bigint",
        "decimal",
        "numeric",
    }
    if data_type not in numeric_types:
        return None
    return col_name


def get_mysql_databases(mysql_container: str, user: str, password: str) -> list[str]:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
        "-e",
        (
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('information_schema','mysql','performance_schema','sys') "
            "ORDER BY schema_name;"
        ),
    ]
    result = run_command(cmd)
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def get_target_databases(target_uri: str, psql_container: str) -> list[str]:
    target = parse_db_uri(target_uri)
    cmd = [
        "docker",
        "exec",
        "-e",
        f"PGPASSWORD={target.get('password', '')}",
        psql_container,
        "psql",
        "-h",
        str(target.get("host", "")),
        "-p",
        str(target.get("port", 5432) or 5432),
        "-U",
        str(target.get("user", "")),
        "-d",
        str(target.get("database", "")),
        "-t",
        "-A",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;",
    ]
    result = run_command(cmd)
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def mysql_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def pg_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_datax_job(
    workspace: str,
    db: str,
    table: str,
    columns: list[str],
    source_uri: str,
    target_uri: str,
    datax_cfg: dict,
    split_pk: str | None = None,
) -> str:
    source = parse_db_uri(source_uri)
    target = parse_db_uri(target_uri)

    channel = int(datax_cfg.get("channel", 2))
    batch_size = int(datax_cfg.get("batch_size", 2000))
    job_dir = datax_cfg.get("job_dir", ".datax_jobs")

    source_jdbc = f"jdbc:mysql://{source['host']}:{source['port']}/{source['database']}"
    mysql_jdbc_params = (datax_cfg.get("mysql_jdbc_params") or "useSSL=false").strip()
    if mysql_jdbc_params:
        sep = "&" if "?" in source_jdbc else "?"
        source_jdbc = f"{source_jdbc}{sep}{mysql_jdbc_params}"
    target_jdbc = f"jdbc:postgresql://{target['host']}:{target['port']}/{target['database']}"
    reader_columns = [mysql_ident(col) for col in columns]
    writer_columns = [
        pg_ident(col.lower() if bool(datax_cfg.get("target_column_lowercase", True)) else col)
        for col in columns
    ]
    target_table = table.lower() if bool(datax_cfg.get("target_table_lowercase", True)) else table

    job = {
        "job": {
            "setting": {
                "speed": {"channel": channel},
                "errorLimit": {"record": 0, "percentage": 0.02},
            },
            "content": [
                {
                    "reader": {
                        "name": "mysqlreader",
                        "parameter": {
                            "username": source["user"],
                            "password": source["password"],
                            "column": reader_columns,
                            "connection": [
                                {
                                    "table": [mysql_ident(table)],
                                    "jdbcUrl": [source_jdbc],
                                }
                            ],
                        },
                    },
                    "writer": {
                        "name": "postgresqlwriter",
                        "parameter": {
                            "username": target["user"],
                            "password": target["password"],
                            "column": writer_columns,
                            "connection": [
                                {
                                    "table": [pg_ident(target_table)],
                                    "jdbcUrl": target_jdbc,
                                }
                            ],
                            "batchSize": batch_size,
                        },
                    },
                }
            ],
        }
    }

    if split_pk:
        job["job"]["content"][0]["reader"]["parameter"]["splitPk"] = split_pk

    job_folder = os.path.join(workspace, job_dir)
    os.makedirs(job_folder, exist_ok=True)
    job_file = os.path.join(job_folder, f"{db}.{table}.json")
    with open(job_file, "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False, indent=2)
    return job_file


def build_datax_command(job_file: str, datax_cfg: dict) -> list[str]:
    datax_home = (datax_cfg.get("home") or "").strip()
    if not datax_home:
        raise ValueError("DataX 配置缺少 datax.home")

    datax_py = os.path.join(datax_home, "bin", "datax.py")
    python_cmd = (datax_cfg.get("python") or "python").strip()
    cmd = [python_cmd, datax_py]

    jvm_opts = (datax_cfg.get("jvm") or "").strip()
    if jvm_opts:
        cmd.extend(["-j", jvm_opts])

    loglevel = (datax_cfg.get("loglevel") or "").strip()
    if loglevel:
        cmd.extend(["--loglevel", loglevel])

    cmd.append(job_file)
    return cmd


def is_cleanup_jobs_on_finish(datax_cfg: dict) -> bool:
    return bool(datax_cfg.get("cleanup_jobs_on_finish", datax_cfg.get("cleanup_jobs_on_success", True)))


def cleanup_datax_jobs_for_db(workspace: str, db: str, datax_cfg: dict) -> None:
    job_dir = datax_cfg.get("job_dir", ".datax_jobs")
    folder = os.path.join(workspace, job_dir)
    if not os.path.isdir(folder):
        return
    pattern = os.path.join(folder, f"{db}.*.json")
    for path in glob.glob(pattern):
        try:
            os.remove(path)
        except OSError:
            pass


def cleanup_datax_job_file(path: str) -> None:
    try:
        if os.path.isfile(path):
            os.remove(path)
    except OSError:
        pass


def cleanup_empty_datax_job_dir(workspace: str, datax_cfg: dict) -> None:
    job_dir = datax_cfg.get("job_dir", ".datax_jobs")
    folder = os.path.join(workspace, job_dir)
    if not os.path.isdir(folder):
        return
    try:
        if not os.listdir(folder):
            os.rmdir(folder)
    except OSError:
        pass


def cleanup_pgloader_rendered_file(path: str) -> None:
    try:
        if os.path.isfile(path):
            os.remove(path)
    except OSError:
        pass


def cleanup_pgloader_rendered_files_for_db(workspace: str, db: str) -> None:
    pattern = os.path.join(workspace, f".pgloader_rendered_{db}*.load")
    for path in glob.glob(pattern):
        cleanup_pgloader_rendered_file(path)


def cleanup_old_logs(workspace: str, log_dirs: list[str], retention_days: int) -> None:
    if retention_days <= 0:
        return
    cutoff = time.time() - (retention_days * 24 * 60 * 60)
    for path in log_dirs:
        if not isinstance(path, str) or not path.strip():
            continue
        folder = path if os.path.isabs(path) else os.path.join(workspace, path)
        if not os.path.isdir(folder):
            continue
        for root, dirs, files in os.walk(folder, topdown=False):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    if os.path.getmtime(file_path) < cutoff:
                        os.remove(file_path)
                except OSError:
                    pass
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.listdir(dir_path):
                        os.rmdir(dir_path)
                except OSError:
                    pass


def cleanup_datax_logs_by_retention(workspace: str, datax_cfg: dict) -> None:
    retention_days_raw = datax_cfg.get("log_retention_days", 7)
    try:
        retention_days = int(retention_days_raw)
    except (TypeError, ValueError):
        retention_days = 7
    log_dirs_raw = datax_cfg.get("log_dirs", ["datax/datax/log", "datax/datax/log_perf"])
    if not isinstance(log_dirs_raw, list):
        return
    log_dirs = [str(item) for item in log_dirs_raw if str(item).strip()]
    cleanup_old_logs(workspace, log_dirs, retention_days)


class PgloaderGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("FastDBConvert")
        self.geometry("1680x980")
        self.minsize(1400, 820)
        self.resizable(True, True)

        self.workspace = os.path.abspath(os.path.dirname(__file__))
        self.queue: "queue.Queue[tuple]" = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.current_process: subprocess.Popen | None = None
        self.active_processes: set[subprocess.Popen] = set()
        self.stop_event = threading.Event()
        self.overall_total_tables = 0
        self.overall_processed_tables = 0
        self.start_time = 0.0
        self.last_progress_time = 0.0
        self.last_progress_count = 0
        self.max_log_lines = 4000
        self.selected_dbs: list[str] = []
        self.info_refresh_after_id: str | None = None
        self.fallback_databases: list[str] = []

        self.config_path = tk.StringVar(value=os.path.join(self.workspace, DEFAULT_CONFIG))
        self.load_template = tk.StringVar()
        self.source_type = tk.StringVar(value="mysql")
        self.source_uri = tk.StringVar()
        self.target_uri = tk.StringVar()
        self.mysql_container = tk.StringVar()
        self.mysql_user = tk.StringVar()
        self.mysql_password = tk.StringVar()
        self.pgloader_image = tk.StringVar()
        self.show_output = tk.BooleanVar(value=True)
        self.datax_enabled = tk.BooleanVar(value=False)
        self.datax_home = tk.StringVar()
        self.datax_python = tk.StringVar(value="python")
        self.datax_source_uri = tk.StringVar()
        self.datax_channel = tk.StringVar(value="2")
        self.datax_batch_size = tk.StringVar(value="2000")
        self.datax_table_parallelism = tk.StringVar(value="30")
        self.datax_log_retention_days = tk.StringVar(value="7")
        self.datax_verbose_log = tk.BooleanVar(value=False)

        self._build_ui()
        self._load_config_safe()
        self._poll_queue()

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=10)
        top.pack(fill=tk.BOTH, expand=True)

        tabs = ttk.Notebook(top)
        tabs.pack(fill=tk.BOTH, expand=True)

        home_tab = ttk.Frame(tabs)
        config_tab = ttk.Frame(tabs)
        tabs.add(home_tab, text="主页")
        tabs.add(config_tab, text="配置")

        home_pane = ttk.Panedwindow(home_tab, orient=tk.HORIZONTAL)
        home_pane.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(home_pane)
        center = ttk.Frame(home_pane)
        right = ttk.Frame(home_pane)
        home_pane.add(left, weight=1)
        home_pane.add(center, weight=3)
        home_pane.add(right, weight=1)

        db_frame = ttk.LabelFrame(left, text="源数据库")
        db_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.db_list = tk.Listbox(db_frame, selectmode=tk.EXTENDED)
        self.db_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.db_list.bind("<<ListboxSelect>>", self._on_db_select)
        db_scroll = ttk.Scrollbar(db_frame, command=self.db_list.yview)
        db_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.db_list.configure(yscrollcommand=db_scroll.set)

        db_buttons = ttk.Frame(left)
        db_buttons.pack(fill=tk.X, padx=5, pady=5)
        self.db_refresh_btn = ttk.Button(db_buttons, text="刷新数据库", command=self._refresh_databases)
        self.db_refresh_btn.pack(side=tk.LEFT)

        log_frame = ttk.LabelFrame(center, text="日志")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.log_text = tk.Text(log_frame, wrap="none")
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll_y = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        log_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        log_scroll_x = ttk.Scrollbar(center, orient=tk.HORIZONTAL, command=self.log_text.xview)
        log_scroll_x.pack(fill=tk.X, padx=5)
        self.log_text.configure(yscrollcommand=log_scroll_y.set, xscrollcommand=log_scroll_x.set)

        target_frame = ttk.LabelFrame(right, text="目标数据库")
        target_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.target_info_text = tk.Text(target_frame, height=12, wrap="word")
        self.target_info_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        target_info_scroll = ttk.Scrollbar(target_frame, command=self.target_info_text.yview)
        target_info_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.target_info_text.configure(yscrollcommand=target_info_scroll.set, state=tk.DISABLED)

        bottom = ttk.Frame(home_tab)
        bottom.pack(fill=tk.X, padx=5, pady=5)

        self.run_structure_button = ttk.Button(bottom, text="同步结构", command=lambda: self._run_selected("structure"))
        self.run_structure_button.pack(side=tk.LEFT)
        self.run_data_button = ttk.Button(bottom, text="同步数据", command=lambda: self._run_selected("data"))
        self.run_data_button.pack(side=tk.LEFT, padx=5)
        self.stop_button = ttk.Button(bottom, text="停止", command=self._stop_run, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        self.progress_label = ttk.Label(bottom, text="空闲")
        self.progress_label.pack(side=tk.LEFT, padx=10)

        self.progress = ttk.Progressbar(bottom, length=240, mode="indeterminate")
        self.progress.pack(side=tk.LEFT, padx=5)

        self.eta_label = ttk.Label(bottom, text="预计剩余: --")
        self.eta_label.pack(side=tk.LEFT, padx=10)

        self.size_label = ttk.Label(bottom, text="已选数据库大小: --")
        self.size_label.pack(side=tk.LEFT, padx=10)

        cfg_frame = ttk.LabelFrame(config_tab, text="配置文件")
        cfg_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(cfg_frame, text="配置文件").grid(row=0, column=0, sticky="w")
        self.config_entry = ttk.Entry(cfg_frame, textvariable=self.config_path, width=80)
        self.config_entry.grid(row=0, column=1, sticky="we", padx=5)
        self.config_browse_btn = ttk.Button(cfg_frame, text="选择", command=self._browse_config)
        self.config_browse_btn.grid(row=0, column=2, padx=5)
        self.config_load_btn = ttk.Button(cfg_frame, text="加载", command=self._load_config_safe)
        self.config_load_btn.grid(row=0, column=3, padx=5)
        self.config_save_btn = ttk.Button(cfg_frame, text="保存", command=self._save_config_safe)
        self.config_save_btn.grid(row=0, column=4, padx=5)

        cfg_frame.columnconfigure(1, weight=1)

        settings = ttk.LabelFrame(config_tab, text="同步配置")
        settings.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        row = 0
        ttk.Label(settings, text="模板文件").grid(row=row, column=0, sticky="w")
        self.load_template_entry = ttk.Entry(settings, textvariable=self.load_template)
        self.load_template_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="源类型").grid(row=row, column=0, sticky="w")
        self.source_type_combo = ttk.Combobox(settings, values=SOURCE_TYPES, textvariable=self.source_type, state="readonly")
        self.source_type_combo.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="源连接 URI").grid(row=row, column=0, sticky="w")
        self.source_uri_entry = ttk.Entry(settings, textvariable=self.source_uri)
        self.source_uri_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="目标连接 URI").grid(row=row, column=0, sticky="w")
        self.target_uri_entry = ttk.Entry(settings, textvariable=self.target_uri)
        self.target_uri_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="MySQL 容器").grid(row=row, column=0, sticky="w")
        self.mysql_container_entry = ttk.Entry(settings, textvariable=self.mysql_container)
        self.mysql_container_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="MySQL 用户").grid(row=row, column=0, sticky="w")
        self.mysql_user_entry = ttk.Entry(settings, textvariable=self.mysql_user)
        self.mysql_user_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="MySQL 密码").grid(row=row, column=0, sticky="w")
        self.mysql_password_entry = ttk.Entry(settings, textvariable=self.mysql_password, show="*")
        self.mysql_password_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="pgloader 镜像").grid(row=row, column=0, sticky="w")
        self.pgloader_image_entry = ttk.Entry(settings, textvariable=self.pgloader_image)
        self.pgloader_image_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="DataX 启用").grid(row=row, column=0, sticky="w")
        self.datax_enabled_check = ttk.Checkbutton(settings, variable=self.datax_enabled)
        self.datax_enabled_check.grid(row=row, column=1, sticky="w", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="DataX Home").grid(row=row, column=0, sticky="w")
        self.datax_home_entry = ttk.Entry(settings, textvariable=self.datax_home)
        self.datax_home_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="DataX Python").grid(row=row, column=0, sticky="w")
        self.datax_python_entry = ttk.Entry(settings, textvariable=self.datax_python)
        self.datax_python_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="DataX 源URI").grid(row=row, column=0, sticky="w")
        self.datax_source_uri_entry = ttk.Entry(settings, textvariable=self.datax_source_uri)
        self.datax_source_uri_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="DataX 并发通道").grid(row=row, column=0, sticky="w")
        self.datax_channel_entry = ttk.Entry(settings, textvariable=self.datax_channel)
        self.datax_channel_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="DataX 批大小").grid(row=row, column=0, sticky="w")
        self.datax_batch_size_entry = ttk.Entry(settings, textvariable=self.datax_batch_size)
        self.datax_batch_size_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="DataX 表并行").grid(row=row, column=0, sticky="w")
        self.datax_table_parallelism_entry = ttk.Entry(settings, textvariable=self.datax_table_parallelism)
        self.datax_table_parallelism_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="日志保留天数").grid(row=row, column=0, sticky="w")
        self.datax_log_retention_days_entry = ttk.Entry(settings, textvariable=self.datax_log_retention_days)
        self.datax_log_retention_days_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="DataX 详细日志").grid(row=row, column=0, sticky="w")
        self.datax_verbose_log_check = ttk.Checkbutton(settings, variable=self.datax_verbose_log)
        self.datax_verbose_log_check.grid(row=row, column=1, sticky="w", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="环境变量 (JSON)").grid(row=row, column=0, sticky="nw")
        self.env_text = tk.Text(settings, height=6)
        self.env_text.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(settings, text="Load 脚本内容").grid(row=row, column=0, sticky="nw")
        self.load_text = tk.Text(settings, height=8)
        self.load_text.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        self.load_file_btns = ttk.Frame(settings)
        self.load_file_btns.grid(row=row, column=1, sticky="w", padx=5, pady=2)
        self.load_reload_btn = ttk.Button(self.load_file_btns, text="载入模板", command=self._reload_template)
        self.load_reload_btn.pack(side=tk.LEFT)
        self.load_save_btn = ttk.Button(self.load_file_btns, text="保存模板", command=self._save_template)
        self.load_save_btn.pack(side=tk.LEFT, padx=5)
        row += 1

        settings.columnconfigure(1, weight=1)

    def _browse_config(self) -> None:
        path = filedialog.askopenfilename(initialdir=self.workspace, filetypes=[("JSON", "*.json")])
        if path:
            self.config_path.set(path)

    def _load_config_safe(self) -> None:
        try:
            config = load_config(self.config_path.get())
        except Exception as exc:
            messagebox.showerror("错误", f"加载配置失败: {exc}")
            return

        self.fallback_databases = [db for db in config.get("databases", []) if isinstance(db, str)]
        self.db_list.delete(0, tk.END)
        self.size_label.configure(text="已选数据库大小: --")

        self.load_template.set(config.get("load_template", "mysql_to_pg.load"))
        source_cfg = config.get("source", {})
        target_cfg = config.get("target", {})
        self.source_type.set(source_cfg.get("type", "mysql"))
        self.source_uri.set(source_cfg.get("uri", ""))
        self.target_uri.set(target_cfg.get("uri", ""))
        mysql_cfg = config.get("mysql", {})
        self.mysql_container.set(mysql_cfg.get("container", "mysql8"))
        self.mysql_user.set(mysql_cfg.get("user", "root"))
        self.mysql_password.set(mysql_cfg.get("password", ""))

        pgloader_cfg = config.get("pgloader", {})
        self.pgloader_image.set(pgloader_cfg.get("image", "dimitri/pgloader:v3.6.7"))
        self.show_output.set(True)
        env = pgloader_cfg.get("env", {})
        self.env_text.delete("1.0", tk.END)
        self.env_text.insert(tk.END, json.dumps(env, ensure_ascii=False, indent=2))

        datax_cfg = config.get("datax", {})
        self.datax_enabled.set(bool(datax_cfg.get("enabled", False)))
        self.datax_home.set(datax_cfg.get("home", ""))
        self.datax_python.set(datax_cfg.get("python", "python"))
        self.datax_source_uri.set(datax_cfg.get("source_uri", ""))
        self.datax_channel.set(str(datax_cfg.get("channel", 2)))
        self.datax_batch_size.set(str(datax_cfg.get("batch_size", 2000)))
        self.datax_table_parallelism.set(str(datax_cfg.get("table_parallelism", 30)))
        self.datax_log_retention_days.set(str(datax_cfg.get("log_retention_days", 7)))
        self.datax_verbose_log.set(bool(datax_cfg.get("show_output", True)) and not bool(datax_cfg.get("compact_log", True)))

        self._reload_template()
        self._refresh_databases()

    def _save_config_safe(self) -> None:
        try:
            env = json.loads(self.env_text.get("1.0", tk.END).strip() or "{}")
        except json.JSONDecodeError as exc:
            messagebox.showerror("错误", f"环境变量 JSON 无效: {exc}")
            return

        try:
            datax_channel = int(self.datax_channel.get().strip() or "2")
            datax_batch_size = int(self.datax_batch_size.get().strip() or "2000")
            datax_table_parallelism = int(self.datax_table_parallelism.get().strip() or "30")
            datax_log_retention_days = int(self.datax_log_retention_days.get().strip() or "7")
        except ValueError:
            messagebox.showerror("错误", "DataX 并发通道、批大小、表并行数、日志保留天数必须是整数。")
            return

        if datax_table_parallelism < 1:
            messagebox.showerror("错误", "DataX 表并行数必须大于等于 1。")
            return
        if datax_log_retention_days < 1:
            messagebox.showerror("错误", "日志保留天数必须大于等于 1。")
            return

        config = {
            "databases": list(self.db_list.get(0, tk.END)),
            "load_template": self.load_template.get().strip(),
            "source": {
                "type": self.source_type.get().strip(),
                "uri": self.source_uri.get().strip(),
            },
            "target": {
                "uri": self.target_uri.get().strip(),
                "psql_container": "postgres16",
            },
            "mysql": {
                "container": self.mysql_container.get().strip(),
                "user": self.mysql_user.get().strip(),
                "password": self.mysql_password.get(),
            },
            "pgloader": {
                "image": self.pgloader_image.get().strip(),
                "env": env,
                "clear_public_before_sync": True,
                "show_output": True,
            },
            "datax": {
                "enabled": bool(self.datax_enabled.get()),
                "home": self.datax_home.get().strip(),
                "python": self.datax_python.get().strip() or "python",
                "jvm": "-Xms2g -Xmx6g -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError",
                "loglevel": "warn",
                "source_uri": self.datax_source_uri.get().strip(),
                "mysql_jdbc_params": "useSSL=false",
                "target_table_lowercase": True,
                "target_column_lowercase": True,
                "table_parallelism": datax_table_parallelism,
                "channel": datax_channel,
                "batch_size": datax_batch_size,
                "job_dir": ".datax_jobs",
                "cleanup_jobs_on_finish": True,
                "show_output": True,
                "compact_log": not bool(self.datax_verbose_log.get()),
                "exclude_table_keywords": [],
                "log_retention_days": datax_log_retention_days,
                "log_dirs": ["datax/datax/log", "datax/datax/log_perf"],
                "env": {},
            },
        }

        try:
            save_config(self.config_path.get(), config)
        except Exception as exc:
            messagebox.showerror("错误", f"保存配置失败: {exc}")
            return

        messagebox.showinfo("已保存", "配置已保存。")

    def _add_db(self) -> None:
        db = tk.simpledialog.askstring("新增数据库", "数据库名称:")
        if db:
            self.db_list.insert(tk.END, db.strip())

    def _remove_db(self) -> None:
        for idx in reversed(self.db_list.curselection()):
            self.db_list.delete(idx)

    def _run_selected(self, mode: str) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("运行中", "当前正在同步，请先完成或停止。")
            return

        if mode == "structure" and not self._confirm_structure_sync():
            return

        dbs = [self.db_list.get(i) for i in self.db_list.curselection()]
        if not dbs:
            messagebox.showinfo("请选择", "请至少选择一个数据库。")
            return

        try:
            env = json.loads(self.env_text.get("1.0", tk.END).strip() or "{}")
        except json.JSONDecodeError as exc:
            messagebox.showerror("错误", f"环境变量 JSON 无效: {exc}")
            return

        try:
            datax_channel = int(self.datax_channel.get().strip() or "2")
            datax_batch_size = int(self.datax_batch_size.get().strip() or "2000")
            datax_table_parallelism = int(self.datax_table_parallelism.get().strip() or "30")
            datax_log_retention_days = int(self.datax_log_retention_days.get().strip() or "7")
        except ValueError:
            messagebox.showerror("错误", "DataX 并发通道、批大小、表并行数、日志保留天数必须是整数。")
            return

        if datax_table_parallelism < 1:
            messagebox.showerror("错误", "DataX 表并行数必须大于等于 1。")
            return
        if datax_log_retention_days < 1:
            messagebox.showerror("错误", "日志保留天数必须大于等于 1。")
            return

        config = {
            "databases": dbs,
            "load_template": self.load_template.get().strip(),
            "source": {
                "type": self.source_type.get().strip(),
                "uri": self.source_uri.get().strip(),
            },
            "target": {
                "uri": self.target_uri.get().strip(),
                "psql_container": "postgres16",
            },
            "mysql": {
                "container": self.mysql_container.get().strip(),
                "user": self.mysql_user.get().strip(),
                "password": self.mysql_password.get(),
            },
            "pgloader": {
                "image": self.pgloader_image.get().strip(),
                "env": env,
                "clear_public_before_sync": True,
                "show_output": True,
            },
            "datax": {
                "enabled": bool(self.datax_enabled.get()),
                "home": self.datax_home.get().strip(),
                "python": self.datax_python.get().strip() or "python",
                "jvm": "-Xms2g -Xmx6g -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError",
                "loglevel": "warn",
                "source_uri": self.datax_source_uri.get().strip(),
                "mysql_jdbc_params": "useSSL=false",
                "target_table_lowercase": True,
                "target_column_lowercase": True,
                "table_parallelism": datax_table_parallelism,
                "channel": datax_channel,
                "batch_size": datax_batch_size,
                "job_dir": ".datax_jobs",
                "cleanup_jobs_on_finish": True,
                "show_output": True,
                "compact_log": not bool(self.datax_verbose_log.get()),
                "exclude_table_keywords": [],
                "log_retention_days": datax_log_retention_days,
                "log_dirs": ["datax/datax/log", "datax/datax/log_perf"],
                "env": {},
            },
        }

        self.run_structure_button.configure(state=tk.DISABLED)
        self.run_data_button.configure(state=tk.DISABLED)
        self.stop_button.configure(state=tk.NORMAL)
        self.progress.configure(value=0)
        title = "结构同步" if mode == "structure" else "数据同步"
        self.progress_label.configure(text=f"{title} 启动中...")
        self.eta_label.configure(text="预计剩余: --")
        self.size_label.configure(text="已选数据库大小: --")
        self.log_text.delete("1.0", tk.END)
        self.stop_event.clear()
        self.overall_total_tables = 0
        self.overall_processed_tables = 0
        self.start_time = time.time()
        self.last_progress_time = self.start_time
        self.last_progress_count = 0

        self._set_controls_running(True)
        self.progress.start(80)

        self.worker_thread = threading.Thread(target=self._worker, args=(config, mode), daemon=True)
        self.worker_thread.start()

    def _stop_run(self) -> None:
        self.stop_event.set()
        if self.current_process and self.current_process.poll() is None:
            try:
                self.current_process.terminate()
            except Exception:
                pass
        for process in list(self.active_processes):
            try:
                if process.poll() is None:
                    process.terminate()
            except Exception:
                pass
        self.queue.put(("log", "\n已请求停止。\n"))
        if not self.current_process:
            self.queue.put(("stopped",))

    def _worker(self, config: dict, mode: str) -> None:
        try:
            cleanup_datax_logs_by_retention(self.workspace, config.get("datax", {}))
            self.overall_total_tables = 0
            self.overall_processed_tables = 0
            total_dbs = len(config["databases"])
            if mode == "structure":
                for db in config["databases"]:
                    source_type = config.get("source", {}).get("type", "mysql")
                    if source_type == "mysql":
                        mysql_cfg = config["mysql"]
                        self.overall_total_tables += get_total_tables(
                            mysql_cfg["container"],
                            mysql_cfg["user"],
                            mysql_cfg["password"],
                            db,
                        )

            for idx, db in enumerate(config["databases"], start=1):
                if self.stop_event.is_set():
                    self.queue.put(("stopped",))
                    return
                title = "结构同步" if mode == "structure" else "数据同步"
                self.queue.put(("log", f"===============================\n开始{title}数据库: {db}\n===============================\n"))

                if mode == "structure":
                    if bool(config.get("pgloader", {}).get("clear_public_before_sync", True)):
                        self.queue.put(("log", f"清理目标库 public 表: {db}\n"))
                        clear_code, clear_output = clear_target_public_tables(db, config)
                        if clear_output:
                            self.queue.put(("log", clear_output + ("" if clear_output.endswith("\n") else "\n")))
                        if clear_code != 0:
                            self.queue.put(("failed", f"清理目标库 public 表失败: {db}\n"))
                            return

                    mysql_cfg = config["mysql"]
                    source_type = config.get("source", {}).get("type", "mysql")
                    total_tables = 0
                    if source_type == "mysql":
                        total_tables = get_total_tables(
                            mysql_cfg["container"],
                            mysql_cfg["user"],
                            mysql_cfg["password"],
                            db,
                        )

                    source_uri = config.get("source", {}).get("uri", "").replace("{{DB_NAME}}", db)
                    target_uri = config.get("target", {}).get("uri", "").replace("{{DB_NAME}}", db)

                    template_path = os.path.join(self.workspace, config["load_template"])
                    rendered_name = f".pgloader_rendered_{db}.load"
                    rendered_path = os.path.join(self.workspace, rendered_name)
                    cleanup_pgloader_temp = bool(config.get("pgloader", {}).get("cleanup_temp_files", True))
                    if cleanup_pgloader_temp:
                        cleanup_pgloader_rendered_files_for_db(self.workspace, db)
                    render_load_file(
                        template_path,
                        rendered_path,
                        {
                            "DB_NAME": db,
                            "SOURCE_URI": source_uri,
                            "TARGET_URI": target_uri,
                        },
                    )

                    cmd = build_pgloader_command(
                        workspace=self.workspace,
                        load_file=rendered_name,
                        image=config["pgloader"]["image"],
                        env=config["pgloader"].get("env", {}),
                    )

                    show_output = bool(config["pgloader"].get("show_output", False))
                    processed = 0
                    tail = deque(maxlen=200)
                    table_line = re.compile(rf"^\s*{re.escape(db)}\.\S+\s+\d+\s+\d+")
                    has_log_error = False

                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        cwd=self.workspace,
                    )
                    self.current_process = process

                    try:
                        assert process.stdout is not None
                        for line in process.stdout:
                            if self.stop_event.is_set():
                                try:
                                    process.terminate()
                                except Exception:
                                    pass
                                self.queue.put(("stopped",))
                                return
                            tail.append(line)
                            if show_output:
                                self.queue.put(("log", line))
                            if is_pgloader_error_log(line):
                                has_log_error = True
                            if table_line.match(line):
                                processed += 1
                                self.overall_processed_tables += 1
                                self.queue.put(("progress", db, processed, total_tables, idx, total_dbs, self.overall_processed_tables, self.overall_total_tables))

                        code = process.wait()
                    finally:
                        if cleanup_pgloader_temp:
                            cleanup_pgloader_rendered_file(rendered_path)

                    if has_log_error:
                        code = 1
                    self.current_process = None
                    if code != 0:
                        self.queue.put(("error", db, list(tail)))
                        return
                    self.queue.put(("log", f"结构同步成功: {db}\n"))
                else:
                    datax_cfg = config.get("datax", {})
                    if not bool(datax_cfg.get("enabled", False)):
                        self.queue.put(("failed", "DataX 未启用，请先勾选 DataX 启用。\n"))
                        return

                    datax_home = (datax_cfg.get("home") or "").strip()
                    datax_py = os.path.join(datax_home, "bin", "datax.py")
                    if not datax_home or not os.path.isfile(datax_py):
                        self.queue.put(("failed", f"DataX 配置无效，未找到: {datax_py}\n"))
                        return

                    mysql_cfg = config.get("mysql", {})
                    source_uri_template = datax_cfg.get("source_uri") or config.get("source", {}).get("uri", "")
                    source_uri = source_uri_template.replace("{{DB_NAME}}", db)
                    target_uri = config.get("target", {}).get("uri", "").replace("{{DB_NAME}}", db)

                    tables = get_mysql_tables(
                        mysql_cfg.get("container", ""),
                        mysql_cfg.get("user", ""),
                        mysql_cfg.get("password", ""),
                        db,
                    )
                    exclude_keywords = datax_cfg.get("exclude_table_keywords", [])
                    if not isinstance(exclude_keywords, list):
                        exclude_keywords = []
                    tables = [table for table in tables if not should_skip_table(table, exclude_keywords)]

                    if not tables:
                        self.queue.put(("log", f"DataX skipped: no tables found in {db}\n"))
                    else:
                        cleanup_on_finish = is_cleanup_jobs_on_finish(datax_cfg)
                        if cleanup_on_finish:
                            cleanup_datax_jobs_for_db(self.workspace, db, datax_cfg)

                        self.queue.put(("log", f"DataX start: {db}, tables={len(tables)}\n"))
                        show_output_datax = bool(datax_cfg.get("show_output", False))
                        compact_log = bool(datax_cfg.get("compact_log", True))

                        table_parallelism = max(1, int(datax_cfg.get("table_parallelism", 3)))
                        self.queue.put(("log", f"DataX table parallelism: {table_parallelism}\n"))
                        process_lock = threading.Lock()

                        def run_one_table(t_idx: int, table: str) -> tuple[int, str, str, str]:
                            if self.stop_event.is_set():
                                return 130, table, "", ""

                            columns = get_mysql_columns(
                                mysql_cfg.get("container", ""),
                                mysql_cfg.get("user", ""),
                                mysql_cfg.get("password", ""),
                                db,
                                table,
                            )
                            if not columns:
                                self.queue.put(("log", f"DataX skipped table: {db}.{table} (no columns)\n"))
                                return 0, table, "", ""

                            split_pk = get_mysql_split_pk(
                                mysql_cfg.get("container", ""),
                                mysql_cfg.get("user", ""),
                                mysql_cfg.get("password", ""),
                                db,
                                table,
                            )
                            channel = int(datax_cfg.get("channel", 2))
                            batch_size = int(datax_cfg.get("batch_size", 2000))
                            split_pk_disp = split_pk if split_pk else "none"

                            job_file = build_datax_job(self.workspace, db, table, columns, source_uri, target_uri, datax_cfg, split_pk=split_pk)
                            cmd_datax = build_datax_command(job_file, datax_cfg)
                            self.queue.put(("log", f"DataX [{t_idx}/{len(tables)}] {db}.{table} (channel={channel}, batch={batch_size}, splitPk={split_pk_disp})\n"))

                            cmd_env = os.environ.copy()
                            for key, value in datax_cfg.get("env", {}).items():
                                cmd_env[str(key)] = str(value)

                            process = subprocess.Popen(
                                cmd_datax,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                text=True,
                                encoding="utf-8",
                                errors="replace",
                                cwd=self.workspace,
                                env=cmd_env,
                            )
                            with process_lock:
                                self.active_processes.add(process)
                                self.current_process = process

                            dtail = deque(maxlen=200)
                            try:
                                assert process.stdout is not None
                                for line in process.stdout:
                                    if self.stop_event.is_set():
                                        try:
                                            process.terminate()
                                        except Exception:
                                            pass
                                        break
                                    dtail.append(line)
                                    if show_output_datax:
                                        if compact_log:
                                            if is_datax_key_log(line):
                                                self.queue.put(("log", line))
                                        else:
                                            self.queue.put(("log", line))

                                dcode = process.wait()
                            finally:
                                with process_lock:
                                    self.active_processes.discard(process)
                                    if self.current_process is process:
                                        self.current_process = None
                                if cleanup_on_finish:
                                    cleanup_datax_job_file(job_file)

                            if self.stop_event.is_set():
                                return 130, table, "", job_file
                            if dcode != 0:
                                return dcode, table, "".join(dtail), job_file
                            return 0, table, "", job_file

                        first_error: tuple[int, str, str, str] | None = None
                        with ThreadPoolExecutor(max_workers=table_parallelism) as executor:
                            futures = {
                                executor.submit(run_one_table, t_idx, table): table
                                for t_idx, table in enumerate(tables, start=1)
                            }
                            for future in as_completed(futures):
                                dcode, failed_table, detail, job_file = future.result()
                                if dcode == 130:
                                    for pending in futures:
                                        pending.cancel()
                                    self.queue.put(("stopped",))
                                    return
                                if dcode != 0 and first_error is None:
                                    first_error = (dcode, failed_table, detail, job_file)
                                    self.stop_event.set()
                                    for pending in futures:
                                        pending.cancel()

                        if first_error is not None:
                            _, failed_table, detail, _ = first_error
                            if cleanup_on_finish:
                                cleanup_empty_datax_job_dir(self.workspace, datax_cfg)
                            self.queue.put(("failed", f"\nDataX failed table: {db}.{failed_table}\n--- DataX output (last 200 lines) ---\n{detail}--- end ---\n"))
                            return

                        if cleanup_on_finish:
                            cleanup_empty_datax_job_dir(self.workspace, datax_cfg)

                        self.queue.put(("log", f"DataX success: {db}\n"))

            self.queue.put(("done",))
        except Exception as exc:
            self.queue.put(("log", f"\n线程异常: {exc}\n"))
            self.queue.put(("stopped",))

    def _poll_queue(self) -> None:
        try:
            while True:
                msg = self.queue.get_nowait()
                self._handle_message(msg)
        except queue.Empty:
            pass
        self.after(200, self._poll_queue)

    def _handle_message(self, msg: tuple) -> None:
        kind = msg[0]
        if kind == "log":
            self.log_text.insert(tk.END, msg[1])
            self._trim_log_lines()
            self.log_text.see(tk.END)
        elif kind == "db_list":
            dbs = msg[1]
            keep_selected = set(self.selected_dbs)
            self.db_list.delete(0, tk.END)
            for db in dbs:
                self.db_list.insert(tk.END, db)
            self.selected_dbs = []
            for idx, db in enumerate(dbs):
                if db in keep_selected:
                    self.db_list.selection_set(idx)
                    self.selected_dbs.append(db)
            if not self.selected_dbs:
                self.size_label.configure(text="已选数据库大小: --")
        elif kind == "target_info":
            self._set_info_text(self.target_info_text, msg[1])
        elif kind == "progress":
            db, processed, total, idx, total_dbs, overall_done, overall_total = msg[1:]
            if total > 0:
                percent = min(100, int((processed * 100) / total))
                self.progress.configure(value=percent)
                self.progress_label.configure(text=f"{db}: {percent}% ({processed}/{total}) [DB {idx}/{total_dbs}]")
            else:
                self.progress_label.configure(text=f"{db}: {processed} tables [DB {idx}/{total_dbs}]")

            self._update_eta(overall_done, overall_total)
        elif kind == "error":
            db, tail = msg[1], msg[2]
            self.log_text.insert(tk.END, f"\n同步失败: {db}\n")
            self.log_text.insert(tk.END, "--- pgloader 输出 (最后 200 行) ---\n")
            self.log_text.insert(tk.END, "".join(tail))
            self.log_text.insert(tk.END, "--- 结束 ---\n")
            tail_text = "".join(tail)
            if "max_locks_per_transaction" in tail_text:
                self.log_text.insert(
                    tk.END,
                    "提示: PostgreSQL 的 max_locks_per_transaction 不足。"
                    "当前模板已去掉 include drop 降低锁压力；"
                    "如仍失败，请在目标库提升该参数并重启 PostgreSQL。\n",
                )
            self.log_text.see(tk.END)
            self._set_idle()
        elif kind == "failed":
            self.log_text.insert(tk.END, msg[1])
            self.log_text.see(tk.END)
            self._set_idle()
        elif kind == "done":
            self._set_idle()
        elif kind == "stopped":
            self.log_text.insert(tk.END, "\n已停止。\n")
            self.log_text.see(tk.END)
            self._set_idle()
        elif kind == "size":
            total_bytes = msg[1]
            self.size_label.configure(text=f"已选数据库大小: {self._format_size(total_bytes)}")

    def _set_idle(self) -> None:
        self.progress.stop()
        self.progress.configure(value=0)
        self.progress_label.configure(text="空闲")
        self.eta_label.configure(text="预计剩余: --")
        self.size_label.configure(text="已选数据库大小: --")
        self.run_structure_button.configure(state=tk.NORMAL)
        self.run_data_button.configure(state=tk.NORMAL)
        self.stop_button.configure(state=tk.DISABLED)
        self.current_process = None
        self.active_processes.clear()
        self.stop_event.clear()
        self._set_controls_running(False)

    def _trim_log_lines(self) -> None:
        total_lines = int(self.log_text.index("end-1c").split(".")[0])
        if total_lines <= self.max_log_lines:
            return
        remove_until = total_lines - self.max_log_lines
        self.log_text.delete("1.0", f"{remove_until}.0")

    def _set_info_text(self, widget: tk.Text, content: str) -> None:
        widget.configure(state=tk.NORMAL)
        widget.delete("1.0", tk.END)
        widget.insert(tk.END, content)
        widget.configure(state=tk.DISABLED)

    def _confirm_structure_sync(self) -> bool:
        dlg = tk.Toplevel(self)
        dlg.title("结构同步确认")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(False, False)

        frame = ttk.Frame(dlg, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            frame,
            text="该操作会先清空目标数据库 public 下所有表。\n请先完成备份，再继续。",
            justify=tk.LEFT,
        ).pack(anchor="w", pady=(0, 10))

        result = {"ok": False}

        def on_confirm() -> None:
            result["ok"] = True
            dlg.destroy()

        def on_cancel() -> None:
            dlg.destroy()

        btns = ttk.Frame(frame)
        btns.pack(fill=tk.X)
        ttk.Button(btns, text="取消", command=on_cancel).pack(side=tk.RIGHT)
        ttk.Button(btns, text="我已备份，开始同步", command=on_confirm).pack(side=tk.RIGHT, padx=6)

        dlg.update_idletasks()
        self.update_idletasks()
        parent_x = self.winfo_rootx()
        parent_y = self.winfo_rooty()
        parent_w = self.winfo_width()
        parent_h = self.winfo_height()
        dialog_w = dlg.winfo_width()
        dialog_h = dlg.winfo_height()
        x = parent_x + max(0, (parent_w - dialog_w) // 2)
        y = parent_y + max(0, (parent_h - dialog_h) // 2)
        dlg.geometry(f"+{x}+{y}")

        dlg.protocol("WM_DELETE_WINDOW", on_cancel)
        dlg.wait_window()
        return result["ok"]

    def _update_eta(self, done: int, total: int) -> None:
        if total <= 0:
            self.eta_label.configure(text="ETA: --")
            return

        now = time.time()
        if done <= 0:
            self.eta_label.configure(text="ETA: --")
            return

        elapsed = now - self.start_time
        if elapsed <= 0:
            self.eta_label.configure(text="ETA: --")
            return

        rate = done / elapsed
        if rate <= 0:
            self.eta_label.configure(text="ETA: --")
            return

        remaining = total - done
        eta_seconds = max(0, int(remaining / rate))
        mins, secs = divmod(eta_seconds, 60)
        hours, mins = divmod(mins, 60)
        if hours > 0:
            eta_text = f"预计剩余: {hours}小时 {mins}分 {secs}秒"
        elif mins > 0:
            eta_text = f"预计剩余: {mins}分 {secs}秒"
        else:
            eta_text = f"预计剩余: {secs}秒"

        overall_percent = min(100, int((done * 100) / total))
        self.eta_label.configure(text=f"{eta_text} | 总进度 {overall_percent}%")

    def _set_controls_running(self, running: bool) -> None:
        state = tk.DISABLED if running else tk.NORMAL
        self.run_structure_button.configure(state=tk.DISABLED if running else tk.NORMAL)
        self.run_data_button.configure(state=tk.DISABLED if running else tk.NORMAL)
        self.stop_button.configure(state=tk.NORMAL if running else tk.DISABLED)

        self.db_list.configure(state=state)
        self.db_refresh_btn.configure(state=state)

        self.config_entry.configure(state=state)
        self.config_browse_btn.configure(state=state)
        self.config_load_btn.configure(state=state)
        self.config_save_btn.configure(state=state)

        self.load_template_entry.configure(state=state)
        self.source_type_combo.configure(state=state)
        self.source_uri_entry.configure(state=state)
        self.target_uri_entry.configure(state=state)
        self.mysql_container_entry.configure(state=state)
        self.mysql_user_entry.configure(state=state)
        self.mysql_password_entry.configure(state=state)
        self.pgloader_image_entry.configure(state=state)
        self.datax_enabled_check.configure(state=state)
        self.datax_home_entry.configure(state=state)
        self.datax_python_entry.configure(state=state)
        self.datax_source_uri_entry.configure(state=state)
        self.datax_channel_entry.configure(state=state)
        self.datax_batch_size_entry.configure(state=state)
        self.datax_table_parallelism_entry.configure(state=state)
        self.datax_log_retention_days_entry.configure(state=state)
        self.datax_verbose_log_check.configure(state=state)
        self.env_text.configure(state=state)
        self.load_text.configure(state=state)
        self.load_reload_btn.configure(state=state)
        self.load_save_btn.configure(state=state)
        # always on

    def _on_db_select(self, _event=None) -> None:
        if self.info_refresh_after_id is not None:
            try:
                self.after_cancel(self.info_refresh_after_id)
            except Exception:
                pass
            self.info_refresh_after_id = None

        dbs = [self.db_list.get(i) for i in self.db_list.curselection()]
        if not dbs:
            self.selected_dbs = []
            self.size_label.configure(text="已选数据库大小: --")
            self._set_info_text(self.target_info_text, "未选择数据库")
            return
        self.selected_dbs = dbs
        self._refresh_db_info()

    def _refresh_databases(self) -> None:
        threading.Thread(target=self._refresh_databases_async, daemon=True).start()

    def _refresh_databases_async(self) -> None:
        mysql_container = self.mysql_container.get().strip()
        mysql_user = self.mysql_user.get().strip()
        mysql_password = self.mysql_password.get()
        source_dbs = get_mysql_databases(mysql_container, mysql_user, mysql_password)
        if not source_dbs:
            source_dbs = list(self.fallback_databases)

        target_uri = self.target_uri.get().strip()
        psql_container = "postgres16"
        try:
            cfg = load_config(self.config_path.get())
            psql_container = cfg.get("target", {}).get("psql_container", "postgres16") or "postgres16"
        except Exception:
            pass

        target_dbs: list[str] = []
        if target_uri:
            try:
                probe_uri = target_uri
                if "{{DB_NAME}}" in probe_uri:
                    probe_uri = probe_uri.replace("{{DB_NAME}}", source_dbs[0] if source_dbs else "postgres")
                target_dbs = get_target_databases(probe_uri, psql_container)
            except Exception:
                target_dbs = []

        self.queue.put(("db_list", source_dbs))
        if target_dbs:
            lines = [f"目标数据库数量: {len(target_dbs)}", ""] + target_dbs
            self.queue.put(("target_info", "\n".join(lines)))
        else:
            self.queue.put(("target_info", "目标数据库查询失败或无数据"))
        self.queue.put(("log", f"已刷新数据库：源库 {len(source_dbs)} 个，目标库 {len(target_dbs)} 个\n"))

    def _refresh_db_info(self) -> None:
        if not self.selected_dbs:
            return
        dbs = list(self.selected_dbs)
        threading.Thread(target=self._refresh_db_info_async, args=(dbs,), daemon=True).start()
        self.info_refresh_after_id = self.after(5000, self._refresh_db_info)

    def _refresh_db_info_async(self, dbs: list[str]) -> None:
        target_lines: list[str] = []

        if self.source_type.get().strip() != "mysql":
            self.queue.put(("size", 0))
        else:
            mysql_container = self.mysql_container.get().strip()
            mysql_user = self.mysql_user.get().strip()
            mysql_password = self.mysql_password.get()
            total_bytes = 0
            total_tables = 0
            for db in dbs:
                cmd = [
                    "docker",
                    "exec",
                    mysql_container,
                    "mysql",
                    f"-u{mysql_user}",
                    f"-p{mysql_password}",
                    "-N",
                    "-e",
                    (
                        "SELECT COUNT(*), COALESCE(SUM(data_length + index_length), 0) "
                        f"FROM information_schema.tables WHERE table_schema='{db}';"
                    ),
                ]
                result = run_command(cmd)
                if result.returncode != 0:
                    continue
                output = (result.stdout or "").strip().splitlines()
                if not output:
                    continue
                parts = output[0].split("\t")
                if len(parts) >= 2:
                    try:
                        count = int(parts[0])
                        size = int(parts[1])
                        total_tables += count
                        total_bytes += size
                    except ValueError:
                        pass
            self.queue.put(("size", total_bytes))

        target_cfg = {
            "uri": self.target_uri.get().strip(),
            "psql_container": "postgres16",
        }
        target_parsed_ok = True
        try:
            parse_db_uri(target_cfg["uri"].replace("{{DB_NAME}}", dbs[0]))
        except Exception:
            target_parsed_ok = False

        if not target_parsed_ok:
            target_lines.append("目标 URI 无效，无法实时统计")
        else:
            for db in dbs:
                target_lines.append(f"[{db}]")
                target_uri = target_cfg["uri"].replace("{{DB_NAME}}", db)
                target = parse_db_uri(target_uri)
                psql_container = target_cfg.get("psql_container", "postgres16")
                sql = (
                    "SELECT COUNT(*)::text || '|' || "
                    "COALESCE(SUM(pg_total_relation_size(to_regclass(quote_ident(schemaname)||'.'||quote_ident(tablename)))),0)::text "
                    "FROM pg_tables WHERE schemaname='public';"
                )
                cmd = [
                    "docker",
                    "exec",
                    "-e",
                    f"PGPASSWORD={target.get('password', '')}",
                    psql_container,
                    "psql",
                    "-h",
                    str(target.get("host", "")),
                    "-p",
                    str(target.get("port", 5432) or 5432),
                    "-U",
                    str(target.get("user", "")),
                    "-d",
                    str(target.get("database", "")),
                    "-t",
                    "-A",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    sql,
                ]
                result = run_command(cmd)
                if result.returncode != 0:
                    target_lines.append("- 连接失败或查询失败")
                    target_lines.append("")
                    continue
                text = (result.stdout or "").strip()
                if not text or "|" not in text:
                    target_lines.append("- 无统计数据")
                    target_lines.append("")
                    continue
                count_str, size_str = text.split("|", 1)
                try:
                    count = int(count_str.strip())
                    size = int(size_str.strip())
                    target_lines.append(f"- public 表数量: {count}")
                    target_lines.append(f"- public 数据量: {self._format_size(size)}")
                except ValueError:
                    target_lines.append("- 统计解析失败")
                target_lines.append("")

        self.queue.put(("target_info", "\n".join(target_lines).strip() or "无数据"))

    def _reload_template(self) -> None:
        template_path = os.path.join(self.workspace, self.load_template.get().strip())
        if not os.path.exists(template_path):
            self.load_text.delete("1.0", tk.END)
            return
        try:
            with open(template_path, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception as exc:
            messagebox.showerror("错误", f"读取模板失败: {exc}")
            return
        self.load_text.delete("1.0", tk.END)
        self.load_text.insert(tk.END, content)

    def _save_template(self) -> None:
        template_path = os.path.join(self.workspace, self.load_template.get().strip())
        try:
            with open(template_path, "w", encoding="utf-8") as f:
                f.write(self.load_text.get("1.0", tk.END))
        except Exception as exc:
            messagebox.showerror("错误", f"保存模板失败: {exc}")
            return
        messagebox.showinfo("已保存", "模板已保存。")

    def _format_size(self, num_bytes: int) -> str:
        if num_bytes < 1024:
            return f"{num_bytes} B"
        for unit in ["KB", "MB", "GB", "TB"]:
            num_bytes /= 1024
            if num_bytes < 1024:
                return f"{num_bytes:.2f} {unit}"
        return f"{num_bytes:.2f} PB"


if __name__ == "__main__":
    app = PgloaderGUI()
    app.mainloop()
