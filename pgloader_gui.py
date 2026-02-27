import json
import os
import queue
import re
import shutil
import socket
import subprocess
import threading
import time
import tkinter as tk
from collections import deque
from datetime import datetime
from tkinter import ttk, messagebox, filedialog
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

from version import APP_NAME, APP_VERSION

DEFAULT_CONFIG = "pgloader_tool.json"
SOURCE_TYPES = ["mysql", "mssql", "sqlite", "pgsql", "redshift", "file"]
TEMPLATE_PRESETS = {
    "MySQL -> PostgreSQL": """LOAD DATABASE
    FROM {{SOURCE_URI}}
    INTO {{TARGET_URI}}

WITH
    include drop,
    create tables,
    create indexes,
    reset sequences,
    foreign keys,
    workers = 2,
    concurrency = 1,
    batch rows = 10000,
    prefetch rows = 10000

CAST
    type tinyint to smallint drop typemod,
    type datetime to timestamp;\n""",
    "SQL Server -> PostgreSQL": """LOAD DATABASE
    FROM {{SOURCE_URI}}
    INTO {{TARGET_URI}}

WITH
    include drop,
    create tables,
    create indexes,
    reset sequences,
    foreign keys,
    workers = 2,
    concurrency = 1,
    batch rows = 10000,
    prefetch rows = 10000;\n""",
    "SQLite -> PostgreSQL": """LOAD DATABASE
    FROM {{SOURCE_URI}}
    INTO {{TARGET_URI}}

WITH
    include drop,
    create tables,
    create indexes,
    reset sequences,
    foreign keys;\n""",
    "CSV -> PostgreSQL": """LOAD CSV
    FROM {{SOURCE_URI}}
    INTO {{TARGET_URI}}

WITH
    truncate,
    workers = 2,
    batch rows = 10000;\n""",
}


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(path: str, config: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def run_command(cmd: list) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


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
    mode: str,
    binary: str,
) -> list:
    if mode == "local":
        return [binary, "--on-error-stop", load_file]

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


def normalize_uri_credentials(uri: str) -> str:
    try:
        parsed = urlparse(uri)
        if not parsed.scheme or not parsed.netloc or parsed.username is None:
            return uri

        username = quote(unquote(parsed.username), safe="")
        password = parsed.password
        userinfo = username
        if password is not None:
            userinfo = f"{username}:{quote(unquote(password), safe='')}"

        host = parsed.hostname or ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = f":{parsed.port}" if parsed.port else ""
        netloc = f"{userinfo}@{host}{port}"
        return parsed._replace(netloc=netloc).geturl()
    except Exception:
        return uri


class PgloaderGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1200x720")
        self.resizable(True, True)

        self.workspace = os.path.abspath(os.path.dirname(__file__))
        self.queue: "queue.Queue[tuple]" = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.current_process: subprocess.Popen | None = None
        self.stop_event = threading.Event()
        self.overall_total_tables = 0
        self.overall_processed_tables = 0
        self.start_time = 0.0
        self.last_progress_time = 0.0
        self.last_progress_count = 0
        self.log_file_path = ""

        self.config_path = tk.StringVar(value=os.path.join(self.workspace, DEFAULT_CONFIG))
        self.profile_var = tk.StringVar()
        self.load_template = tk.StringVar()
        self.source_type = tk.StringVar(value="mysql")
        self.source_uri = tk.StringVar()
        self.target_uri = tk.StringVar()
        self.target_admin_uri = tk.StringVar()
        self.mysql_container = tk.StringVar()
        self.mysql_user = tk.StringVar()
        self.mysql_password = tk.StringVar()
        self.pgloader_image = tk.StringVar()
        self.pg_admin_image = tk.StringVar(value="postgres:16.11")
        self.pgloader_mode = tk.StringVar(value="docker")
        self.pgloader_binary = tk.StringVar(value="pgloader")
        self.show_output = tk.BooleanVar(value=True)
        self.template_preset = tk.StringVar()
        self.test_in_container = tk.BooleanVar(value=False)
        self.target_user_name = tk.StringVar()
        self.target_user_password = tk.StringVar()
        self.target_user_auto = tk.BooleanVar(value=True)
        self.log_filter_var = tk.StringVar()
        self.log_filter_entry: ttk.Entry | None = None
        self.log_clear_btn: ttk.Button | None = None
        self.log_save_btn: ttk.Button | None = None
        self.log_lines: list[str] = []

        self._build_ui()
        self._load_config_safe()
        self._poll_queue()
        self.after(250, self._show_startup_tips)

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=10)
        top.pack(fill=tk.BOTH, expand=True)

        header = ttk.Frame(top)
        header.pack(fill=tk.X, pady=(0, 6))
        self.app_label = ttk.Label(header, text=f"{APP_NAME} v{APP_VERSION}")
        self.app_label.pack(side=tk.LEFT)
        self.update_btn = ttk.Button(header, text="检查更新", command=self._check_update)
        self.update_btn.pack(side=tk.RIGHT)
        paned = ttk.Panedwindow(top, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        left_container = ttk.Frame(paned)
        right_container = ttk.Frame(paned)
        paned.add(left_container, weight=3)
        paned.add(right_container, weight=2)

        notebook = ttk.Notebook(left_container)
        notebook.pack(fill=tk.BOTH, expand=True)

        tab_basic = ttk.Frame(notebook)
        tab_advanced = ttk.Frame(notebook)
        tab_templates = ttk.Frame(notebook)
        notebook.add(tab_basic, text="基础设置")
        notebook.add(tab_advanced, text="高级设置")
        notebook.add(tab_templates, text="模板设置")

        cfg_frame = ttk.LabelFrame(tab_basic, text="配置")
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

        ttk.Label(cfg_frame, text="配置档案").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.profile_combo = ttk.Combobox(cfg_frame, textvariable=self.profile_var, state="readonly")
        self.profile_combo.grid(row=1, column=1, sticky="we", padx=5, pady=(6, 0))
        self.profile_combo.bind("<<ComboboxSelected>>", self._on_profile_select)
        self.profile_refresh_btn = ttk.Button(cfg_frame, text="刷新", command=self._refresh_profiles)
        self.profile_refresh_btn.grid(row=1, column=2, padx=5, pady=(6, 0))

        cfg_frame.columnconfigure(1, weight=1)

        mid = ttk.Frame(tab_basic)
        mid.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        left = ttk.Frame(mid)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)

        right = ttk.Frame(mid)
        right.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5)

        db_frame = ttk.LabelFrame(left, text="数据库")
        db_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.db_list = tk.Listbox(db_frame, selectmode=tk.EXTENDED)
        self.db_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.db_list.bind("<<ListboxSelect>>", self._on_db_select)
        db_scroll = ttk.Scrollbar(db_frame, command=self.db_list.yview)
        db_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.db_list.configure(yscrollcommand=db_scroll.set)

        db_buttons = ttk.Frame(left)
        db_buttons.pack(fill=tk.X, pady=5)
        self.db_add_btn = ttk.Button(db_buttons, text="新增", command=self._add_db)
        self.db_add_btn.pack(side=tk.LEFT)
        self.db_remove_btn = ttk.Button(db_buttons, text="删除", command=self._remove_db)
        self.db_remove_btn.pack(side=tk.LEFT, padx=5)

        settings = ttk.LabelFrame(right, text="连接设置")
        settings.pack(fill=tk.BOTH, expand=True, pady=5)

        row = 0
        ttk.Label(settings, text="源类型").grid(row=row, column=0, sticky="w")
        self.source_type_combo = ttk.Combobox(settings, values=SOURCE_TYPES, textvariable=self.source_type, state="readonly")
        self.source_type_combo.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        self.source_type_combo.bind("<<ComboboxSelected>>", self._on_source_type_change)
        row += 1

        ttk.Label(settings, text="源连接 URI").grid(row=row, column=0, sticky="w")
        self.source_uri_entry = ttk.Entry(settings, textvariable=self.source_uri)
        self.source_uri_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        self.test_source_btn = ttk.Button(settings, text="测试源连接", command=lambda: self._test_connection("source"))
        self.test_source_btn.grid(row=row, column=1, sticky="e", padx=5, pady=(0, 2))
        row += 1

        ttk.Label(settings, text="目标连接 URI").grid(row=row, column=0, sticky="w")
        self.target_uri_entry = ttk.Entry(settings, textvariable=self.target_uri)
        self.target_uri_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        self.test_target_btn = ttk.Button(settings, text="测试目标连接", command=lambda: self._test_connection("target"))
        self.test_target_btn.grid(row=row, column=1, sticky="e", padx=5, pady=(0, 2))
        row += 1

        self.test_container_check = ttk.Checkbutton(settings, text="容器内测试(适用于 host.docker.internal)", variable=self.test_in_container)
        self.test_container_check.grid(row=row, column=1, sticky="w", padx=5, pady=(0, 2))
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

        settings.columnconfigure(1, weight=1)

        advanced = ttk.Frame(tab_advanced)
        advanced.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        adv_left = ttk.Frame(advanced)
        adv_left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)

        adv_conn = ttk.LabelFrame(adv_left, text="pgloader 设置")
        adv_conn.pack(fill=tk.X, padx=5, pady=5)

        row = 0
        ttk.Label(adv_conn, text="运行方式").grid(row=row, column=0, sticky="w")
        self.pgloader_mode_combo = ttk.Combobox(adv_conn, values=["docker", "local"], textvariable=self.pgloader_mode, state="readonly")
        self.pgloader_mode_combo.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        self.pgloader_mode_combo.bind("<<ComboboxSelected>>", self._on_pgloader_mode_change)
        row += 1

        ttk.Label(adv_conn, text="pgloader 路径").grid(row=row, column=0, sticky="w")
        self.pgloader_binary_entry = ttk.Entry(adv_conn, textvariable=self.pgloader_binary)
        self.pgloader_binary_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(adv_conn, text="pgloader 镜像").grid(row=row, column=0, sticky="w")
        self.pgloader_image_entry = ttk.Entry(adv_conn, textvariable=self.pgloader_image)
        self.pgloader_image_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(adv_conn, text="PG 管理镜像").grid(row=row, column=0, sticky="w")
        self.pg_admin_image_entry = ttk.Entry(adv_conn, textvariable=self.pg_admin_image)
        self.pg_admin_image_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        self.check_docker_btn = ttk.Button(adv_conn, text="检查 Docker", command=self._check_docker)
        self.check_docker_btn.grid(row=row, column=1, sticky="e", padx=5, pady=(0, 2))
        row += 1

        self.pull_pgloader_btn = ttk.Button(adv_conn, text="拉取 pgloader 镜像", command=self._pull_pgloader_image)
        self.pull_pgloader_btn.grid(row=row, column=1, sticky="e", padx=5, pady=(0, 2))
        row += 1

        self.pull_pgadmin_btn = ttk.Button(adv_conn, text="拉取 PG 管理镜像", command=self._pull_pgadmin_image)
        self.pull_pgadmin_btn.grid(row=row, column=1, sticky="e", padx=5, pady=(0, 2))
        row += 1

        ttk.Label(adv_conn, text="环境变量 (JSON)").grid(row=row, column=0, sticky="nw")
        self.env_text = tk.Text(adv_conn, height=6)
        self.env_text.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1
        adv_conn.columnconfigure(1, weight=1)

        adv_user = ttk.LabelFrame(adv_left, text="目标用户")
        adv_user.pack(fill=tk.X, padx=5, pady=5)

        row = 0
        ttk.Label(adv_user, text="管理员 URI").grid(row=row, column=0, sticky="w")
        self.target_admin_uri_entry = ttk.Entry(adv_user, textvariable=self.target_admin_uri)
        self.target_admin_uri_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(adv_user, text="用户名").grid(row=row, column=0, sticky="w")
        self.target_user_name_entry = ttk.Entry(adv_user, textvariable=self.target_user_name)
        self.target_user_name_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(adv_user, text="默认密码").grid(row=row, column=0, sticky="w")
        self.target_user_password_entry = ttk.Entry(adv_user, textvariable=self.target_user_password, show="*")
        self.target_user_password_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        self.target_user_auto_check = ttk.Checkbutton(adv_user, text="自动创建并授权", variable=self.target_user_auto)
        self.target_user_auto_check.grid(row=row, column=1, sticky="w", padx=5, pady=(0, 2))
        row += 1
        adv_user.columnconfigure(1, weight=1)

        adv_template = ttk.LabelFrame(tab_templates, text="模板设置")
        adv_template.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        row = 0
        ttk.Label(adv_template, text="模板文件").grid(row=row, column=0, sticky="w")
        self.load_template_entry = ttk.Entry(adv_template, textvariable=self.load_template)
        self.load_template_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        ttk.Label(adv_template, text="模板示例").grid(row=row, column=0, sticky="w")
        self.template_combo = ttk.Combobox(adv_template, values=list(TEMPLATE_PRESETS.keys()), textvariable=self.template_preset, state="readonly")
        self.template_combo.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        self.template_apply_btn = ttk.Button(adv_template, text="应用模板", command=self._apply_template_preset)
        self.template_apply_btn.grid(row=row, column=1, sticky="e", padx=5, pady=(0, 2))
        row += 1

        ttk.Label(adv_template, text="Load 脚本内容").grid(row=row, column=0, sticky="nw")
        self.load_text = tk.Text(adv_template, height=12)
        self.load_text.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        row += 1

        self.load_file_btns = ttk.Frame(adv_template)
        self.load_file_btns.grid(row=row, column=1, sticky="w", padx=5, pady=2)
        self.load_reload_btn = ttk.Button(self.load_file_btns, text="载入模板", command=self._reload_template)
        self.load_reload_btn.pack(side=tk.LEFT)
        self.load_save_btn = ttk.Button(self.load_file_btns, text="保存模板", command=self._save_template)
        self.load_save_btn.pack(side=tk.LEFT, padx=5)
        row += 1
        adv_template.columnconfigure(1, weight=1)

        bottom = ttk.Frame(top)
        bottom.pack(fill=tk.X, padx=5, pady=5)

        self.run_button = ttk.Button(bottom, text="开始同步", command=self._run_selected)
        self.run_button.pack(side=tk.LEFT)
        self.stop_button = ttk.Button(bottom, text="停止", command=self._stop_run, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        self.progress_label = ttk.Label(bottom, text="空闲")
        self.progress_label.pack(side=tk.LEFT, padx=10)

        self.progress = ttk.Progressbar(bottom, length=300, mode="indeterminate")
        self.progress.pack(side=tk.LEFT, padx=5)

        self.eta_label = ttk.Label(bottom, text="预计剩余: --")
        self.eta_label.pack(side=tk.LEFT, padx=10)

        self.size_label = ttk.Label(bottom, text="已选数据库大小: --")
        self.size_label.pack(side=tk.LEFT, padx=10)

        log_frame = ttk.LabelFrame(right_container, text="日志")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        log_toolbar = ttk.Frame(log_frame)
        log_toolbar.pack(fill=tk.X, padx=5, pady=(4, 2))

        ttk.Label(log_toolbar, text="过滤").pack(side=tk.LEFT)
        self.log_filter_var = tk.StringVar()
        self.log_filter_entry = ttk.Entry(log_toolbar, textvariable=self.log_filter_var, width=20)
        self.log_filter_entry.pack(side=tk.LEFT, padx=5)
        self.log_filter_entry.bind("<KeyRelease>", self._apply_log_filter)

        self.log_clear_btn = ttk.Button(log_toolbar, text="清空", command=self._clear_log_view)
        self.log_clear_btn.pack(side=tk.LEFT, padx=5)

        self.log_save_btn = ttk.Button(log_toolbar, text="保存", command=self._save_log_file)
        self.log_save_btn.pack(side=tk.LEFT)

        self.log_text = tk.Text(log_frame, height=12)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0), pady=(2, 5))
        log_scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y, pady=(2, 5))
        self.log_text.configure(yscrollcommand=log_scroll.set)

        self._refresh_profiles()

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

        self.db_list.delete(0, tk.END)
        for db in config.get("databases", []):
            self.db_list.insert(tk.END, db)
        self.size_label.configure(text="已选数据库大小: --")

        self.load_template.set(config.get("load_template", "mysql_to_pg.load"))
        source_cfg = config.get("source", {})
        target_cfg = config.get("target", {})
        self.source_type.set(source_cfg.get("type", "mysql"))
        self.source_uri.set(source_cfg.get("uri", ""))
        self.target_uri.set(target_cfg.get("uri", ""))
        self.target_admin_uri.set(target_cfg.get("admin_uri", ""))
        target_user = config.get("target_user", {})
        self.target_user_name.set(target_user.get("name", ""))
        self.target_user_password.set(target_user.get("password", ""))
        self.target_user_auto.set(bool(target_user.get("auto_create", False)))
        mysql_cfg = config.get("mysql", {})
        self.mysql_container.set(mysql_cfg.get("container", "mysql8"))
        self.mysql_user.set(mysql_cfg.get("user", "root"))
        self.mysql_password.set(mysql_cfg.get("password", ""))

        pgloader_cfg = config.get("pgloader", {})
        self.pgloader_image.set(pgloader_cfg.get("image", "dimitri/pgloader"))
        self.show_output.set(True)
        env = pgloader_cfg.get("env", {})
        self.env_text.delete("1.0", tk.END)
        self.env_text.insert(tk.END, json.dumps(env, ensure_ascii=False, indent=2))

        pg_admin_cfg = config.get("pg_admin", {})
        self.pg_admin_image.set(pg_admin_cfg.get("image", "postgres:14"))

        self._reload_template()
        self._sync_profile_selection()
        self._on_source_type_change()
        self._on_pgloader_mode_change()

    def _save_config_safe(self) -> None:
        try:
            env = json.loads(self.env_text.get("1.0", tk.END).strip() or "{}")
        except json.JSONDecodeError as exc:
            messagebox.showerror("错误", f"环境变量 JSON 无效: {exc}")
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
                "admin_uri": self.target_admin_uri.get().strip(),
            },
            "target_user": {
                "auto_create": bool(self.target_user_auto.get()),
                "name": self.target_user_name.get().strip(),
                "password": self.target_user_password.get(),
            },
            "pg_admin": {
                "image": self.pg_admin_image.get().strip() or "postgres:14",
            },
            "mysql": {
                "container": self.mysql_container.get().strip(),
                "user": self.mysql_user.get().strip(),
                "password": self.mysql_password.get(),
            },
            "pgloader": {
                "image": self.pgloader_image.get().strip(),
                "mode": self.pgloader_mode.get().strip(),
                "binary": self.pgloader_binary.get().strip(),
                "env": env,
                "show_output": True,
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

    def _run_selected(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("运行中", "当前正在同步，请先完成或停止。")
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

        config = {
            "databases": dbs,
            "load_template": self.load_template.get().strip(),
            "source": {
                "type": self.source_type.get().strip(),
                "uri": self.source_uri.get().strip(),
            },
            "target": {
                "uri": self.target_uri.get().strip(),
                "admin_uri": self.target_admin_uri.get().strip(),
            },
            "target_user": {
                "auto_create": bool(self.target_user_auto.get()),
                "name": self.target_user_name.get().strip(),
                "password": self.target_user_password.get(),
            },
            "pg_admin": {
                "image": self.pg_admin_image.get().strip() or "postgres:14",
            },
            "mysql": {
                "container": self.mysql_container.get().strip(),
                "user": self.mysql_user.get().strip(),
                "password": self.mysql_password.get(),
            },
            "pgloader": {
                "image": self.pgloader_image.get().strip(),
                "mode": self.pgloader_mode.get().strip(),
                "binary": self.pgloader_binary.get().strip(),
                "env": env,
                "show_output": True,
            },
        }

        errors = self._validate_config(config)
        if errors:
            messagebox.showerror("配置错误", "\n".join(errors))
            return

        self.run_button.configure(state=tk.DISABLED)
        self.stop_button.configure(state=tk.NORMAL)
        self.progress.configure(value=0)
        self.progress_label.configure(text="启动中...")
        self.eta_label.configure(text="预计剩余: --")
        self.size_label.configure(text="已选数据库大小: --")
        self.log_text.delete("1.0", tk.END)
        self.log_lines = []
        self.stop_event.clear()
        self.overall_total_tables = 0
        self.overall_processed_tables = 0
        self.start_time = time.time()
        self.last_progress_time = self.start_time
        self.last_progress_count = 0
        self.log_file_path = self._prepare_log_file()
        if self.log_file_path:
            self.log_text.insert(tk.END, f"日志文件: {self.log_file_path}\n")

        self._set_controls_running(True)
        self.progress.start(80)

        self.worker_thread = threading.Thread(target=self._worker, args=(config,), daemon=True)
        self.worker_thread.start()

    def _stop_run(self) -> None:
        self.stop_event.set()
        if self.current_process and self.current_process.poll() is None:
            try:
                self.current_process.terminate()
            except Exception:
                pass
        self.queue.put(("log", "\n已请求停止。\n"))
        if not self.current_process:
            self.queue.put(("stopped",))

    def _worker(self, config: dict) -> None:
        try:
            self.overall_total_tables = 0
            self.overall_processed_tables = 0
            total_dbs = len(config["databases"])
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
                self.queue.put(("log", f"===============================\n开始同步数据库: {db}\n===============================\n"))

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
                if config.get("target_user", {}).get("auto_create", False):
                    target_uri = self._apply_target_user_uri(target_uri, config.get("target_user", {}))
                    self._append_log(f"Target URI: {self._redact_uri(target_uri)} (auto user)\n")

                test_ok, test_msg = self._test_target_connection(target_uri, config)
                if not test_ok and config.get("target_user", {}).get("auto_create", False):
                    ensure_ok, ensure_msg = self._ensure_pg_user(config, db)
                    if not ensure_ok:
                        self.queue.put(("error", db, [f"Target connection test failed: {test_msg}\n", f"PG user ensure failed: {ensure_msg}\n"]))
                        return
                    if ensure_msg and ensure_msg != "skipped":
                        self._append_log(f"{ensure_msg}\n")
                    test_ok, test_msg = self._test_target_connection(target_uri, config)

                if not test_ok:
                    self.queue.put(("error", db, [f"Target connection test failed: {test_msg}\n"]))
                    return

                ensure_ok, ensure_msg = self._ensure_pg_user(config, db)
                if not ensure_ok:
                    self._append_log(f"Warning: PG user ensure failed, continue with existing target user: {ensure_msg}\n")
                elif ensure_msg and ensure_msg != "skipped":
                    self._append_log(f"{ensure_msg}\n")

                template_path = os.path.join(self.workspace, config["load_template"])
                rendered_name = f".pgloader_rendered_{db}.load"
                rendered_path = os.path.join(self.workspace, rendered_name)
                render_load_file(
                    template_path,
                    rendered_path,
                    {
                        "DB_NAME": db,
                        "SOURCE_URI": source_uri,
                        "TARGET_URI": target_uri,
                    },
                )

                pgloader_mode = config.get("pgloader", {}).get("mode", "docker")
                pgloader_binary = config.get("pgloader", {}).get("binary", "pgloader")
                load_file = rendered_name if pgloader_mode == "docker" else rendered_path
                cmd = build_pgloader_command(
                    workspace=self.workspace,
                    load_file=load_file,
                    image=config["pgloader"]["image"],
                    env=config["pgloader"].get("env", {}),
                    mode=pgloader_mode,
                    binary=pgloader_binary,
                )

                show_output = bool(config["pgloader"].get("show_output", False))
                processed = 0
                tail = deque(maxlen=200)
                table_line = re.compile(rf"^\s*{re.escape(db)}\.(\S+)\s+\d+\s+\d+")
                current_table = ""
                had_error = False

                log_fp = None
                if self.log_file_path:
                    log_fp = open(self.log_file_path, "a", encoding="utf-8")

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
                    if log_fp:
                        log_fp.write(line)
                    if "ERROR" in line or "FATAL" in line:
                        had_error = True
                    if show_output:
                        self.queue.put(("log", line))
                    match = table_line.match(line)
                    if match:
                        processed += 1
                        self.overall_processed_tables += 1
                        current_table = match.group(1)
                        self.queue.put((
                            "progress",
                            db,
                            processed,
                            total_tables,
                            idx,
                            total_dbs,
                            self.overall_processed_tables,
                            self.overall_total_tables,
                            current_table,
                        ))

                if log_fp:
                    log_fp.flush()
                    log_fp.close()

                code = process.wait()
                self.current_process = None
                if code != 0 or had_error:
                    self.queue.put(("error", db, list(tail)))
                    return
                self.queue.put(("log", f"同步成功: {db}\n"))

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
            self.log_lines.append(msg[1])
            self._render_log_view()
        elif kind == "progress":
            db, processed, total, idx, total_dbs, overall_done, overall_total, current_table = msg[1:]
            if total > 0:
                percent = min(100, int((processed * 100) / total))
                self.progress.configure(value=percent)
                table_text = f" | {current_table}" if current_table else ""
                self.progress_label.configure(text=f"{db}: {percent}% ({processed}/{total}) [DB {idx}/{total_dbs}]{table_text}")
            else:
                table_text = f" | {current_table}" if current_table else ""
                self.progress_label.configure(text=f"{db}: {processed} tables [DB {idx}/{total_dbs}]{table_text}")

            self._update_eta(overall_done, overall_total)
        elif kind == "error":
            db, tail = msg[1], msg[2]
            self.log_text.insert(tk.END, f"\n同步失败: {db}\n")
            self.log_text.insert(tk.END, "--- pgloader 输出 (最后 200 行) ---\n")
            self.log_text.insert(tk.END, "".join(tail))
            self.log_text.insert(tk.END, "--- 结束 ---\n")
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

    def _append_log(self, text: str) -> None:
        self.queue.put(("log", text))
        if self.log_file_path:
            try:
                with open(self.log_file_path, "a", encoding="utf-8") as f:
                    f.write(text)
            except OSError:
                pass

    def _render_log_view(self) -> None:
        if not self.log_text:
            return
        flt = self.log_filter_var.get().strip().lower()
        if flt:
            lines = [line for line in self.log_lines if flt in line.lower()]
        else:
            lines = self.log_lines
        self.log_text.delete("1.0", tk.END)
        self.log_text.insert(tk.END, "".join(lines))
        self.log_text.see(tk.END)

    def _apply_log_filter(self, _event=None) -> None:
        self._render_log_view()

    def _clear_log_view(self) -> None:
        self.log_lines = []
        if self.log_text:
            self.log_text.delete("1.0", tk.END)

    def _save_log_file(self) -> None:
        if not self.log_lines:
            messagebox.showinfo("日志", "当前没有可保存的日志。")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".log",
            filetypes=[("Log", "*.log"), ("Text", "*.txt")],
            initialdir=self.workspace,
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("".join(self.log_lines))
        except OSError as exc:
            messagebox.showerror("日志", f"保存失败: {exc}")
            return
        messagebox.showinfo("日志", "日志已保存。")

    def _set_idle(self) -> None:
        self.progress.stop()
        self.progress.configure(value=0)
        self.progress_label.configure(text="空闲")
        self.eta_label.configure(text="预计剩余: --")
        self.size_label.configure(text="已选数据库大小: --")
        self.run_button.configure(state=tk.NORMAL)
        self.stop_button.configure(state=tk.DISABLED)
        self.current_process = None
        self.stop_event.clear()
        self._set_controls_running(False)

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
        self.run_button.configure(state=tk.DISABLED if running else tk.NORMAL)
        self.stop_button.configure(state=tk.NORMAL if running else tk.DISABLED)

        self.db_list.configure(state=state)
        self.db_add_btn.configure(state=state)
        self.db_remove_btn.configure(state=state)

        self.config_entry.configure(state=state)
        self.config_browse_btn.configure(state=state)
        self.config_load_btn.configure(state=state)
        self.config_save_btn.configure(state=state)

        self.load_template_entry.configure(state=state)
        self.source_type_combo.configure(state=state)
        self.source_uri_entry.configure(state=state)
        self.target_uri_entry.configure(state=state)
        self.test_source_btn.configure(state=state)
        self.test_target_btn.configure(state=state)
        self.mysql_container_entry.configure(state=state)
        self.mysql_user_entry.configure(state=state)
        self.mysql_password_entry.configure(state=state)
        self.pgloader_image_entry.configure(state=state)
        self.pg_admin_image_entry.configure(state=state)
        self.env_text.configure(state=state)
        self.template_combo.configure(state=state)
        self.template_apply_btn.configure(state=state)
        self.load_text.configure(state=state)
        self.load_reload_btn.configure(state=state)
        self.load_save_btn.configure(state=state)
        self.test_container_check.configure(state=state)
        self.target_user_name_entry.configure(state=state)
        self.target_user_password_entry.configure(state=state)
        self.target_user_auto_check.configure(state=state)
        self.target_admin_uri_entry.configure(state=state)
        # always on

    def _on_db_select(self, _event=None) -> None:
        dbs = [self.db_list.get(i) for i in self.db_list.curselection()]
        if not dbs:
            self.size_label.configure(text="已选数据库大小: --")
            return
        threading.Thread(target=self._compute_size_async, args=(dbs,), daemon=True).start()

    def _compute_size_async(self, dbs: list[str]) -> None:
        if self.source_type.get().strip() != "mysql":
            self.queue.put(("size", 0))
            return
        mysql_container = self.mysql_container.get().strip()
        mysql_user = self.mysql_user.get().strip()
        mysql_password = self.mysql_password.get()

        total_bytes = 0
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
                    "SELECT COALESCE(SUM(data_length + index_length), 0) "
                    f"FROM information_schema.tables WHERE table_schema='{db}';"
                ),
            ]
            result = run_command(cmd)
            if result.returncode != 0:
                continue
            output = (result.stdout or "").strip().splitlines()
            if output:
                try:
                    total_bytes += int(output[0].strip())
                except ValueError:
                    pass

        self.queue.put(("size", total_bytes))

    def _refresh_profiles(self) -> None:
        try:
            files = [f for f in os.listdir(self.workspace) if f.lower().endswith(".json")]
        except OSError:
            files = []
        self.profile_combo.configure(values=files)
        self._sync_profile_selection()

    def _sync_profile_selection(self) -> None:
        current = os.path.basename(self.config_path.get())
        if current in self.profile_combo.cget("values"):
            self.profile_var.set(current)

    def _on_profile_select(self, _event=None) -> None:
        name = self.profile_var.get().strip()
        if not name:
            return
        self.config_path.set(os.path.join(self.workspace, name))
        self._load_config_safe()

    def _apply_template_preset(self) -> None:
        preset = self.template_preset.get()
        content = TEMPLATE_PRESETS.get(preset)
        if not content:
            return
        self.load_text.delete("1.0", tk.END)
        self.load_text.insert(tk.END, content)

    def _on_source_type_change(self, _event=None) -> None:
        is_mysql = self.source_type.get().strip() == "mysql"
        state = tk.NORMAL if is_mysql else tk.DISABLED
        self.mysql_container_entry.configure(state=state)
        self.mysql_user_entry.configure(state=state)
        self.mysql_password_entry.configure(state=state)

    def _on_pgloader_mode_change(self, _event=None) -> None:
        mode = self.pgloader_mode.get().strip()
        if mode == "local":
            self.pgloader_image_entry.configure(state=tk.DISABLED)
            self.pull_pgloader_btn.configure(state=tk.DISABLED)
            self.pgloader_binary_entry.configure(state=tk.NORMAL)
        else:
            self.pgloader_image_entry.configure(state=tk.NORMAL)
            self.pull_pgloader_btn.configure(state=tk.NORMAL)
            self.pgloader_binary_entry.configure(state=tk.DISABLED)

    def _validate_config(self, config: dict) -> list[str]:
        errors: list[str] = []
        load_template = config.get("load_template", "").strip()
        if not load_template:
            errors.append("请填写模板文件路径。")
        template_path = os.path.join(self.workspace, load_template)
        if load_template and not os.path.exists(template_path):
            errors.append("模板文件不存在。")

        source_uri = config.get("source", {}).get("uri", "").strip()
        target_uri = config.get("target", {}).get("uri", "").strip()
        if not source_uri:
            errors.append("请填写源连接 URI。")
        if not target_uri:
            errors.append("请填写目标连接 URI。")

        if load_template and os.path.exists(template_path):
            try:
                with open(template_path, "r", encoding="utf-8") as f:
                    content = f.read()
            except OSError:
                content = ""
            if "{{SOURCE_URI}}" not in content or "{{TARGET_URI}}" not in content:
                errors.append("模板文件需包含 {{SOURCE_URI}} 和 {{TARGET_URI}} 占位符。")

        if config.get("source", {}).get("type", "") == "mysql":
            mysql_cfg = config.get("mysql", {})
            if not mysql_cfg.get("container"):
                errors.append("MySQL 容器名不能为空。")
            if not mysql_cfg.get("user"):
                errors.append("MySQL 用户名不能为空。")

        pgloader_cfg = config.get("pgloader", {})
        mode = pgloader_cfg.get("mode", "docker")
        if mode == "docker":
            if not pgloader_cfg.get("image"):
                errors.append("pgloader 镜像不能为空。")
        else:
            binary = pgloader_cfg.get("binary", "")
            if not binary:
                errors.append("pgloader 路径不能为空。")
            elif not shutil.which(binary):
                errors.append("未找到本地 pgloader，请检查路径或 PATH。")

        target_user = config.get("target_user", {})
        if target_user.get("auto_create", False):
            if not target_user.get("name"):
                errors.append("目标用户名不能为空。")
            if not target_user.get("password"):
                errors.append("目标用户密码不能为空。")
            if not self.pg_admin_image.get().strip():
                errors.append("PG 管理镜像不能为空。")
            if not self.target_admin_uri.get().strip():
                errors.append("管理员 URI 不能为空。")

        return errors

    def _prepare_log_file(self) -> str:
        logs_dir = os.path.join(self.workspace, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(logs_dir, f"run_{timestamp}.log")

    def _test_connection(self, kind: str) -> None:
        threading.Thread(target=self._test_connection_async, args=(kind,), daemon=True).start()

    def _test_connection_async(self, kind: str) -> None:
        uri = self.source_uri.get().strip() if kind == "source" else self.target_uri.get().strip()
        source_type = self.source_type.get().strip()
        if self.test_in_container.get():
            ok, msg = self._check_uri_in_container(uri, source_type, kind == "source")
        else:
            ok, msg = self._check_uri(uri, source_type, kind == "source")
        title = "连接成功" if ok else "连接失败"
        self.after(0, lambda: messagebox.showinfo(title, msg) if ok else messagebox.showerror(title, msg))

    def _check_uri(self, uri: str, source_type: str, is_source: bool) -> tuple[bool, str]:
        if not uri:
            return False, "URI 不能为空。"

        parsed = urlparse(uri)
        scheme = parsed.scheme.lower()

        if scheme in {"http", "https"}:
            return True, "HTTP(S) 源地址格式正常。"

        if scheme == "sqlite":
            path = parsed.path.lstrip("/")
            if not path:
                return False, "SQLite 文件路径为空。"
            if os.path.exists(path):
                return True, "SQLite 文件存在。"
            return False, "SQLite 文件不存在。"

        if scheme == "file" or (is_source and source_type == "file"):
            path = parsed.path.lstrip("/") or uri.replace("file://", "")
            if os.path.exists(path):
                return True, "文件路径存在。"
            return False, "文件路径不存在。"

        host = parsed.hostname
        port = parsed.port
        if not host or not port:
            return False, "无法解析主机或端口。"

        if host == "host.docker.internal":
            return False, "host.docker.internal 仅在容器内可用，请勾选“容器内测试”。"

        try:
            with socket.create_connection((host, port), timeout=3):
                return True, f"可以连接到 {host}:{port}"
        except OSError as exc:
            return False, f"无法连接到 {host}:{port} ({exc})"

    def _check_uri_in_container(self, uri: str, source_type: str, is_source: bool) -> tuple[bool, str]:
        if not uri:
            return False, "URI 不能为空。"

        parsed = urlparse(uri)
        scheme = parsed.scheme.lower()

        if scheme in {"http", "https"}:
            cmd = [
                "docker",
                "run",
                "--rm",
                "busybox:1.36",
                "sh",
                "-c",
                f"wget -q --spider --timeout=3 {uri}",
            ]
            result = run_command(cmd)
            if result.returncode == 0:
                return True, "容器内可访问 HTTP(S) 地址。"
            return False, "容器内无法访问 HTTP(S) 地址。"

        if scheme == "sqlite" or scheme == "file" or (is_source and source_type == "file"):
            return False, "容器内无法直接检查本地文件，请确认已挂载到容器。"

        host = parsed.hostname
        port = parsed.port
        if not host or not port:
            return False, "无法解析主机或端口。"

        cmd = [
            "docker",
            "run",
            "--rm",
            "busybox:1.36",
            "sh",
            "-c",
            f"nc -z -w 3 {host} {port}",
        ]
        result = run_command(cmd)
        if result.returncode == 0:
            return True, f"容器内可连接到 {host}:{port}"
        return False, f"容器内无法连接到 {host}:{port}"

    def _ensure_pg_user(self, config: dict, db: str) -> tuple[bool, str]:
        target_user = config.get("target_user", {})
        if not target_user.get("auto_create", False):
            return True, "skipped"

        username = str(target_user.get("name", "")).strip()
        password = str(target_user.get("password", "")).strip()
        if not username or not password:
            return False, "目标用户或密码为空"
        if any(ch in password for ch in ["@", ":", "/", "?"]):
            self._append_log("Warning: 目标用户密码包含特殊字符，建议改为字母/数字以避免解析差异。\n")

        target_cfg = config.get("target", {})
        admin_uri = str(target_cfg.get("admin_uri", "")).strip()
        if not admin_uri:
            return False, "管理员 URI 为空"
        admin_uri = admin_uri.replace("{{DB_NAME}}", db)
        admin_uri = normalize_uri_credentials(admin_uri)

        image = config.get("pg_admin", {}).get("image", "postgres:14")

        safe_user = username.replace("\"", "\"\"")
        safe_pass = password.replace("'", "''")
        safe_db = db.replace("\"", "\"\"")
        sql = (
            "DO $$ BEGIN "
            f"IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{safe_user}') THEN "
            f"ALTER ROLE \"{safe_user}\" WITH LOGIN PASSWORD '{safe_pass}'; "
            "ELSE "
            f"CREATE ROLE \"{safe_user}\" LOGIN PASSWORD '{safe_pass}'; "
            "END IF; END $$;\n"
            f"GRANT ALL PRIVILEGES ON DATABASE \"{safe_db}\" TO \"{safe_user}\";\n"
            "DO $$ BEGIN "
            f"EXECUTE 'GRANT USAGE ON SCHEMA public TO \"{safe_user}\"'; "
            f"EXECUTE 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"{safe_user}\"'; "
            f"EXECUTE 'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"{safe_user}\"'; "
            f"EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"{safe_user}\"'; "
            f"EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO \"{safe_user}\"'; "
            "END $$;\n"
        )

        sql_name = f".pg_admin_{db}.sql"
        sql_path = os.path.join(self.workspace, sql_name)
        try:
            with open(sql_path, "w", encoding="utf-8") as f:
                f.write(sql)
        except OSError as exc:
            return False, f"写入 SQL 失败: {exc}"

        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{self.workspace}:/work",
            image,
            "psql",
            admin_uri,
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            f"/work/{sql_name}",
        ]
        self._append_log(f"Ensuring PG user: {username}\n")
        self._append_log(f"Admin URI: {self._redact_uri(admin_uri)}\n")
        self._append_log(f"Admin image: {image}\n")
        result = run_command(cmd)
        if result.returncode != 0:
            if result.stdout:
                self._append_log(result.stdout)
            if result.stderr:
                self._append_log(result.stderr)
            return False, (result.stdout or result.stderr or "psql failed").strip()
        self._append_log("PG user ensured and privileges granted.\n")
        return True, "PG 用户已确保存在并可使用。"

    def _test_target_connection(self, target_uri: str, config: dict) -> tuple[bool, str]:
        image = config.get("pg_admin", {}).get("image", "postgres:14")
        cmd = [
            "docker",
            "run",
            "--rm",
            image,
            "psql",
            target_uri,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "SELECT 1;",
        ]
        self._append_log(f"Testing target connection: {self._redact_uri(target_uri)}\n")
        self._append_log(f"Test image: {image}\n")
        result = run_command(cmd)
        if result.returncode != 0:
            if result.stdout:
                self._append_log(result.stdout)
            if result.stderr:
                self._append_log(result.stderr)
            return False, (result.stdout or result.stderr or "psql failed").strip()
        self._append_log("Target connection OK.\n")
        return True, "ok"

    def _redact_uri(self, uri: str) -> str:
        try:
            parsed = urlparse(uri)
            if not parsed.scheme or not parsed.netloc:
                return uri
            netloc = parsed.netloc
            if "@" in netloc:
                userinfo, hostinfo = netloc.split("@", 1)
                if ":" in userinfo:
                    user = userinfo.split(":", 1)[0]
                    netloc = f"{user}:***@{hostinfo}"
            return parsed._replace(netloc=netloc).geturl()
        except Exception:
            return uri

    def _apply_target_user_uri(self, target_uri: str, target_user: dict) -> str:
        username = str(target_user.get("name", "")).strip()
        password = str(target_user.get("password", "")).strip()
        if not username or not password:
            return target_uri

        try:
            parsed = urlparse(target_uri)
            if not parsed.scheme or not parsed.netloc:
                return target_uri

            host = parsed.hostname or ""
            port = f":{parsed.port}" if parsed.port else ""
            user = quote(username, safe="")
            pwd = quote(password, safe="")
            netloc = f"{user}:{pwd}@{host}{port}"
            return parsed._replace(netloc=netloc).geturl()
        except Exception:
            return target_uri

    def _check_update(self) -> None:
        def worker() -> None:
            url = "https://api.github.com/repos/cwxiao/FastToPG/releases/latest"
            try:
                req = Request(url, headers={"User-Agent": "FastToPG"})
                with urlopen(req, timeout=6) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                latest = (data.get("tag_name") or "").lstrip("v")
                if not latest:
                    raise ValueError("无法解析版本")
                if self._compare_versions(APP_VERSION, latest) >= 0:
                    msg = f"已是最新版本 (v{APP_VERSION})"
                else:
                    msg = f"发现新版本 v{latest}"
                self.after(0, lambda: messagebox.showinfo("更新检查", msg))
            except Exception as exc:
                self.after(0, lambda: messagebox.showerror("更新检查失败", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _show_startup_tips(self) -> None:
        tips = (
            "使用指引:\n"
            "1) 基础设置里填写源/目标 URI，选择数据库。\n"
            "2) 高级设置里可选择 pgloader 运行方式(Docker/本地)。\n"
            "3) Docker 模式需要可用的 Docker 环境，可点“检查 Docker”。\n"
            "4) 未拉取镜像可点“拉取 pgloader 镜像/PG 管理镜像”。\n"
            "5) 如使用 host.docker.internal，请勾选“容器内测试”。\n"
        )
        messagebox.showinfo("FastToPG 使用提示", tips)

    def _check_docker(self) -> None:
        def worker() -> None:
            result = run_command(["docker", "info"])
            if result.returncode == 0:
                self.after(0, lambda: messagebox.showinfo("Docker", "Docker 可用。"))
            else:
                err = (result.stderr or result.stdout or "Docker 不可用").strip()
                self.after(0, lambda: messagebox.showerror("Docker", err))

        threading.Thread(target=worker, daemon=True).start()

    def _pull_pgloader_image(self) -> None:
        image = self.pgloader_image.get().strip()
        if not image:
            messagebox.showerror("错误", "pgloader 镜像不能为空。")
            return
        self._run_docker_pull(image)

    def _pull_pgadmin_image(self) -> None:
        image = self.pg_admin_image.get().strip()
        if not image:
            messagebox.showerror("错误", "PG 管理镜像不能为空。")
            return
        self._run_docker_pull(image)

    def _run_docker_pull(self, image: str) -> None:
        def worker() -> None:
            check = run_command(["docker", "info"])
            if check.returncode != 0:
                err = (check.stderr or check.stdout or "Docker 不可用").strip()
                self.after(0, lambda: messagebox.showerror("Docker", err))
                return

            self._append_log(f"Pulling image: {image}\n")
            process = subprocess.Popen(
                ["docker", "pull", image],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            assert process.stdout is not None
            for line in process.stdout:
                self._append_log(line)
            code = process.wait()
            if code == 0:
                self._append_log(f"Image pulled: {image}\n")
            else:
                self._append_log(f"Image pull failed: {image}\n")

        threading.Thread(target=worker, daemon=True).start()

    def _compare_versions(self, current: str, latest: str) -> int:
        def parse(v: str) -> list[int]:
            return [int(x) for x in re.findall(r"\d+", v)]

        cur = parse(current)
        lat = parse(latest)
        for a, b in zip(cur, lat, strict=False):
            if a != b:
                return 1 if a > b else -1
        if len(cur) == len(lat):
            return 0
        return 1 if len(cur) > len(lat) else -1

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
