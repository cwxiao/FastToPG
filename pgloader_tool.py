import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import json
import os
import re
import subprocess
import sys
import threading
import time
from collections import deque
from urllib.parse import unquote, urlparse
from typing import Dict, List, Optional


def should_skip_table(table: str, keywords: List[str]) -> bool:
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


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_command(
    cmd: List[str],
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)


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


def render_load_file(template_path: str, output_path: str, replacements: Dict[str, str]) -> None:
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
    env: Dict[str, str],
) -> List[str]:
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


def parse_db_uri(uri: str) -> Dict[str, str | int]:
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


def clear_target_public_tables(db: str, config: Dict) -> int:
    target_cfg = config.get("target", {})
    target_uri = target_cfg.get("uri", "").replace("{{DB_NAME}}", db)
    target = parse_db_uri(target_uri)

    if target.get("scheme") not in ("postgresql", "pgsql", "postgres"):
        print(f"Skip clear public: unsupported target scheme {target.get('scheme')}")
        return 1

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

    print(f"Clearing target public tables: {target.get('database', '')}")
    container_result = run_command(container_cmd)
    if container_result.returncode == 0:
        return 0

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
    if result.returncode != 0:
        if container_result.stdout:
            sys.stdout.write(container_result.stdout)
        if container_result.stderr:
            sys.stdout.write(container_result.stderr)
        if result.stdout:
            sys.stdout.write(result.stdout)
        if result.stderr:
            sys.stdout.write(result.stderr)
    return result.returncode


def get_mysql_tables(mysql_container: str, user: str, password: str, db: str) -> List[str]:
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


def get_mysql_columns(mysql_container: str, user: str, password: str, db: str, table: str) -> List[str]:
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


def get_mysql_split_pk(mysql_container: str, user: str, password: str, db: str, table: str) -> Optional[str]:
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


def mysql_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def pg_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_datax_job(
    workspace: str,
    db: str,
    table: str,
    columns: List[str],
    source_uri: str,
    target_uri: str,
    datax_cfg: Dict,
    split_pk: Optional[str] = None,
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


def build_datax_command(job_file: str, datax_cfg: Dict) -> List[str]:
    datax_home = (datax_cfg.get("home") or "").strip()
    if not datax_home:
        raise ValueError("DataX config missing: datax.home")

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


def is_cleanup_jobs_on_finish(datax_cfg: Dict) -> bool:
    return bool(datax_cfg.get("cleanup_jobs_on_finish", datax_cfg.get("cleanup_jobs_on_success", True)))


def cleanup_datax_jobs_for_db(workspace: str, db: str, datax_cfg: Dict) -> None:
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


def cleanup_empty_datax_job_dir(workspace: str, datax_cfg: Dict) -> None:
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


def cleanup_old_logs(workspace: str, log_dirs: List[str], retention_days: int) -> None:
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


def cleanup_datax_logs_by_retention(workspace: str, datax_cfg: Dict) -> None:
    retention_days_raw = datax_cfg.get("log_retention_days", 7)
    try:
        retention_days = int(retention_days_raw)
    except (TypeError, ValueError):
        retention_days = 7
    log_dirs = datax_cfg.get("log_dirs", ["datax/datax/log", "datax/datax/log_perf"])
    if not isinstance(log_dirs, list):
        return
    cleanup_old_logs(workspace, [str(item) for item in log_dirs], retention_days)


def run_datax_for_db(db: str, config: Dict, workspace: str) -> int:
    datax_cfg = config.get("datax", {})
    if not bool(datax_cfg.get("enabled", False)):
        return 0

    datax_home = (datax_cfg.get("home") or "").strip()
    if not datax_home:
        print("DataX skipped: missing datax.home in config")
        return 1

    datax_py = os.path.join(datax_home, "bin", "datax.py")
    if not os.path.isfile(datax_py):
        print(f"DataX skipped: not found {datax_py}")
        return 1

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
    if not tables:
        print(f"DataX skipped: no tables found in {db}")
        return 0

    cleanup_on_finish = is_cleanup_jobs_on_finish(datax_cfg)

    if cleanup_on_finish:
        cleanup_datax_jobs_for_db(workspace, db, datax_cfg)

    exclude_keywords = datax_cfg.get("exclude_table_keywords", [])
    if not isinstance(exclude_keywords, list):
        exclude_keywords = []
    filtered_tables = [table for table in tables if not should_skip_table(table, exclude_keywords)]
    skipped_by_rule = len(tables) - len(filtered_tables)
    tables = filtered_tables

    if not tables:
        print(f"DataX skipped: no tables left after exclude rule in {db}")
        return 0

    print(f"DataX start: {db}, tables={len(tables)}")
    if skipped_by_rule > 0:
        print(f"DataX skip-by-rule: {skipped_by_rule} tables")
    show_output = bool(datax_cfg.get("show_output", False))
    compact_log = bool(datax_cfg.get("compact_log", True))

    table_parallelism = max(1, int(datax_cfg.get("table_parallelism", 3)))
    print(f"DataX table parallelism: {table_parallelism}")
    output_lock = threading.Lock()

    def run_one_table(idx: int, table: str) -> tuple[int, str, List[str], str]:
        columns = get_mysql_columns(
            mysql_cfg.get("container", ""),
            mysql_cfg.get("user", ""),
            mysql_cfg.get("password", ""),
            db,
            table,
        )
        if not columns:
            with output_lock:
                print(f"DataX skipped table: {db}.{table} (no columns)")
            return 0, table, [], ""

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
        job_file = build_datax_job(workspace, db, table, columns, source_uri, target_uri, datax_cfg, split_pk=split_pk)
        cmd = build_datax_command(job_file, datax_cfg)
        with output_lock:
            print(f"DataX [{idx}/{len(tables)}] {db}.{table} (channel={channel}, batch={batch_size}, splitPk={split_pk_disp})")

        cmd_env = os.environ.copy()
        for key, value in datax_cfg.get("env", {}).items():
            cmd_env[str(key)] = str(value)

        tail = deque(maxlen=200)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=workspace,
            env=cmd_env,
            encoding="utf-8",
            errors="replace",
        )
        try:
            assert process.stdout is not None
            for line in process.stdout:
                tail.append(line)
                if show_output:
                    if compact_log:
                        if is_datax_key_log(line):
                            with output_lock:
                                sys.stdout.write(line)
                    else:
                        with output_lock:
                            sys.stdout.write(line)
            code = process.wait()
        finally:
            if cleanup_on_finish:
                cleanup_datax_job_file(job_file)
        return code, table, list(tail), job_file

    first_error: tuple[int, str, List[str], str] | None = None
    with ThreadPoolExecutor(max_workers=table_parallelism) as executor:
        futures = {
            executor.submit(run_one_table, idx, table): (idx, table)
            for idx, table in enumerate(tables, start=1)
        }
        for future in as_completed(futures):
            code, table, tail, job_file = future.result()
            if code != 0 and first_error is None:
                first_error = (code, table, tail, job_file)
                for pending in futures:
                    pending.cancel()

    if first_error is not None:
        code, table, tail, _ = first_error
        sys.stdout.write(f"\nDataX failed table: {db}.{table}\n")
        sys.stdout.write("--- DataX output (last 200 lines) ---\n")
        sys.stdout.writelines(tail)
        sys.stdout.write("--- end ---\n")
        return code

    if cleanup_on_finish:
        cleanup_empty_datax_job_dir(workspace, datax_cfg)

    print(f"DataX success: {db}")
    return 0


def print_progress(db: str, processed: int, total: int) -> None:
    if total > 0:
        percent = min(100, int((processed * 100) / total))
        bar_len = 30
        filled = int(bar_len * percent / 100)
        bar = "#" * filled + "-" * (bar_len - filled)
        msg = f"{db} [{bar}] {percent}% ({processed}/{total} tables)"
    else:
        msg = f"{db} {processed} tables processed"
    sys.stdout.write("\r" + msg + " " * 10)
    sys.stdout.flush()


def run_pgloader_for_db(
    db: str,
    config: Dict,
    workspace: str,
) -> int:
    mysql_cfg = config.get("mysql", {})
    pgloader_cfg = config["pgloader"]
    source_cfg = config.get("source", {})
    target_cfg = config.get("target", {})
    source_type = source_cfg.get("type", "mysql")

    if bool(pgloader_cfg.get("clear_public_before_sync", True)):
        clear_code = clear_target_public_tables(db, config)
        if clear_code != 0:
            print(f"Failed to clear target public tables: {db}")
            return clear_code

    total_tables = 0
    if source_type == "mysql":
        total_tables = get_total_tables(
            mysql_cfg.get("container", ""),
            mysql_cfg.get("user", ""),
            mysql_cfg.get("password", ""),
            db,
        )

    source_uri = source_cfg.get("uri", "").replace("{{DB_NAME}}", db)
    target_uri = target_cfg.get("uri", "").replace("{{DB_NAME}}", db)

    template_path = os.path.join(workspace, config["load_template"])
    rendered_name = f".pgloader_rendered_{db}.load"
    rendered_path = os.path.join(workspace, rendered_name)
    cleanup_pgloader_temp = bool(pgloader_cfg.get("cleanup_temp_files", True))
    if cleanup_pgloader_temp:
        cleanup_pgloader_rendered_files_for_db(workspace, db)

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
        workspace=workspace,
        load_file=rendered_name,
        image=pgloader_cfg["image"],
        env=pgloader_cfg.get("env", {}),
    )

    show_output = bool(pgloader_cfg.get("show_output", False))
    processed = 0
    tail = deque(maxlen=200)
    table_line = re.compile(rf"^\s*{re.escape(db)}\.\S+\s+\d+\s+\d+")
    has_log_error = False

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=workspace,
        encoding="utf-8",
        errors="replace",
    )

    try:
        assert process.stdout is not None
        for line in process.stdout:
            tail.append(line)
            if show_output:
                sys.stdout.write(line)
            if is_pgloader_error_log(line):
                has_log_error = True
            if table_line.match(line):
                processed += 1
                print_progress(db, processed, total_tables)

        code = process.wait()
    finally:
        if cleanup_pgloader_temp:
            cleanup_pgloader_rendered_file(rendered_path)
    if has_log_error:
        code = 1
    if not show_output:
        sys.stdout.write("\r")
        sys.stdout.flush()
    print_progress(db, processed, total_tables)
    sys.stdout.write("\n")
    sys.stdout.flush()

    if code != 0:
        sys.stdout.write("\n--- pgloader output (last 200 lines) ---\n")
        sys.stdout.writelines(tail)
        sys.stdout.write("--- end ---\n")
        tail_text = "".join(tail)
        if "max_locks_per_transaction" in tail_text:
            sys.stdout.write(
                "Tip: PostgreSQL 提示 max_locks_per_transaction 不足。"
                "当前模板已移除 include drop 以降低锁压力；"
                "如仍失败，请在目标库提升该参数并重启 PostgreSQL。\n"
            )

    return code


def main() -> int:
    parser = argparse.ArgumentParser(description="pgloader helper with progress")
    parser.add_argument(
        "--config",
        default="pgloader_tool.json",
        help="Path to config JSON file",
    )
    parser.add_argument(
        "--db",
        action="append",
        help="Database name to sync (can be repeated)",
    )
    parser.add_argument(
        "--action",
        choices=["structure", "data"],
        default="structure",
        help="Run only structure sync (pgloader) or only data sync (DataX)",
    )
    args = parser.parse_args()

    workspace = os.path.abspath(os.path.dirname(__file__))
    config = load_config(os.path.join(workspace, args.config))
    cleanup_datax_logs_by_retention(workspace, config.get("datax", {}))

    databases = args.db if args.db else config.get("databases", [])
    if not databases:
        print("No databases configured.")
        return 1

    for db in databases:
        print("=" * 30)
        print(f"Sync database: {db}")
        print("=" * 30)
        if args.action == "structure":
            code = run_pgloader_for_db(db, config, workspace)
            if code != 0:
                print(f"Structure sync failed: {db}")
                return code
            print(f"Structure sync success: {db}")
        else:
            code = run_datax_for_db(db, config, workspace)
            if code != 0:
                print(f"Data sync failed: {db}")
                return code
            print(f"Data sync success: {db}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
