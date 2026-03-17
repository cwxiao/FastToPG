import glob
import json
import os
import time

from .common import parse_db_uri


def mysql_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"

def pg_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def build_mysql_query_sql(table: str, column_defs: list[dict[str, str]]) -> str:
    select_items: list[str] = []
    for column in column_defs:
        name = str(column.get("name", "")).strip()
        data_type = str(column.get("data_type", "")).strip().lower()
        if not name:
            continue
        ident = mysql_ident(name)
        if data_type == "json":
            select_items.append(f"CAST({ident} AS CHAR) AS {ident}")
        else:
            select_items.append(ident)
    return f"SELECT {', '.join(select_items)} FROM {mysql_ident(table)}"

def has_mysql_json_columns(column_defs: list[dict[str, str]]) -> bool:
    return any(str(column.get("data_type", "")).strip().lower() == "json" for column in column_defs)

def build_datax_job(
    workspace: str,
    db: str,
    table: str,
    columns: list[str],
    source_uri: str,
    target_uri: str,
    datax_cfg: dict,
    split_pk: str | None = None,
    column_defs: list[dict[str, str]] | None = None,
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
    normalized_column_defs = column_defs or [{"name": col, "data_type": ""} for col in columns]
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
                            "connection": [
                                {
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

    reader_parameter = job["job"]["content"][0]["reader"]["parameter"]
    reader_connection = reader_parameter["connection"][0]
    if has_mysql_json_columns(normalized_column_defs):
        reader_connection["querySql"] = [build_mysql_query_sql(table, normalized_column_defs)]
    else:
        reader_parameter["column"] = reader_columns
        reader_connection["table"] = [mysql_ident(table)]

    if split_pk and "querySql" not in reader_connection:
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

def resolve_datax_home(workspace: str, datax_cfg: dict) -> str:
    home = (datax_cfg.get("home") or "").strip()
    if not home:
        return ""
    if os.path.isabs(home):
        return home
    return os.path.join(workspace, home)

def is_datax_jvm_oom(text: str) -> bool:
    low = (text or "").lower()
    return (
        "insufficient memory for the java runtime environment" in low
        or "native memory allocation (mmap) failed" in low
        or "errno=1455" in low
    )

