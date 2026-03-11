import json
import os
import queue
import re
import subprocess
import threading
import time
import tkinter as tk
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
import glob
from collections import deque
from tkinter import ttk, messagebox, filedialog
from urllib.parse import quote, unquote, urlparse

DEFAULT_CONFIG = "pgloader_tool.json"
SYNC_HISTORY_FILE = "sync_history.json"
URI_HISTORY_FILE = "uri_history.json"
SOURCE_TYPES = ["mysql", "mssql", "sqlite", "pgsql", "redshift", "file"]


def should_skip_table(table: str, keywords: list[str]) -> bool:
    name = table.lower()
    for keyword in keywords:
        token = keyword.strip().lower()
        if token and token in name:
            return True
    return False


def parse_selected_tables(values: list[str] | None) -> list[str]:
    if not values:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        for token in str(raw).split(","):
            name = token.strip()
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(name)
    return result


def filter_tables_by_selected(tables: list[str], selected_tables: list[str]) -> tuple[list[str], list[str]]:
    if not selected_tables:
        return list(tables), []
    selected_map = {name.lower(): name for name in selected_tables}
    selected_keys = set(selected_map.keys())
    filtered = [table for table in tables if table.lower() in selected_keys]
    existed_keys = {table.lower() for table in filtered}
    missing = [selected_map[key] for key in selected_keys if key not in existed_keys]
    missing.sort()
    return filtered, missing


def build_pgloader_table_filter_clause(tables: list[str]) -> str:
    if not tables:
        return ""
    lines = ["INCLUDING ONLY TABLE NAMES MATCHING"]
    for idx, table in enumerate(tables):
        regex = "^" + re.escape(table).replace("/", r"\/") + "$"
        suffix = "," if idx < len(tables) - 1 else ""
        lines.append(f"     ~/{regex}/{suffix}")
    return "\n" + "\n".join(lines) + "\n"


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
    # Force UTF-8 decoding and replace invalid bytes to avoid locale decode errors from docker exec output
    return subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def normalize_db_uri(uri: str) -> str:
    text = (uri or "").strip()
    if not text:
        return ""
    return re.sub(r"^([a-zA-Z][a-zA-Z0-9+.-]*):/(?!/)", r"\1://", text)


def mask_uri_password(uri: str) -> str:
    text = normalize_db_uri(uri)
    if not text:
        return ""
    parsed = urlparse(text)
    if not parsed.scheme:
        return text
    user = parsed.username or ""
    password = parsed.password
    host = parsed.hostname or ""
    port = parsed.port
    db = (parsed.path or "").lstrip("/")
    auth = user
    if password is not None:
        auth = f"{user}:***" if user else "***"
    netloc = auth + ("@" if auth else "") + host + (f":{port}" if port else "")
    return f"{parsed.scheme}://{netloc}/{db}"


def build_mysql_uri_from_conn(conn: dict[str, str | int], db: str) -> str:
    user = quote(str(conn.get("user", "")), safe="")
    password = quote(str(conn.get("password", "")), safe="")
    host = (str(conn.get("host", "")) or "127.0.0.1").strip()
    port_raw = conn.get("port", 3306)
    port = int(port_raw or 3306)
    auth = user if not password else f"{user}:{password}"
    return f"mysql://{auth}@{host}:{port}/{db}"


def resolve_mysql_conn(config: dict, db: str) -> dict[str, str | int]:
    mysql_cfg = config.get("mysql", {})
    source_uri = normalize_db_uri((config.get("source", {}).get("uri") or "").replace("{{DB_NAME}}", db))

    host = ""
    port = 0
    user = str(mysql_cfg.get("user", ""))
    password = str(mysql_cfg.get("password", ""))

    try:
        source = parse_db_uri(source_uri)
        if str(source.get("scheme", "")).lower() == "mysql":
            host = str(source.get("host", "") or "")
            port = int(source.get("port", 0) or 0)
            user = str(source.get("user", "") or user)
            password = str(source.get("password", "") or password)
    except Exception:
        pass

    return {
        "container": str(mysql_cfg.get("container", "")),
        "host": host,
        "port": port,
        "user": user,
        "password": password,
    }


def get_total_tables(mysql_container: str, user: str, password: str, db: str, host: str = "", port: int = 0) -> int:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
    ]
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-P", str(port)])
    cmd.extend([
        "-e",
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='{db}';",
    ])
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

    table_filter = (replacements.get("TABLE_FILTER") or "").strip()
    if table_filter and "{{TABLE_FILTER}}" not in content:
        cast_index = content.find("\nCAST")
        if cast_index >= 0:
            content = content[:cast_index] + "\n" + table_filter + "\n" + content[cast_index:]
        else:
            content = content.rstrip() + "\n\n" + table_filter + "\n"

    for key, value in replacements.items():
        content = content.replace(f"{{{{{key}}}}}", value)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)


def patch_rendered_load_for_full_sync(path: str) -> None:
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return

    updated = re.sub(r"(^\s*)foreign\s+keys\s*,\s*$", r"\1no foreign keys,", content, flags=re.IGNORECASE | re.MULTILINE)
    if updated == content:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(updated)
    except Exception:
        pass


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


def pg_quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def _make_psql_exec_cmd(psql_container: str, pg_password: str, pg_host: str, pg_port: str,
                         pg_user: str, pg_db: str, sql: str) -> list:
    return [
        "docker", "exec",
        "-e", f"PGPASSWORD={pg_password}",
        psql_container,
        "psql",
        "-h", pg_host,
        "-p", pg_port,
        "-U", pg_user,
        "-d", pg_db,
        "-v", "ON_ERROR_STOP=1",
        "-q",
        "-c", sql,
    ]


def run_psql_container_sql(target: dict, target_cfg: dict, sql: str) -> tuple[int, str]:
    pg_user = str(target.get("user", ""))
    pg_db = str(target.get("database", ""))
    pg_host = str(target.get("host", ""))
    pg_port = str(target.get("port", 5432) or 5432)
    pg_password = str(target.get("password", ""))
    psql_container = (target_cfg.get("psql_container") or "postgres16").strip()

    # First try with the configured host
    exec_cmd = _make_psql_exec_cmd(psql_container, pg_password, pg_host, pg_port, pg_user, pg_db, sql)
    exec_result = run_command(exec_cmd)
    if exec_result.returncode == 0:
        return 0, (exec_result.stdout or "") + (exec_result.stderr or "")

    first_error = (exec_result.stdout or "") + (exec_result.stderr or "")

    # Only fallback to host.docker.internal if the failure is a connection error,
    # not a SQL/auth error from PG itself (which means connection already worked).
    is_conn_error = any(tok in first_error.lower() for tok in (
        "could not connect", "connection to server", "connection refused",
        "no route to host", "network is unreachable", "name or service not known",
    ))

    fallback_error = ""
    if is_conn_error and pg_host not in ("host.docker.internal", "localhost", "127.0.0.1"):
        fallback_cmd = _make_psql_exec_cmd(psql_container, pg_password, "host.docker.internal", pg_port, pg_user, pg_db, sql)
        fallback_result = run_command(fallback_cmd)
        if fallback_result.returncode == 0:
            return 0, (fallback_result.stdout or "") + (fallback_result.stderr or "")
        fallback_error = (fallback_result.stdout or "") + (fallback_result.stderr or "")

    output_parts = [f"容器 psql（docker exec）执行失败，container={psql_container}。\n"]
    if first_error.strip():
        output_parts.append(f"  错误：{first_error.strip()}\n")
    if fallback_error.strip():
        output_parts.append(f"  尝试 host.docker.internal:{pg_port} 错误：{fallback_error.strip()}\n")
    return exec_result.returncode, "".join(output_parts)


def build_clear_public_sql(selected_tables: list[str] | None = None) -> str:
    selected = parse_selected_tables(selected_tables)
    if not selected:
        return (
            "DO $$ "
            "DECLARE r record; "
            "BEGIN "
            "FOR r IN SELECT tablename FROM pg_tables WHERE schemaname='public' LOOP "
            "EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', r.tablename); "
            "END LOOP; "
            "END $$;"
        )

    stmts = [f"DROP TABLE IF EXISTS public.{pg_quote_ident(table)} CASCADE;" for table in selected]
    return "\n".join(stmts)


def clear_target_public_tables(db: str, config: dict, selected_tables: list[str] | None = None) -> tuple[int, str]:
    target_cfg = config.get("target", {})
    target_uri = target_cfg.get("uri", "").replace("{{DB_NAME}}", db)
    target = parse_db_uri(target_uri)

    if target.get("scheme") not in ("postgresql", "pgsql", "postgres"):
        return 1, f"Skip clear public: unsupported target scheme {target.get('scheme')}\n"

    sql = build_clear_public_sql(selected_tables)

    selected = parse_selected_tables(selected_tables)
    prefix = (
        f"Clearing selected target tables: {target.get('database', '')}, tables={len(selected)}\n"
        if selected
        else f"Clearing target public tables: {target.get('database', '')}\n"
    )
    code, out = run_psql_container_sql(target, target_cfg, sql)
    if code == 0:
        return 0, prefix + out
    return code, prefix + out


def build_clear_public_views_sql() -> str:
    return (
        "DO $$ "
        "DECLARE r record; "
        "BEGIN "
        "FOR r IN SELECT viewname FROM pg_views WHERE schemaname='public' LOOP "
        "EXECUTE format('DROP VIEW IF EXISTS public.%I CASCADE', r.viewname); "
        "END LOOP; "
        "END $$;"
    )


def clear_target_public_views(db: str, config: dict) -> tuple[int, str]:
    target_cfg = config.get("target", {})
    target_uri = target_cfg.get("uri", "").replace("{{DB_NAME}}", db)
    target = parse_db_uri(target_uri)

    if target.get("scheme") not in ("postgresql", "pgsql", "postgres"):
        return 1, f"Skip clear public views: unsupported target scheme {target.get('scheme')}\n"

    sql = build_clear_public_views_sql()
    prefix = f"清理目标库 public 视图: {target.get('database', '')}\n"
    code, out = run_psql_container_sql(target, target_cfg, sql)
    if code == 0:
        return 0, prefix + out
    return code, prefix + out


def get_mysql_tables(mysql_container: str, user: str, password: str, db: str, host: str = "", port: int = 0) -> list[str]:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
    ]
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-P", str(port)])
    cmd.extend([
        "-e",
        (
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema='{db}' AND table_type='BASE TABLE' ORDER BY table_name;"
        ),
    ])
    result = run_command(cmd)
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def get_mysql_views(mysql_container: str, user: str, password: str, db: str, host: str = "", port: int = 0) -> list[str]:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
    ]
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-P", str(port)])
    cmd.extend([
        "-e",
        (
            "SELECT table_name FROM information_schema.views "
            f"WHERE table_schema='{db}' ORDER BY table_name;"
        ),
    ])
    result = run_command(cmd)
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def get_mysql_view_definition(
    mysql_container: str,
    user: str,
    password: str,
    db: str,
    view_name: str,
    host: str = "",
    port: int = 0,
) -> str:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
    ]
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-P", str(port)])
    cmd.extend([
        "-e",
        (
            "SELECT view_definition FROM information_schema.views "
            f"WHERE table_schema='{db}' AND table_name='{view_name}';"
        ),
    ])
    result = run_command(cmd)
    if result.returncode != 0:
        return ""
    lines = [line.rstrip("\r") for line in (result.stdout or "").splitlines() if line.strip()]
    if not lines:
        return ""
    return "\n".join(lines)


def transform_mysql_view_definition(view_sql: str, source_db: str) -> str:
    transformed = (view_sql or "").strip().rstrip(";")
    if not transformed:
        return ""

    transformed = re.sub(rf"`{re.escape(source_db)}`\.", "", transformed, flags=re.IGNORECASE)
    transformed = re.sub(rf"\b{re.escape(source_db)}\.", "", transformed, flags=re.IGNORECASE)

    def _replace_bt_ident(match: re.Match[str]) -> str:
        return pg_quote_ident(match.group(1).lower())

    def _replace_year_func(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        return f"EXTRACT(YEAR FROM {expr})"

    def _replace_month_func(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        return f"EXTRACT(MONTH FROM {expr})"

    def _replace_quarter_func(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        return f"EXTRACT(QUARTER FROM {expr})"

    transformed = re.sub(r"`([^`]+)`", _replace_bt_ident, transformed)
    transformed = re.sub(r"\bIFNULL\(", "COALESCE(", transformed, flags=re.IGNORECASE)
    transformed = re.sub(r"\bYEAR\s*\(\s*([^\(\)]+?)\s*\)", _replace_year_func, transformed, flags=re.IGNORECASE)
    transformed = re.sub(r"\bMONTH\s*\(\s*([^\(\)]+?)\s*\)", _replace_month_func, transformed, flags=re.IGNORECASE)
    transformed = re.sub(r"\bQUARTER\s*\(\s*([^\(\)]+?)\s*\)", _replace_quarter_func, transformed, flags=re.IGNORECASE)
    return transformed


def sync_views_for_db(db: str, config: dict) -> tuple[int, str]:
    mysql_conn = resolve_mysql_conn(config, db)
    views = get_mysql_views(
        str(mysql_conn.get("container", "")),
        str(mysql_conn.get("user", "")),
        str(mysql_conn.get("password", "")),
        db,
        host=str(mysql_conn.get("host", "")),
        port=int(mysql_conn.get("port", 0) or 0),
    )
    if not views:
        return 0, f"View sync skipped: no views found in {db}\n"

    output_lines: list[str] = [f"View sync start: {db}, views={len(views)}\n"]
    output_lines.append(f"Source view list ({db}): {', '.join(views)}\n")
    target_cfg = config.get("target", {})
    target_uri = target_cfg.get("uri", "").replace("{{DB_NAME}}", db)
    target = parse_db_uri(target_uri)

    if target.get("scheme") not in ("postgresql", "pgsql", "postgres"):
        return 1, f"Skip view sync: unsupported target scheme {target.get('scheme')}\n"

    view_sql_map: dict[str, str] = {}
    for view_name in views:
        raw_definition = get_mysql_view_definition(
            str(mysql_conn.get("container", "")),
            str(mysql_conn.get("user", "")),
            str(mysql_conn.get("password", "")),
            db,
            view_name,
            host=str(mysql_conn.get("host", "")),
            port=int(mysql_conn.get("port", 0) or 0),
        )
        if not raw_definition:
            output_lines.append(f"View sync skipped: {db}.{view_name} (empty definition)\n")
            continue

        definition = transform_mysql_view_definition(raw_definition, db)
        if not definition:
            output_lines.append(f"View sync skipped: {db}.{view_name} (unsupported definition)\n")
            continue

        view_sql_map[view_name] = definition

    if not view_sql_map:
        output_lines.append(f"View sync skipped: no valid view definitions in {db}\n")
        return 0, "".join(output_lines)

    pending = list(view_sql_map.keys())
    total_valid = len(pending)
    synced_count = 0
    round_no = 0

    while pending:
        round_no += 1
        round_progress = 0
        next_pending: list[str] = []
        failed_output_by_view: dict[str, str] = {}
        output_lines.append(f"View sync round {round_no}: pending={len(pending)}\n")

        for view_name in pending:
            definition = view_sql_map[view_name]

            view_ident = pg_quote_ident(view_name.lower())
            sql = f"CREATE OR REPLACE VIEW public.{view_ident} AS {definition};"
            code, out = run_psql_container_sql(target, target_cfg, sql)
            if code != 0:
                next_pending.append(view_name)
                failed_output_by_view[view_name] = out
                continue

            synced_count += 1
            round_progress += 1
            output_lines.append(f"View synced: {db}.{view_name} [{synced_count}/{total_valid}]\n")

        if not next_pending:
            output_lines.append(f"View sync success: {db}\n")
            return 0, "".join(output_lines)

        if round_progress == 0:
            first_failed = next_pending[0]
            unresolved = ", ".join(next_pending)
            output_lines.append(f"View sync failed: unresolved dependent or incompatible views in {db}: {unresolved}\n")
            detail = failed_output_by_view.get(first_failed, "")
            if detail:
                output_lines.append(detail)
            return 1, "".join(output_lines)

        pending = next_pending

    output_lines.append(f"View sync success: {db}\n")
    return 0, "".join(output_lines)


def get_mysql_columns(
    mysql_container: str,
    user: str,
    password: str,
    db: str,
    table: str,
    host: str = "",
    port: int = 0,
) -> list[str]:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
    ]
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-P", str(port)])
    cmd.extend([
        "-e",
        (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='{db}' AND table_name='{table}' ORDER BY ordinal_position;"
        ),
    ])
    result = run_command(cmd)
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def get_mysql_primary_key_map(
    mysql_container: str,
    user: str,
    password: str,
    db: str,
    host: str = "",
    port: int = 0,
) -> dict[str, list[str]]:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
    ]
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-P", str(port)])
    cmd.extend([
        "-e",
        (
            "SELECT tc.table_name, k.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage k "
            "ON tc.constraint_name = k.constraint_name "
            "AND tc.table_schema = k.table_schema "
            "AND tc.table_name = k.table_name "
            f"WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema='{db}' "
            "ORDER BY tc.table_name, k.ordinal_position;"
        ),
    ])
    result = run_command(cmd)
    if result.returncode != 0:
        return {}

    pk_map: dict[str, list[str]] = {}
    for line in (result.stdout or "").splitlines():
        row = line.strip()
        if not row:
            continue
        parts = row.split("\t")
        if len(parts) < 2:
            continue
        table_name = parts[0].strip()
        column_name = parts[1].strip()
        if not table_name or not column_name:
            continue
        pk_map.setdefault(table_name, []).append(column_name)
    return pk_map


def sql_quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_add_primary_keys_sql(pk_map: dict[str, list[str]]) -> str:
    blocks: list[str] = []
    for table, columns in pk_map.items():
        if not columns:
            continue
        # pgloader lowercases all identifiers; match that when building PG SQL
        pg_table = table.lower()
        pg_columns = [col.lower() for col in columns]
        table_literal = sql_quote_literal(pg_table)
        table_ident = pg_quote_ident(pg_table)
        columns_sql = ", ".join(pg_quote_ident(col) for col in pg_columns)
        blocks.append(
            "DO $$ "
            "BEGIN "
            "IF EXISTS ("
            "SELECT 1 FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace "
            f"WHERE n.nspname='public' AND t.relname={table_literal}"
            ") "
            "AND NOT EXISTS ("
            "SELECT 1 FROM pg_constraint c "
            "JOIN pg_class t ON c.conrelid = t.oid "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            f"WHERE n.nspname='public' AND t.relname={table_literal} AND c.contype='p'"
            ") "
            f"THEN ALTER TABLE public.{table_ident} ADD PRIMARY KEY ({columns_sql}); "
            "END IF; "
            "END $$;"
        )
    return "\n".join(blocks)


def ensure_target_primary_keys(db: str, config: dict, selected_tables: list[str] | None = None) -> tuple[int, str]:
    mysql_conn = resolve_mysql_conn(config, db)
    pk_map = get_mysql_primary_key_map(
        str(mysql_conn.get("container", "")),
        str(mysql_conn.get("user", "")),
        str(mysql_conn.get("password", "")),
        db,
        host=str(mysql_conn.get("host", "")),
        port=int(mysql_conn.get("port", 0) or 0),
    )
    selected = parse_selected_tables(selected_tables)
    if selected:
        selected_keys = {name.lower() for name in selected}
        pk_map = {table: cols for table, cols in pk_map.items() if table.lower() in selected_keys}

    if not pk_map:
        return 0, f"Primary key ensure skipped: no source primary keys found in {db}\n"

    target_cfg = config.get("target", {})
    target_uri = target_cfg.get("uri", "").replace("{{DB_NAME}}", db)
    target = parse_db_uri(target_uri)

    if target.get("scheme") not in ("postgresql", "pgsql", "postgres"):
        return 1, f"Skip ensure primary keys: unsupported target scheme {target.get('scheme')}\n"

    output_lines: list[str] = [f"Ensuring target primary keys: {target.get('database', '')}, tables={len(pk_map)}\n"]
    for table, columns in pk_map.items():
        sql = build_add_primary_keys_sql({table: columns})
        if not sql:
            continue

        code, out = run_psql_container_sql(target, target_cfg, sql)
        if code == 0:
            continue

        output_lines.append(f"Primary key ensure failed table: {table}\n")
        output_lines.append(out)
        return code, "".join(output_lines)

    output_lines.append(f"Primary key ensure success: {db}\n")
    return 0, "".join(output_lines)


def build_clear_public_data_sql(selected_tables: list[str] | None = None) -> str:
    selected = parse_selected_tables(selected_tables)
    if not selected:
        return (
            "DO $$ "
            "DECLARE r record; "
            "BEGIN "
            "FOR r IN SELECT tablename FROM pg_tables WHERE schemaname='public' LOOP "
            "EXECUTE format('TRUNCATE TABLE public.%I RESTART IDENTITY CASCADE', r.tablename); "
            "END LOOP; "
            "END $$;"
        )

    stmts = [f"TRUNCATE TABLE public.{pg_quote_ident(table)} RESTART IDENTITY CASCADE;" for table in selected]
    return "\n".join(stmts)


def clear_target_public_table_data(db: str, config: dict, selected_tables: list[str] | None = None) -> tuple[int, str]:
    target_cfg = config.get("target", {})
    target_uri = target_cfg.get("uri", "").replace("{{DB_NAME}}", db)
    target = parse_db_uri(target_uri)

    if target.get("scheme") not in ("postgresql", "pgsql", "postgres"):
        return 1, f"Skip clear public data: unsupported target scheme {target.get('scheme')}\n"

    sql = build_clear_public_data_sql(selected_tables)

    selected = parse_selected_tables(selected_tables)
    prefix = (
        f"清理目标库选中表数据: {target.get('database', '')}，tables={len(selected)}\n"
        if selected
        else f"清理目标库全表数据: {target.get('database', '')}\n"
    )
    code, out = run_psql_container_sql(target, target_cfg, sql)
    if code == 0:
        return 0, prefix + out
    return code, prefix + out


def get_mysql_split_pk(
    mysql_container: str,
    user: str,
    password: str,
    db: str,
    table: str,
    host: str = "",
    port: int = 0,
) -> str | None:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
    ]
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-P", str(port)])
    cmd.extend([
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
    ])
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


def get_mysql_databases(mysql_container: str, user: str, password: str, host: str = "", port: int = 0) -> list[str]:
    cmd = [
        "docker",
        "exec",
        mysql_container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-N",
    ]
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-P", str(port)])
    cmd.extend([
        "-e",
        (
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('information_schema','mysql','performance_schema','sys') "
            "ORDER BY schema_name;"
        ),
    ])
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
        self.selected_tables: list[str] = []
        self.table_all_items: list[str] = []
        self.info_refresh_after_id: str | None = None
        self.fallback_databases: list[str] = []
        self.full_sync_dbs: list[str] = []
        self.sync_history_path = os.path.join(self.workspace, SYNC_HISTORY_FILE)
        self.sync_history_records: list[dict] = []
        self.uri_history_path = os.path.join(self.workspace, URI_HISTORY_FILE)
        self.uri_history_records: list[str] = []
        self.current_mode = ""
        self.eta_task_total = 0
        self.eta_task_done = 0

        self.config_path = tk.StringVar(value=os.path.join(self.workspace, DEFAULT_CONFIG))
        self.load_template = tk.StringVar()
        self.source_type = tk.StringVar(value="mysql")
        self.source_uri = tk.StringVar()
        self.target_uri = tk.StringVar()
        self.target_psql_container = tk.StringVar(value="postgres16")
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
        self.table_filter_keyword = tk.StringVar(value="")

        self._build_ui()
        self._load_uri_history_records()
        self._load_sync_history_records()
        self._load_config_safe()
        self._poll_queue()

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=10)
        top.pack(fill=tk.BOTH, expand=True)

        tabs = ttk.Notebook(top)
        tabs.pack(fill=tk.BOTH, expand=True)

        home_tab = ttk.Frame(tabs)
        config_tab = ttk.Frame(tabs)
        history_tab = ttk.Frame(tabs)
        tabs.add(home_tab, text="主页")
        tabs.add(config_tab, text="配置")
        tabs.add(history_tab, text="同步历史记录")

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

        db_left = ttk.Frame(db_frame)
        db_left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.db_list = tk.Listbox(db_left, selectmode=tk.EXTENDED)
        self.db_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.db_list.bind("<<ListboxSelect>>", self._on_db_select)
        db_scroll = ttk.Scrollbar(db_left, command=self.db_list.yview)
        db_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.db_list.configure(yscrollcommand=db_scroll.set)

        db_mid = ttk.Frame(db_frame, width=56)
        db_mid.pack(side=tk.LEFT, fill=tk.Y, padx=2)
        self.full_add_btn = ttk.Button(db_mid, text=">>", width=4, command=self._add_to_full_sync)
        self.full_add_btn.pack(pady=(26, 6))
        self.full_remove_btn = ttk.Button(db_mid, text="<<", width=4, command=self._remove_from_full_sync)
        self.full_remove_btn.pack(pady=6)

        db_right = ttk.LabelFrame(db_frame, text="全同步数据库池")
        db_right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(2, 5), pady=5)
        self.full_sync_list = tk.Listbox(db_right, selectmode=tk.EXTENDED)
        self.full_sync_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        full_scroll = ttk.Scrollbar(db_right, command=self.full_sync_list.yview)
        full_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.full_sync_list.configure(yscrollcommand=full_scroll.set)

        db_buttons = ttk.Frame(left)
        db_buttons.pack(fill=tk.X, padx=5, pady=5)
        self.db_refresh_btn = ttk.Button(db_buttons, text="刷新数据库", command=self._refresh_databases)
        self.db_refresh_btn.pack(side=tk.LEFT)
        self.full_clear_btn = ttk.Button(db_buttons, text="清空全同步池", command=self._clear_full_sync_pool)
        self.full_clear_btn.pack(side=tk.LEFT, padx=5)

        table_frame = ttk.LabelFrame(left, text="源表（单库可多选）")
        table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.table_list = tk.Listbox(table_frame, selectmode=tk.EXTENDED)
        self.table_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.table_list.bind("<<ListboxSelect>>", self._on_table_select)
        table_scroll = ttk.Scrollbar(table_frame, command=self.table_list.yview)
        table_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.table_list.configure(yscrollcommand=table_scroll.set)

        table_buttons = ttk.Frame(left)
        table_buttons.pack(fill=tk.X, padx=5, pady=5)
        self.table_refresh_btn = ttk.Button(table_buttons, text="刷新表", command=self._refresh_tables)
        self.table_refresh_btn.pack(side=tk.LEFT)
        self.table_select_all_btn = ttk.Button(table_buttons, text="全选", command=self._select_all_filtered_tables)
        self.table_select_all_btn.pack(side=tk.LEFT, padx=5)
        self.table_clear_btn = ttk.Button(table_buttons, text="清空选择", command=self._clear_table_selection)
        self.table_clear_btn.pack(side=tk.LEFT)

        table_filter_row = ttk.Frame(left)
        table_filter_row.pack(fill=tk.X, padx=5, pady=(0, 5))
        ttk.Label(table_filter_row, text="表过滤").pack(side=tk.LEFT)
        self.table_filter_entry = ttk.Entry(table_filter_row, textvariable=self.table_filter_keyword)
        self.table_filter_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.table_filter_entry.bind("<KeyRelease>", self._on_table_filter_change)

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
        self.run_view_button = ttk.Button(bottom, text="同步视图", command=lambda: self._run_selected("view"))
        self.run_view_button.pack(side=tk.LEFT, padx=5)
        self.run_full_button = ttk.Button(bottom, text="全同步", command=lambda: self._run_selected("full"))
        self.run_full_button.pack(side=tk.LEFT, padx=5)
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
        self.source_uri_history_btn = ttk.Button(
            settings,
            text="历史",
            width=6,
            command=lambda: self._open_uri_history_dialog(self.source_uri),
        )
        self.source_uri_history_btn.grid(row=row, column=2, padx=(0, 5), pady=2)
        row += 1

        ttk.Label(settings, text="目标连接 URI").grid(row=row, column=0, sticky="w")
        self.target_uri_entry = ttk.Entry(settings, textvariable=self.target_uri)
        self.target_uri_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
        self.target_uri_history_btn = ttk.Button(
            settings,
            text="历史",
            width=6,
            command=lambda: self._open_uri_history_dialog(self.target_uri),
        )
        self.target_uri_history_btn.grid(row=row, column=2, padx=(0, 5), pady=2)
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
        self.datax_source_uri_history_btn = ttk.Button(
            settings,
            text="历史",
            width=6,
            command=lambda: self._open_uri_history_dialog(self.datax_source_uri),
        )
        self.datax_source_uri_history_btn.grid(row=row, column=2, padx=(0, 5), pady=2)
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

        history_frame = ttk.LabelFrame(history_tab, text="当前机器同步历史")
        history_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        history_actions = ttk.Frame(history_tab)
        history_actions.pack(fill=tk.X, padx=8, pady=(0, 8))
        self.history_clear_btn = ttk.Button(history_actions, text="清空历史", command=self._clear_sync_history)
        self.history_clear_btn.pack(side=tk.RIGHT)

        columns = ("source_db", "target_db", "sync_time", "result", "duration")
        self.history_tree = ttk.Treeview(history_frame, columns=columns, show="headings")
        self.history_tree.heading("source_db", text="源数据库")
        self.history_tree.heading("target_db", text="目标数据库")
        self.history_tree.heading("sync_time", text="同步时间")
        self.history_tree.heading("result", text="同步结果")
        self.history_tree.heading("duration", text="同步耗时")
        self.history_tree.column("source_db", width=260, anchor=tk.W)
        self.history_tree.column("target_db", width=260, anchor=tk.W)
        self.history_tree.column("sync_time", width=180, anchor=tk.CENTER)
        self.history_tree.column("result", width=120, anchor=tk.CENTER)
        self.history_tree.column("duration", width=120, anchor=tk.CENTER)
        self.history_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        history_scroll = ttk.Scrollbar(history_frame, command=self.history_tree.yview)
        history_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.history_tree.configure(yscrollcommand=history_scroll.set)

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
        self._clear_table_list()
        self.size_label.configure(text="已选数据库大小: --")

        self.load_template.set(config.get("load_template", "mysql_to_pg.load"))
        source_cfg = config.get("source", {})
        target_cfg = config.get("target", {})
        self.source_type.set(source_cfg.get("type", "mysql"))
        self.source_uri.set(source_cfg.get("uri", ""))
        self.target_uri.set(target_cfg.get("uri", ""))
        self.target_psql_container.set((target_cfg.get("psql_container") or "postgres16").strip())
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
                "uri": normalize_db_uri(self.source_uri.get().strip()),
            },
            "target": {
                "uri": normalize_db_uri(self.target_uri.get().strip()),
                "psql_container": self.target_psql_container.get().strip() or "postgres16",
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
                "source_uri": normalize_db_uri(self.datax_source_uri.get().strip()),
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

        self._remember_uri(config["source"].get("uri", ""))
        self._remember_uri(config["target"].get("uri", ""))
        self._remember_uri(config.get("datax", {}).get("source_uri", ""))

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

        if mode in ("structure", "full") and not self._confirm_structure_sync():
            return
        if mode == "view" and not self._confirm_view_sync():
            return

        if mode == "full":
            dbs = list(self.full_sync_list.get(0, tk.END))
            if not dbs:
                messagebox.showinfo("请选择", "请先将数据库加入全同步数据库池。")
                return
            selected_tables: list[str] = []
        else:
            dbs = [self.db_list.get(i) for i in self.db_list.curselection()]
            if not dbs:
                messagebox.showinfo("请选择", "请至少选择一个数据库。")
                return

            selected_tables = parse_selected_tables(self.selected_tables)
            if selected_tables and len(dbs) != 1:
                messagebox.showerror("错误", "选择单表/多表迁移时，只能选择一个数据库。")
                return
            if mode == "view":
                selected_tables = []

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
            "selected_tables": selected_tables,
            "load_template": self.load_template.get().strip(),
            "source": {
                "type": self.source_type.get().strip(),
                "uri": self.source_uri.get().strip(),
            },
            "target": {
                "uri": self.target_uri.get().strip(),
                "psql_container": self.target_psql_container.get().strip() or "postgres16",
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
                "sync_views": True,
                "clear_public_views_before_view_sync": bool(mode == "view"),
                "clear_table_data_before_data_sync": bool(mode == "full"),
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

        self._remember_uri(config["source"].get("uri", ""))
        self._remember_uri(config["target"].get("uri", ""))
        self._remember_uri(config.get("datax", {}).get("source_uri", ""))

        self.run_structure_button.configure(state=tk.DISABLED)
        self.run_data_button.configure(state=tk.DISABLED)
        self.run_view_button.configure(state=tk.DISABLED)
        self.run_full_button.configure(state=tk.DISABLED)
        self.stop_button.configure(state=tk.NORMAL)
        self.progress.configure(value=0)
        title = "结构同步" if mode == "structure" else ("数据同步" if mode == "data" else ("视图同步" if mode == "view" else "全同步"))
        self.progress_label.configure(text=f"{title} 启动中...")
        self.eta_label.configure(text="预计剩余: --")
        self.size_label.configure(text="已选数据库大小: --")
        self.log_text.delete("1.0", tk.END)
        self.stop_event.clear()
        self.overall_total_tables = 0
        self.overall_processed_tables = 0
        self.current_mode = mode
        self.eta_task_total = len(dbs) * (4 if mode == "full" else 1)
        self.eta_task_done = 0
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
            selected_tables = parse_selected_tables(config.get("selected_tables", []))
            if selected_tables and total_dbs != 1:
                self.queue.put(("failed", "选择单表/多表迁移时，只能选择一个数据库。\n"))
                return

            if mode in ("structure", "full"):
                for db in config["databases"]:
                    if config.get("source", {}).get("type", "mysql") != "mysql":
                        continue
                    mysql_conn = resolve_mysql_conn(config, db)
                    if selected_tables:
                        all_tables = get_mysql_tables(
                            str(mysql_conn.get("container", "")),
                            str(mysql_conn.get("user", "")),
                            str(mysql_conn.get("password", "")),
                            db,
                            host=str(mysql_conn.get("host", "")),
                            port=int(mysql_conn.get("port", 0) or 0),
                        )
                        selected_found, _ = filter_tables_by_selected(all_tables, selected_tables)
                        self.overall_total_tables += len(selected_found)
                    else:
                        self.overall_total_tables += get_total_tables(
                            str(mysql_conn.get("container", "")),
                            str(mysql_conn.get("user", "")),
                            str(mysql_conn.get("password", "")),
                            db,
                            host=str(mysql_conn.get("host", "")),
                            port=int(mysql_conn.get("port", 0) or 0),
                        )

            if mode == "full":
                execution_items = []
                for db in config["databases"]:
                    execution_items.append((db, "structure"))
                    execution_items.append((db, "primary_key"))
                    execution_items.append((db, "view"))
                    execution_items.append((db, "data"))
            elif mode == "structure":
                execution_items = [(db, "structure") for db in config["databases"]]
            elif mode == "view":
                execution_items = [(db, "view") for db in config["databases"]]
            else:
                execution_items = [(db, "data") for db in config["databases"]]

            for db, phase in execution_items:
                idx = config["databases"].index(db) + 1
                if self.stop_event.is_set():
                    self.queue.put(("stopped",))
                    return

                if mode == "full":
                    if phase == "structure":
                        phase_name = "结构同步"
                    elif phase == "primary_key":
                        phase_name = "主键同步"
                    elif phase == "view":
                        phase_name = "视图同步"
                    else:
                        phase_name = "数据同步"
                    self.queue.put(("log", f"\n>>>> 全同步阶段：{db} - {phase_name} <<<<\n"))

                db_start_time = time.time()

                def push_db_history(result: str) -> None:
                    target_db = ""
                    target_uri = config.get("target", {}).get("uri", "")
                    if isinstance(target_uri, str) and target_uri.strip():
                        try:
                            parsed = parse_db_uri(target_uri.replace("{{DB_NAME}}", db))
                            target_db = str(parsed.get("database", "") or "")
                        except Exception:
                            target_db = ""
                    duration_seconds = max(0.0, time.time() - db_start_time)
                    sync_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(db_start_time))
                    self.queue.put((
                        "history",
                        {
                            "source_db": db,
                            "target_db": target_db,
                            "sync_time": sync_time,
                            "result": result,
                            "duration_seconds": duration_seconds,
                        },
                    ))

                if phase == "structure":
                    title = "结构同步"
                elif phase == "primary_key":
                    title = "主键同步"
                elif phase == "view":
                    title = "视图同步"
                else:
                    title = "数据同步"
                self.queue.put(("log", f"===============================\n开始{title}数据库: {db}\n===============================\n"))

                if phase == "structure":
                    if bool(config.get("pgloader", {}).get("clear_public_before_sync", True)):
                        if selected_tables:
                            self.queue.put(("log", f"清理目标库选中表: {db}，tables={len(selected_tables)}\n"))
                        else:
                            self.queue.put(("log", f"清理目标库 public 表: {db}\n"))
                        clear_code, clear_output = clear_target_public_tables(db, config, selected_tables=selected_tables)
                        if clear_output:
                            self.queue.put(("log", clear_output + ("" if clear_output.endswith("\n") else "\n")))
                        if clear_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"清理目标库 public 表失败: {db}\n"))
                            return

                    mysql_conn = resolve_mysql_conn(config, db)
                    source_type = config.get("source", {}).get("type", "mysql")
                    total_tables = 0
                    table_filter_clause = ""
                    if source_type == "mysql":
                        if selected_tables:
                            all_tables = get_mysql_tables(
                                str(mysql_conn.get("container", "")),
                                str(mysql_conn.get("user", "")),
                                str(mysql_conn.get("password", "")),
                                db,
                                host=str(mysql_conn.get("host", "")),
                                port=int(mysql_conn.get("port", 0) or 0),
                            )
                            selected_found, missing_tables = filter_tables_by_selected(all_tables, selected_tables)
                            if missing_tables:
                                self.queue.put(("log", f"选中表在 {db} 中不存在: {', '.join(missing_tables)}\n"))
                            if not selected_found:
                                push_db_history("失败")
                                self.queue.put(("failed", f"未找到可迁移表: {db}\n"))
                                return
                            total_tables = len(selected_found)
                            table_filter_clause = build_pgloader_table_filter_clause(selected_found)
                        else:
                            total_tables = get_total_tables(
                                str(mysql_conn.get("container", "")),
                                str(mysql_conn.get("user", "")),
                                str(mysql_conn.get("password", "")),
                                db,
                                host=str(mysql_conn.get("host", "")),
                                port=int(mysql_conn.get("port", 0) or 0),
                            )

                    source_uri = normalize_db_uri(config.get("source", {}).get("uri", "").replace("{{DB_NAME}}", db))
                    if source_type == "mysql":
                        source_uri = build_mysql_uri_from_conn(mysql_conn, db)
                    target_uri = normalize_db_uri(config.get("target", {}).get("uri", "").replace("{{DB_NAME}}", db))

                    self.queue.put(("log", f"结构同步源URI: {mask_uri_password(source_uri)}\n"))
                    self.queue.put(("log", f"结构同步目标URI: {mask_uri_password(target_uri)}\n"))

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
                            "TABLE_FILTER": table_filter_clause,
                        },
                    )
                    if mode == "full":
                        patch_rendered_load_for_full_sync(rendered_path)
                        self.queue.put(("log", "全同步结构阶段：已禁用外键创建，按 结构->主键->视图->数据 顺序执行。\n"))

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
                                push_db_history("已停止")
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
                        push_db_history("失败")
                        self.queue.put(("error", db, list(tail)))
                        return

                    ensure_pk_enabled = bool(config.get("pgloader", {}).get("ensure_primary_keys", True))
                    if ensure_pk_enabled and mode != "full":
                        pk_code, pk_output = ensure_target_primary_keys(db, config, selected_tables=selected_tables)
                        if pk_output:
                            self.queue.put(("log", pk_output + ("" if pk_output.endswith("\n") else "\n")))
                        if pk_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"补主键失败: {db}\n"))
                            return

                    sync_views_enabled = bool(config.get("pgloader", {}).get("sync_views", True))
                    if sync_views_enabled and mode != "full":
                        view_code, view_output = sync_views_for_db(db, config)
                        if view_output:
                            self.queue.put(("log", view_output + ("" if view_output.endswith("\n") else "\n")))
                        if view_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"同步视图失败: {db}\n"))
                            return

                    push_db_history("成功")
                    self.queue.put(("log", f"结构同步成功: {db}\n"))
                    self.queue.put(("phase_done",))
                elif phase == "primary_key":
                    ensure_pk_enabled = bool(config.get("pgloader", {}).get("ensure_primary_keys", True))
                    if ensure_pk_enabled:
                        pk_code, pk_output = ensure_target_primary_keys(db, config, selected_tables=selected_tables)
                        if pk_output:
                            self.queue.put(("log", pk_output + ("" if pk_output.endswith("\n") else "\n")))
                        if pk_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"补主键失败: {db}\n"))
                            return
                    push_db_history("成功")
                    self.queue.put(("log", f"主键同步成功: {db}\n"))
                    self.queue.put(("phase_done",))
                elif phase == "view":
                    if bool(config.get("pgloader", {}).get("clear_public_views_before_view_sync", False)):
                        clear_view_code, clear_view_output = clear_target_public_views(db, config)
                        if clear_view_output:
                            self.queue.put(("log", clear_view_output + ("" if clear_view_output.endswith("\n") else "\n")))
                        if clear_view_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"清理目标库视图失败: {db}\n"))
                            return

                    sync_views_enabled = bool(config.get("pgloader", {}).get("sync_views", True))
                    if sync_views_enabled:
                        view_code, view_output = sync_views_for_db(db, config)
                        if view_output:
                            self.queue.put(("log", view_output + ("" if view_output.endswith("\n") else "\n")))
                        if view_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"同步视图失败: {db}\n"))
                            return
                    push_db_history("成功")
                    self.queue.put(("log", f"视图同步成功: {db}\n"))
                    self.queue.put(("phase_done",))
                else:
                    if bool(config.get("pgloader", {}).get("clear_table_data_before_data_sync", False)):
                        clear_data_code, clear_data_output = clear_target_public_table_data(db, config, selected_tables=None)
                        if clear_data_output:
                            self.queue.put(("log", clear_data_output + ("" if clear_data_output.endswith("\n") else "\n")))
                        if clear_data_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"清理目标库表数据失败: {db}\n"))
                            return

                    datax_cfg = config.get("datax", {})
                    if not bool(datax_cfg.get("enabled", False)):
                        push_db_history("失败")
                        self.queue.put(("failed", "DataX 未启用，请先勾选 DataX 启用。\n"))
                        return

                    datax_home = resolve_datax_home(self.workspace, datax_cfg)
                    datax_py = os.path.join(datax_home, "bin", "datax.py")
                    if not datax_home or not os.path.isfile(datax_py):
                        push_db_history("失败")
                        self.queue.put((
                            "failed",
                            f"DataX 配置无效，未找到: {datax_py}\n"
                            "请检查 datax.home 配置和当前工作目录。\n",
                        ))
                        return

                    datax_cmd_cfg = dict(datax_cfg)
                    datax_cmd_cfg["home"] = datax_home

                    mysql_conn = resolve_mysql_conn(config, db)
                    source_uri_template = datax_cfg.get("source_uri") or config.get("source", {}).get("uri", "")
                    source_uri = normalize_db_uri(source_uri_template.replace("{{DB_NAME}}", db))
                    target_uri = normalize_db_uri(config.get("target", {}).get("uri", "").replace("{{DB_NAME}}", db))

                    tables = get_mysql_tables(
                        str(mysql_conn.get("container", "")),
                        str(mysql_conn.get("user", "")),
                        str(mysql_conn.get("password", "")),
                        db,
                        host=str(mysql_conn.get("host", "")),
                        port=int(mysql_conn.get("port", 0) or 0),
                    )
                    if selected_tables:
                        tables, missing_tables = filter_tables_by_selected(tables, selected_tables)
                        if missing_tables:
                            self.queue.put(("log", f"选中表在 {db} 中不存在: {', '.join(missing_tables)}\n"))
                    else:
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
                                str(mysql_conn.get("container", "")),
                                str(mysql_conn.get("user", "")),
                                str(mysql_conn.get("password", "")),
                                db,
                                table,
                                host=str(mysql_conn.get("host", "")),
                                port=int(mysql_conn.get("port", 0) or 0),
                            )
                            if not columns:
                                self.queue.put(("log", f"DataX skipped table: {db}.{table} (no columns)\n"))
                                return 0, table, "", ""

                            split_pk = get_mysql_split_pk(
                                str(mysql_conn.get("container", "")),
                                str(mysql_conn.get("user", "")),
                                str(mysql_conn.get("password", "")),
                                db,
                                table,
                                host=str(mysql_conn.get("host", "")),
                                port=int(mysql_conn.get("port", 0) or 0),
                            )
                            channel = int(datax_cfg.get("channel", 2))
                            batch_size = int(datax_cfg.get("batch_size", 2000))
                            split_pk_disp = split_pk if split_pk else "none"
                            job_file = build_datax_job(self.workspace, db, table, columns, source_uri, target_uri, datax_cfg, split_pk=split_pk)
                            cmd_datax = build_datax_command(job_file, datax_cmd_cfg)
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
                                try:
                                    dcode, failed_table, detail, _ = future.result()
                                except CancelledError:
                                    # Other table tasks are cancelled after first failure; ignore these.
                                    continue
                                if dcode == 130:
                                    if first_error is not None:
                                        continue
                                    for pending in futures:
                                        pending.cancel()
                                    push_db_history("已停止")
                                    self.queue.put(("stopped",))
                                    return
                                if dcode != 0 and first_error is None:
                                    first_error = (dcode, failed_table, detail, "")
                                    self.stop_event.set()
                                    for pending in futures:
                                        pending.cancel()

                        if first_error is not None:
                            _, failed_table, detail, _ = first_error
                            if cleanup_on_finish:
                                cleanup_empty_datax_job_dir(self.workspace, datax_cfg)
                            tip = ""
                            if is_datax_jvm_oom(detail):
                                tip = (
                                    "Tip: DataX JVM 内存不足，请降低 DataX 表并行/通道/批大小，"
                                    "并将 JVM 调小（如 -Xms256m -Xmx1024m）。\n"
                                )
                            push_db_history("失败")
                            self.queue.put(("failed", f"\nDataX failed table: {db}.{failed_table}\n--- DataX output (last 200 lines) ---\n{detail}--- end ---\n{tip}"))
                            return

                        if cleanup_on_finish:
                            cleanup_empty_datax_job_dir(self.workspace, datax_cfg)
                        self.queue.put(("log", f"DataX success: {db}\n"))

                    if not self.stop_event.is_set():
                        push_db_history("成功")
                        self.queue.put(("phase_done",))

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
            db_keys = {db.lower() for db in dbs}
            self.full_sync_dbs = [db for db in self.full_sync_dbs if db.lower() in db_keys]
            self._refresh_full_sync_list_widget()
            self.selected_dbs = []
            for idx, db in enumerate(dbs):
                if db in keep_selected:
                    self.db_list.selection_set(idx)
                    self.selected_dbs.append(db)
            if not self.selected_dbs:
                self.size_label.configure(text="已选数据库大小: --")
                self._clear_table_list()
            elif len(self.selected_dbs) == 1:
                self._refresh_tables()
        elif kind == "table_list":
            db, tables = msg[1], msg[2]
            keep_selected = set(self.selected_tables)
            self.table_all_items = list(tables)
            self.selected_tables = [table for table in self.table_all_items if table in keep_selected]
            self._refresh_table_list_widget()
            self.queue.put(("log", f"已刷新表: {db}，共 {len(tables)} 张\n"))
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
            if self.current_mode == "full":
                self._update_eta(self.eta_task_done, self.eta_task_total)
            else:
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
        elif kind == "history":
            self._append_sync_history_record(msg[1])
        elif kind == "phase_done":
            self.eta_task_done = min(self.eta_task_total, self.eta_task_done + 1)
            self._update_eta(self.eta_task_done, self.eta_task_total)

    def _set_idle(self) -> None:
        self.progress.stop()
        self.progress.configure(value=0)
        self.progress_label.configure(text="空闲")
        self.eta_label.configure(text="预计剩余: --")
        self.size_label.configure(text="已选数据库大小: --")
        self.run_structure_button.configure(state=tk.NORMAL)
        self.run_data_button.configure(state=tk.NORMAL)
        self.run_view_button.configure(state=tk.NORMAL)
        self.run_full_button.configure(state=tk.NORMAL)
        self.stop_button.configure(state=tk.DISABLED)
        self.current_process = None
        self.active_processes.clear()
        self.stop_event.clear()
        self.current_mode = ""
        self.eta_task_total = 0
        self.eta_task_done = 0
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

        selected_tables = parse_selected_tables(self.selected_tables)
        if selected_tables and len(self.selected_dbs) == 1:
            tip = (
                f"该操作会先清空目标数据库 public 下所选表（{len(selected_tables)} 张）。\n"
                "请先完成备份，再继续。"
            )
        else:
            tip = "该操作会先清空目标数据库 public 下所有表。\n请先完成备份，再继续。"

        ttk.Label(frame, text=tip, justify=tk.LEFT).pack(anchor="w", pady=(0, 10))

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

    def _confirm_view_sync(self) -> bool:
        dlg = tk.Toplevel(self)
        dlg.title("视图同步确认")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(False, False)

        frame = ttk.Frame(dlg, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)

        tip = "该操作会先清空目标数据库 public 下全部视图，然后再执行视图同步。\n请先完成备份，再继续。"
        ttk.Label(frame, text=tip, justify=tk.LEFT).pack(anchor="w", pady=(0, 10))

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
            self.eta_label.configure(text="预计剩余: --")
            return

        now = time.time()
        if done <= 0:
            self.eta_label.configure(text="预计剩余: --")
            return

        elapsed = now - self.start_time
        if elapsed <= 0:
            self.eta_label.configure(text="预计剩余: --")
            return

        rate = done / elapsed
        if rate <= 0:
            self.eta_label.configure(text="预计剩余: --")
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
        self.run_view_button.configure(state=tk.DISABLED if running else tk.NORMAL)
        self.run_full_button.configure(state=tk.DISABLED if running else tk.NORMAL)
        self.stop_button.configure(state=tk.NORMAL if running else tk.DISABLED)

        self.db_list.configure(state=state)
        self.full_sync_list.configure(state=state)
        self.db_refresh_btn.configure(state=state)
        self.full_add_btn.configure(state=state)
        self.full_remove_btn.configure(state=state)
        self.full_clear_btn.configure(state=state)
        self.table_list.configure(state=state)
        self.table_refresh_btn.configure(state=state)
        self.table_select_all_btn.configure(state=state)
        self.table_clear_btn.configure(state=state)
        self.table_filter_entry.configure(state=state)

        self.config_entry.configure(state=state)
        self.config_browse_btn.configure(state=state)
        self.config_load_btn.configure(state=state)
        self.config_save_btn.configure(state=state)

        self.load_template_entry.configure(state=state)
        self.source_type_combo.configure(state=state)
        self.source_uri_entry.configure(state=state)
        self.source_uri_history_btn.configure(state=state)
        self.target_uri_entry.configure(state=state)
        self.target_uri_history_btn.configure(state=state)
        self.mysql_container_entry.configure(state=state)
        self.mysql_user_entry.configure(state=state)
        self.mysql_password_entry.configure(state=state)
        self.pgloader_image_entry.configure(state=state)
        self.datax_enabled_check.configure(state=state)
        self.datax_home_entry.configure(state=state)
        self.datax_python_entry.configure(state=state)
        self.datax_source_uri_entry.configure(state=state)
        self.datax_source_uri_history_btn.configure(state=state)
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
            self.selected_tables = []
            self.size_label.configure(text="已选数据库大小: --")
            self._set_info_text(self.target_info_text, "未选择数据库")
            self._clear_table_list()
            return
        self.selected_dbs = dbs
        if len(dbs) == 1:
            self.selected_tables = []
            self.table_all_items = []
            self._refresh_tables()
        else:
            self.selected_tables = []
            self._clear_table_list()
        self._refresh_db_info()

    def _on_table_select(self, _event=None) -> None:
        visible_tables = [self.table_list.get(i) for i in range(self.table_list.size())]
        selected_visible = {self.table_list.get(i) for i in self.table_list.curselection()}
        keep = set(self.selected_tables)
        for table in visible_tables:
            keep.discard(table)
        keep.update(selected_visible)
        self.selected_tables = [table for table in self.table_all_items if table in keep]

    def _refresh_full_sync_list_widget(self) -> None:
        self.full_sync_list.delete(0, tk.END)
        for db in self.full_sync_dbs:
            self.full_sync_list.insert(tk.END, db)

    def _add_to_full_sync(self) -> None:
        selected = [self.db_list.get(i) for i in self.db_list.curselection()]
        if not selected:
            return
        exists = {db.lower() for db in self.full_sync_dbs}
        changed = False
        for db in selected:
            key = db.lower()
            if key in exists:
                continue
            self.full_sync_dbs.append(db)
            exists.add(key)
            changed = True
        if changed:
            self._refresh_full_sync_list_widget()

    def _remove_from_full_sync(self) -> None:
        selected = [self.full_sync_list.get(i) for i in self.full_sync_list.curselection()]
        if not selected:
            return
        remove_keys = {db.lower() for db in selected}
        self.full_sync_dbs = [db for db in self.full_sync_dbs if db.lower() not in remove_keys]
        self._refresh_full_sync_list_widget()

    def _clear_full_sync_pool(self) -> None:
        self.full_sync_dbs = []
        self._refresh_full_sync_list_widget()

    def _on_table_filter_change(self, _event=None) -> None:
        self._refresh_table_list_widget()

    def _refresh_table_list_widget(self) -> None:
        keyword = self.table_filter_keyword.get().strip().lower()
        if keyword:
            visible = [table for table in self.table_all_items if keyword in table.lower()]
        else:
            visible = list(self.table_all_items)

        selected_set = set(self.selected_tables)
        self.table_list.delete(0, tk.END)
        for idx, table in enumerate(visible):
            self.table_list.insert(tk.END, table)
            if table in selected_set:
                self.table_list.selection_set(idx)

        self.selected_tables = [table for table in self.table_all_items if table in selected_set]

    def _select_all_filtered_tables(self) -> None:
        visible = [self.table_list.get(i) for i in range(self.table_list.size())]
        keep = set(self.selected_tables)
        keep.update(visible)
        self.selected_tables = [table for table in self.table_all_items if table in keep]
        if self.table_list.size() > 0:
            self.table_list.selection_set(0, tk.END)

    def _clear_table_selection(self) -> None:
        self.selected_tables = []
        self.table_list.selection_clear(0, tk.END)

    def _clear_table_list(self) -> None:
        self.selected_tables = []
        self.table_all_items = []
        self.table_filter_keyword.set("")
        self.table_list.delete(0, tk.END)

    def _refresh_tables(self) -> None:
        if len(self.selected_dbs) != 1:
            self.selected_tables = []
            self._clear_table_list()
            return
        db = self.selected_dbs[0]
        threading.Thread(target=self._refresh_tables_async, args=(db,), daemon=True).start()

    def _refresh_tables_async(self, db: str) -> None:
        mysql_cfg = {
            "container": self.mysql_container.get().strip(),
            "user": self.mysql_user.get().strip(),
            "password": self.mysql_password.get(),
        }
        source_uri = self.source_uri.get().strip()
        conn = resolve_mysql_conn({"mysql": mysql_cfg, "source": {"uri": source_uri}}, db)
        tables = get_mysql_tables(
            str(conn.get("container", "")),
            str(conn.get("user", "")),
            str(conn.get("password", "")),
            db,
            host=str(conn.get("host", "")),
            port=int(conn.get("port", 0) or 0),
        )
        self.queue.put(("table_list", db, tables))

    def _refresh_databases(self) -> None:
        threading.Thread(target=self._refresh_databases_async, daemon=True).start()

    def _refresh_databases_async(self) -> None:
        mysql_cfg = {
            "container": self.mysql_container.get().strip(),
            "user": self.mysql_user.get().strip(),
            "password": self.mysql_password.get(),
        }
        source_uri = self.source_uri.get().strip()
        source_dbs: list[str] = []
        parsed_source = None
        if source_uri:
            try:
                parsed_source = parse_db_uri(source_uri.replace("{{DB_NAME}}", "mysql"))
            except Exception:
                parsed_source = None
        if parsed_source and str(parsed_source.get("scheme", "")).lower() not in ("", "mysql"):
            self.queue.put(("log", f"源库 URI 不是 mysql://，当前为 {parsed_source.get('scheme')}://，无法刷新 MySQL 源库列表。\n"))
        else:
            conn = resolve_mysql_conn({"mysql": mysql_cfg, "source": {"uri": source_uri}}, "mysql")
            source_dbs = get_mysql_databases(
                str(conn.get("container", "")),
                str(conn.get("user", "")),
                str(conn.get("password", "")),
                host=str(conn.get("host", "")),
                port=int(conn.get("port", 0) or 0),
            )
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
            self.queue.put((
                "target_info",
                "目标数据库查询失败或无数据\n"
                "请检查 psql 命令是否可用（本机 PostgreSQL 客户端是否安装并可执行）。\n"
                "如使用容器查询，请确认 psql_container 容器正在运行。",
            ))
            self.queue.put((
                "log",
                "目标库查询失败：psql 命令可能不可用，请检查本机 pg 客户端（psql）或容器状态。\n",
            ))
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
            mysql_cfg = {
                "container": self.mysql_container.get().strip(),
                "user": self.mysql_user.get().strip(),
                "password": self.mysql_password.get(),
            }
            source_uri = self.source_uri.get().strip()
            total_bytes = 0
            total_tables = 0
            for db in dbs:
                conn = resolve_mysql_conn({"mysql": mysql_cfg, "source": {"uri": source_uri}}, db)
                cmd = [
                    "docker",
                    "exec",
                    str(conn.get("container", "")),
                    "mysql",
                    f"-u{str(conn.get('user', ''))}",
                    f"-p{str(conn.get('password', ''))}",
                    "-N",
                ]
                host = str(conn.get("host", ""))
                port = int(conn.get("port", 0) or 0)
                if host:
                    cmd.extend(["-h", host])
                if port:
                    cmd.extend(["-P", str(port)])
                cmd.extend([
                    "-e",
                    (
                        "SELECT COUNT(*), COALESCE(SUM(data_length + index_length), 0) "
                        f"FROM information_schema.tables WHERE table_schema='{db}';"
                    ),
                ])
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

    def _load_sync_history_records(self) -> None:
        self.sync_history_records = []
        if not os.path.isfile(self.sync_history_path):
            self._refresh_history_tree()
            return
        try:
            with open(self.sync_history_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.sync_history_records = [item for item in data if isinstance(item, dict)]
        except Exception:
            self.sync_history_records = []
        self._refresh_history_tree()

    def _load_uri_history_records(self) -> None:
        self.uri_history_records = []
        if not os.path.isfile(self.uri_history_path):
            return
        try:
            with open(self.uri_history_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.uri_history_records = [str(item).strip() for item in data if str(item).strip()]
        except Exception:
            self.uri_history_records = []

    def _save_uri_history_records(self) -> None:
        try:
            with open(self.uri_history_path, "w", encoding="utf-8") as f:
                json.dump(self.uri_history_records, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _remember_uri(self, uri: str) -> None:
        value = str(uri or "").strip()
        if not value:
            return
        existed = [item for item in self.uri_history_records if item != value]
        self.uri_history_records = [value] + existed
        if len(self.uri_history_records) > 200:
            self.uri_history_records = self.uri_history_records[:200]
        self._save_uri_history_records()

    def _open_uri_history_dialog(self, target_var: tk.StringVar) -> None:
        history = [item for item in self.uri_history_records if item.strip()]
        if not history:
            messagebox.showinfo("提示", "暂无数据库链接历史。")
            return

        dlg = tk.Toplevel(self)
        dlg.title("选择数据库链接历史")
        dlg.transient(self)
        dlg.grab_set()
        dlg.geometry("980x420")
        dlg.minsize(760, 320)

        frame = ttk.Frame(dlg, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)
        ttk.Label(frame, text="双击或选中后点击“使用所选链接”即可回填。", justify=tk.LEFT).pack(anchor="w", pady=(0, 8))

        list_frame = ttk.Frame(frame)
        list_frame.pack(fill=tk.BOTH, expand=True)
        listbox = tk.Listbox(list_frame, selectmode=tk.SINGLE)
        listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar = ttk.Scrollbar(list_frame, command=listbox.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        listbox.configure(yscrollcommand=scrollbar.set)

        for item in history:
            listbox.insert(tk.END, item)

        if history:
            listbox.selection_set(0)

        result = {"chosen": ""}

        def refresh_listbox(select_index: int = 0) -> None:
            listbox.delete(0, tk.END)
            for item in history:
                listbox.insert(tk.END, item)
            if history:
                select_index = max(0, min(select_index, len(history) - 1))
                listbox.selection_set(select_index)

        def choose_selected() -> None:
            selected = listbox.curselection()
            if not selected:
                return
            value = listbox.get(selected[0])
            result["chosen"] = value
            dlg.destroy()

        def delete_selected() -> None:
            selected = listbox.curselection()
            if not selected:
                messagebox.showinfo("提示", "请先选择一条历史链接。")
                return
            idx = selected[0]
            value = listbox.get(idx)
            ok = messagebox.askyesno("确认", "确定删除所选历史链接吗？")
            if not ok:
                return
            try:
                history.remove(value)
            except ValueError:
                return
            self.uri_history_records = [item for item in self.uri_history_records if item != value]
            self._save_uri_history_records()
            if not history:
                messagebox.showinfo("提示", "历史链接已全部删除。")
                dlg.destroy()
                return
            refresh_listbox(select_index=idx)

        def on_double_click(_event=None) -> None:
            choose_selected()

        listbox.bind("<Double-Button-1>", on_double_click)

        btns = ttk.Frame(frame)
        btns.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(btns, text="取消", command=dlg.destroy).pack(side=tk.RIGHT)
        ttk.Button(btns, text="删除所选", command=delete_selected).pack(side=tk.RIGHT, padx=6)
        ttk.Button(btns, text="使用所选链接", command=choose_selected).pack(side=tk.RIGHT, padx=6)

        dlg.wait_window()
        if result["chosen"]:
            target_var.set(result["chosen"])
            self._remember_uri(result["chosen"])

    def _save_sync_history_records(self) -> None:
        try:
            with open(self.sync_history_path, "w", encoding="utf-8") as f:
                json.dump(self.sync_history_records, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _format_duration_text(self, duration_seconds: float) -> str:
        if duration_seconds < 60:
            return f"{duration_seconds:.1f}s"
        mins, secs = divmod(int(duration_seconds), 60)
        hours, mins = divmod(mins, 60)
        if hours > 0:
            return f"{hours}h {mins}m {secs}s"
        return f"{mins}m {secs}s"

    def _append_sync_history_record(self, record: dict) -> None:
        if not isinstance(record, dict):
            return
        source_db = str(record.get("source_db", "") or "")
        target_db = str(record.get("target_db", "") or "")
        sync_time = str(record.get("sync_time", "") or "")
        result = str(record.get("result", "") or "")
        duration_seconds_raw = record.get("duration_seconds", 0)
        try:
            duration_seconds = float(duration_seconds_raw)
        except (TypeError, ValueError):
            duration_seconds = 0.0

        new_item = {
            "source_db": source_db,
            "target_db": target_db,
            "sync_time": sync_time,
            "result": result,
            "duration_seconds": duration_seconds,
        }
        self.sync_history_records.insert(0, new_item)
        if len(self.sync_history_records) > 1000:
            self.sync_history_records = self.sync_history_records[:1000]
        self._save_sync_history_records()
        self._refresh_history_tree()

    def _refresh_history_tree(self) -> None:
        if not hasattr(self, "history_tree"):
            return
        for item_id in self.history_tree.get_children():
            self.history_tree.delete(item_id)

        for item in self.sync_history_records:
            duration_text = self._format_duration_text(float(item.get("duration_seconds", 0) or 0))
            self.history_tree.insert(
                "",
                tk.END,
                values=(
                    str(item.get("source_db", "") or ""),
                    str(item.get("target_db", "") or ""),
                    str(item.get("sync_time", "") or ""),
                    str(item.get("result", "") or ""),
                    duration_text,
                ),
            )

    def _clear_sync_history(self) -> None:
        if not self.sync_history_records and not os.path.isfile(self.sync_history_path):
            messagebox.showinfo("提示", "暂无历史记录可清空。")
            return

        ok = messagebox.askyesno("确认", "确定要清空当前机器的同步历史记录吗？")
        if not ok:
            return

        self.sync_history_records = []
        try:
            if os.path.isfile(self.sync_history_path):
                os.remove(self.sync_history_path)
        except OSError:
            pass

        self._refresh_history_tree()
        self.queue.put(("log", "已清空同步历史记录。\n"))

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
