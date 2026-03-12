import re

from .common import (
    parse_db_uri,
    parse_selected_tables,
    pg_quote_ident,
    resolve_mysql_conn,
    run_command,
)


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
        "--default-character-set=utf8mb4",
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
        "--default-character-set=utf8mb4",
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
        "--default-character-set=utf8mb4",
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
    host_raw = str(target.get("host", "") or "")
    port_raw = str(target.get("port", 5432) or 5432)
    user_raw = str(target.get("user", "") or "")
    password_raw = str(target.get("password", "") or "")
    db_raw = str(target.get("database", "") or "")

    host_candidates: list[str] = []
    for host in [host_raw, "host.docker.internal"]:
        value = host.strip()
        if value and value not in host_candidates:
            host_candidates.append(value)

    db_candidates: list[str] = []
    for db in [db_raw, "postgres"]:
        value = db.strip()
        if value and value not in db_candidates:
            db_candidates.append(value)

    sql = "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;"

    for host in host_candidates:
        for db in db_candidates:
            cmd = [
                "docker",
                "exec",
                "-e",
                f"PGPASSWORD={password_raw}",
                psql_container,
                "psql",
                "-h",
                host,
                "-p",
                port_raw,
                "-U",
                user_raw,
                "-d",
                db,
                "-t",
                "-A",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                sql,
            ]
            result = run_command(cmd)
            if result.returncode != 0:
                continue
            return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    return []

