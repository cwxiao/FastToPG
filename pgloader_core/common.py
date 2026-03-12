import json
import os
import re
import subprocess
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

