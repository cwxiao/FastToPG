import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from collections import deque
from typing import Dict, List, Optional
from urllib.parse import quote, unquote, urlparse

from version import APP_NAME, APP_VERSION


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_command(cmd: List[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def write_sql_file(output_path: str, sql: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(sql)


def redact_uri(uri: str) -> str:
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


def apply_target_user_uri(target_uri: str, target_user: Dict) -> str:
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
    mode: str,
    binary: str,
) -> List[str]:
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


def print_progress(db: str, processed: int, total: int, table: str = "") -> None:
    if total > 0:
        percent = min(100, int((processed * 100) / total))
        bar_len = 30
        filled = int(bar_len * percent / 100)
        bar = "#" * filled + "-" * (bar_len - filled)
        table_text = f" | {table}" if table else ""
        msg = f"{db} [{bar}] {percent}% ({processed}/{total} tables){table_text}"
    else:
        table_text = f" | {table}" if table else ""
        msg = f"{db} {processed} tables processed{table_text}"
    sys.stdout.write("\r" + msg + " " * 10)
    sys.stdout.flush()


def run_pgloader_for_db(
    db: str,
    config: Dict,
    workspace: str,
    log_path: str,
) -> int:
    mysql_cfg = config.get("mysql", {})
    pgloader_cfg = config["pgloader"]
    source_cfg = config.get("source", {})
    target_cfg = config.get("target", {})
    source_type = source_cfg.get("type", "mysql")

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
    target_user = config.get("target_user", {})
    use_target_user = bool(target_user.get("auto_create", False))
    if use_target_user:
        target_uri = apply_target_user_uri(target_uri, target_user)

    test_ok, test_msg = test_pg_connection(config, workspace, target_uri, log_path)
    if not test_ok and use_target_user:
        ensure_ok, ensure_msg = ensure_pg_user(config, workspace, db, log_path)
        if not ensure_ok:
            print(f"Target connection test failed: {test_msg}")
            print(f"Failed to ensure pg user: {ensure_msg}")
            return 1
        if ensure_msg and ensure_msg != "skipped":
            print(ensure_msg)
        test_ok, test_msg = test_pg_connection(config, workspace, target_uri, log_path)
    if not test_ok:
        print(f"Target connection test failed: {test_msg}")
        return 1
    template_path = os.path.join(workspace, config["load_template"])
    rendered_name = f".pgloader_rendered_{db}.load"
    rendered_path = os.path.join(workspace, rendered_name)
    render_load_file(
        template_path,
        rendered_path,
        {
            "DB_NAME": db,
            "SOURCE_URI": source_uri,
            "TARGET_URI": target_uri,
        },
    )

    pgloader_mode = pgloader_cfg.get("mode", "docker")
    pgloader_binary = pgloader_cfg.get("binary", "pgloader")
    load_file = rendered_name if pgloader_mode == "docker" else rendered_path

    cmd = build_pgloader_command(
        workspace=workspace,
        load_file=load_file,
        image=pgloader_cfg["image"],
        env=pgloader_cfg.get("env", {}),
        mode=pgloader_mode,
        binary=pgloader_binary,
    )
    if test_ok:
        ensure_ok, ensure_msg = ensure_pg_user(config, workspace, db, log_path)
        if not ensure_ok:
            warning = f"Warning: failed to ensure pg user, continue with existing target user: {ensure_msg}"
            print(warning)
            with open(log_path, "a", encoding="utf-8") as log_fp:
                log_fp.write(warning + "\n")
        elif ensure_msg and ensure_msg != "skipped":
            print(ensure_msg)

    show_output = bool(pgloader_cfg.get("show_output", False))
    processed = 0
    tail = deque(maxlen=200)
    table_line = re.compile(rf"^\s*{re.escape(db)}\.(\S+)\s+\d+\s+\d+")
    current_table = ""
    had_error = False

    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    log_fp = open(log_path, "a", encoding="utf-8")
    log_fp.write(f"{APP_NAME} v{APP_VERSION} - {datetime.now().isoformat()}\n")
    log_fp.write(f"Database: {db}\n")
    log_fp.write(f"Mode: {pgloader_mode}\n")
    if use_target_user:
        log_fp.write(f"Target URI: {redact_uri(target_uri)} (auto user)\n")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=workspace,
    )

    assert process.stdout is not None
    for line in process.stdout:
        tail.append(line)
        log_fp.write(line)
        if "ERROR" in line or "FATAL" in line:
            had_error = True
        if show_output:
            sys.stdout.write(line)
        match = table_line.match(line)
        if match:
            processed += 1
            current_table = match.group(1)
            print_progress(db, processed, total_tables, current_table)

    code = process.wait()
    log_fp.flush()
    log_fp.close()
    if not show_output:
        sys.stdout.write("\r")
        sys.stdout.flush()
    print_progress(db, processed, total_tables, current_table)
    sys.stdout.write("\n")
    sys.stdout.flush()

    if code != 0 or had_error:
        summary = extract_error_lines(list(tail))
        if summary:
            sys.stdout.write("\n--- error summary ---\n")
            sys.stdout.write("\n".join(summary) + "\n")
            sys.stdout.write("--- end ---\n")
        sys.stdout.write("\n--- pgloader output (last 200 lines) ---\n")
        sys.stdout.writelines(tail)
        sys.stdout.write("--- end ---\n")
        return code if code != 0 else 1

    return code


def prepare_log_path(workspace: str, db: str) -> str:
    logs_dir = os.path.join(workspace, "logs")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(logs_dir, f"run_{timestamp}_{db}.log")


def validate_config(config: Dict, workspace: str) -> List[str]:
    errors: List[str] = []
    load_template = str(config.get("load_template", "")).strip()
    if not load_template:
        errors.append("load_template is required")
    template_path = os.path.join(workspace, load_template)
    if load_template and not os.path.exists(template_path):
        errors.append("load_template file not found")

    source_cfg = config.get("source", {})
    target_cfg = config.get("target", {})
    source_uri = str(source_cfg.get("uri", "")).strip()
    target_uri = str(target_cfg.get("uri", "")).strip()
    if not source_uri:
        errors.append("source.uri is required")
    if not target_uri:
        errors.append("target.uri is required")

    if load_template and os.path.exists(template_path):
        try:
            with open(template_path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError:
            content = ""
        if "{{SOURCE_URI}}" not in content or "{{TARGET_URI}}" not in content:
            errors.append("load_template must contain {{SOURCE_URI}} and {{TARGET_URI}}")

    if source_cfg.get("type") == "mysql":
        mysql_cfg = config.get("mysql", {})
        if not mysql_cfg.get("container"):
            errors.append("mysql.container is required")
        if not mysql_cfg.get("user"):
            errors.append("mysql.user is required")

    pgloader_cfg = config.get("pgloader", {})
    mode = pgloader_cfg.get("mode", "docker")
    if mode == "docker":
        if not pgloader_cfg.get("image"):
            errors.append("pgloader.image is required when mode is docker")
    else:
        binary = pgloader_cfg.get("binary", "")
        if not binary:
            errors.append("pgloader.binary is required when mode is local")
        elif not shutil.which(binary):
            errors.append("pgloader binary not found in PATH")

    target_user = config.get("target_user", {})
    if target_user.get("auto_create", False):
        if not target_user.get("name"):
            errors.append("target_user.name is required when auto_create is true")
        if not target_user.get("password"):
            errors.append("target_user.password is required when auto_create is true")
        pg_admin_cfg = config.get("pg_admin", {})
        if not pg_admin_cfg.get("image"):
            errors.append("pg_admin.image is required when auto_create is true")
        target_cfg = config.get("target", {})
        if not target_cfg.get("admin_uri"):
            errors.append("target.admin_uri is required when auto_create is true")

    return errors


def extract_error_lines(lines: List[str]) -> List[str]:
    error_lines = []
    for line in lines:
        if "ERROR" in line or "FATAL" in line:
            error_lines.append(line.strip())
    return error_lines[:20]


def ensure_pg_user(config: Dict, workspace: str, db: str, log_path: str) -> tuple[bool, str]:
    target_user = config.get("target_user", {})
    if not target_user.get("auto_create", False):
        return True, "skipped"

    username = str(target_user.get("name", "")).strip()
    password = str(target_user.get("password", "")).strip()
    if not username or not password:
        return False, "target_user name/password required"
    if any(ch in password for ch in ["@", ":", "/", "?"]):
        with open(log_path, "a", encoding="utf-8") as log_fp:
            log_fp.write("Warning: target_user password has special chars; consider alnum only.\n")

    target_cfg = config.get("target", {})
    admin_uri = str(target_cfg.get("admin_uri") or target_cfg.get("uri", "")).strip()
    if not admin_uri:
        return False, "target uri required"
    admin_uri = admin_uri.replace("{{DB_NAME}}", db)
    admin_uri = normalize_uri_credentials(admin_uri)

    admin_cfg = config.get("pg_admin", {})
    image = admin_cfg.get("image", "postgres:14")

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
    sql_path = os.path.join(workspace, sql_name)
    write_sql_file(sql_path, sql)

    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as log_fp:
        log_fp.write(f"Ensuring PG user: {username}\n")
        log_fp.write(f"Admin URI: {redact_uri(admin_uri)}\n")
        log_fp.write(f"Admin image: {image}\n")

    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{workspace}:/work",
        image,
        "psql",
        admin_uri,
        "-v",
        "ON_ERROR_STOP=1",
        "-f",
        f"/work/{sql_name}",
    ]

    result = run_command(cmd, cwd=workspace)
    if result.returncode != 0:
        with open(log_path, "a", encoding="utf-8") as log_fp:
            if result.stdout:
                log_fp.write(result.stdout)
            if result.stderr:
                log_fp.write(result.stderr)
        return False, (result.stdout or result.stderr or "psql failed").strip()
    with open(log_path, "a", encoding="utf-8") as log_fp:
        log_fp.write("PG user ensured and privileges granted.\n")
    return True, "PG user ensured and ready."


def test_pg_connection(config: Dict, workspace: str, target_uri: str, log_path: str) -> tuple[bool, str]:
    pg_admin_cfg = config.get("pg_admin", {})
    image = pg_admin_cfg.get("image", "postgres:14")
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

    with open(log_path, "a", encoding="utf-8") as log_fp:
        log_fp.write(f"Testing target connection: {redact_uri(target_uri)}\n")
        log_fp.write(f"Test image: {image}\n")

    result = run_command(cmd, cwd=workspace)
    if result.returncode != 0:
        with open(log_path, "a", encoding="utf-8") as log_fp:
            if result.stdout:
                log_fp.write(result.stdout)
            if result.stderr:
                log_fp.write(result.stderr)
        return False, (result.stdout or result.stderr or "psql failed").strip()
    with open(log_path, "a", encoding="utf-8") as log_fp:
        log_fp.write("Target connection OK.\n")
    return True, "ok"


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
    args = parser.parse_args()

    workspace = os.path.abspath(os.path.dirname(__file__))
    config = load_config(os.path.join(workspace, args.config))

    errors = validate_config(config, workspace)
    if errors:
        print("Configuration errors:")
        for err in errors:
            print(f"- {err}")
        return 1

    databases = args.db if args.db else config.get("databases", [])
    if not databases:
        print("No databases configured.")
        return 1

    for db in databases:
        print("=" * 30)
        print(f"Migrating database: {db}")
        print("=" * 30)
        log_path = prepare_log_path(workspace, db)
        code = run_pgloader_for_db(db, config, workspace, log_path)
        if code != 0:
            print(f"Migration failed: {db}")
            print(f"Log file: {log_path}")
            return code
        print(f"Migration success: {db}")
        print(f"Log file: {log_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
