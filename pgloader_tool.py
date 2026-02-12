import argparse
import json
import os
import re
import subprocess
import sys
from collections import deque
from typing import Dict, List, Optional


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_command(cmd: List[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


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


def build_pgloader_command(
    workspace: str,
    load_template: str,
    db: str,
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
        f"sed 's/{{{{DB_NAME}}}}/{db}/g' /pgloader/{load_template} > /tmp/load.load; "
        "pgloader --on-error-stop /tmp/load.load",
    ])
    return cmd


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
    mysql_cfg = config["mysql"]
    pgloader_cfg = config["pgloader"]
    total_tables = get_total_tables(
        mysql_cfg["container"],
        mysql_cfg["user"],
        mysql_cfg["password"],
        db,
    )

    cmd = build_pgloader_command(
        workspace=workspace,
        load_template=config["load_template"],
        db=db,
        image=pgloader_cfg["image"],
        env=pgloader_cfg.get("env", {}),
    )

    show_output = bool(pgloader_cfg.get("show_output", False))
    processed = 0
    tail = deque(maxlen=200)
    table_line = re.compile(rf"^\s*{re.escape(db)}\.\S+\s+\d+\s+\d+")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=workspace,
    )

    assert process.stdout is not None
    for line in process.stdout:
        tail.append(line)
        if show_output:
            sys.stdout.write(line)
        if table_line.match(line):
            processed += 1
            print_progress(db, processed, total_tables)

    code = process.wait()
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
    args = parser.parse_args()

    workspace = os.path.abspath(os.path.dirname(__file__))
    config = load_config(os.path.join(workspace, args.config))

    databases = args.db if args.db else config.get("databases", [])
    if not databases:
        print("No databases configured.")
        return 1

    for db in databases:
        print("=" * 30)
        print(f"Migrating database: {db}")
        print("=" * 30)
        code = run_pgloader_for_db(db, config, workspace)
        if code != 0:
            print(f"Migration failed: {db}")
            return code
        print(f"Migration success: {db}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
