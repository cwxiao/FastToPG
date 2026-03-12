import json
import os
import queue
import re
import subprocess
import threading
import time
import tkinter as tk
from collections import deque
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from tkinter import filedialog, messagebox, ttk

from pgloader_gui_core import (
    DEFAULT_CONFIG,
    SOURCE_TYPES,
    SYNC_HISTORY_FILE,
    URI_HISTORY_FILE,
    build_datax_command,
    build_datax_job,
    build_mysql_uri_from_conn,
    build_pgloader_command,
    build_pgloader_table_filter_clause,
    cleanup_datax_job_file,
    cleanup_datax_jobs_for_db,
    cleanup_datax_logs_by_retention,
    cleanup_empty_datax_job_dir,
    cleanup_pgloader_rendered_file,
    cleanup_pgloader_rendered_files_for_db,
    clear_target_public_table_data,
    clear_target_public_tables,
    clear_target_public_views,
    ensure_target_primary_keys,
    filter_tables_by_selected,
    get_mysql_columns,
    get_mysql_databases,
    get_mysql_split_pk,
    get_mysql_tables,
    get_target_databases,
    get_total_tables,
    is_cleanup_jobs_on_finish,
    is_datax_jvm_oom,
    is_datax_key_log,
    is_pgloader_error_log,
    load_config,
    mask_uri_password,
    normalize_db_uri,
    parse_db_uri,
    parse_selected_tables,
    patch_rendered_load_for_full_sync,
    render_load_file,
    resolve_datax_home,
    resolve_mysql_conn,
    run_command,
    save_config,
    should_skip_table,
    sync_views_for_db,
)


class DataMixin:
    def _add_db(self) -> None:
        db = tk.simpledialog.askstring("新增数据库", "数据库名称:")
        if db:
            self.db_list.insert(tk.END, db.strip())

    def _remove_db(self) -> None:
        for idx in reversed(self.db_list.curselection()):
            self.db_list.delete(idx)

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
        psql_container = self.target_psql_container.get().strip() or "postgres16"

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
                "目标库查询失败：请检查目标 URI 主机/端口、psql_container 容器状态，以及容器到目标库网络连通性。\n",
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

    def _format_size(self, num_bytes: int) -> str:
        if num_bytes < 1024:
            return f"{num_bytes} B"
        for unit in ["KB", "MB", "GB", "TB"]:
            num_bytes /= 1024
            if num_bytes < 1024:
                return f"{num_bytes:.2f} {unit}"
        return f"{num_bytes:.2f} PB"

