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

class WorkerMixin:

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
