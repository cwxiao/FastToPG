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


class HistoryMixin:
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

    def _is_postgres_uri(self, uri: str) -> bool:
        text = normalize_db_uri(str(uri or "").strip())
        if not text:
            return False
        try:
            scheme = str(parse_db_uri(text).get("scheme", "")).lower()
        except Exception:
            return False
        return scheme in ("postgresql", "postgres", "pgsql")

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
        ttk.Label(
            frame,
            text="历史链接已按 PostgreSQL 与其他类型分组。双击或选中后点击“使用所选链接”即可回填。",
            justify=tk.LEFT,
        ).pack(anchor="w", pady=(0, 8))

        notebook = ttk.Notebook(frame)
        notebook.pack(fill=tk.BOTH, expand=True)

        pg_tab = ttk.Frame(notebook)
        other_tab = ttk.Frame(notebook)
        notebook.add(pg_tab, text="PostgreSQL")
        notebook.add(other_tab, text="其他")

        def _build_list(parent: ttk.Frame) -> tuple[tk.Listbox, ttk.Scrollbar]:
            listbox = tk.Listbox(parent, selectmode=tk.SINGLE)
            listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar = ttk.Scrollbar(parent, command=listbox.yview)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            listbox.configure(yscrollcommand=scrollbar.set)
            return listbox, scrollbar

        pg_listbox, _ = _build_list(pg_tab)
        other_listbox, _ = _build_list(other_tab)

        result = {"chosen": ""}
        grouped = {"pg": [], "other": []}

        def regroup_history() -> None:
            grouped["pg"] = [item for item in history if self._is_postgres_uri(item)]
            grouped["other"] = [item for item in history if not self._is_postgres_uri(item)]

        def _active_listbox() -> tk.Listbox:
            current_tab = notebook.select()
            return pg_listbox if current_tab == str(pg_tab) else other_listbox

        def refresh_listbox(select_key: str | None = None, select_index: int = 0) -> None:
            regroup_history()

            pg_listbox.delete(0, tk.END)
            for item in grouped["pg"]:
                pg_listbox.insert(tk.END, item)

            other_listbox.delete(0, tk.END)
            for item in grouped["other"]:
                other_listbox.insert(tk.END, item)

            if select_key == "pg" and grouped["pg"]:
                idx = max(0, min(select_index, len(grouped["pg"]) - 1))
                pg_listbox.selection_set(idx)
                notebook.select(pg_tab)
                return
            if select_key == "other" and grouped["other"]:
                idx = max(0, min(select_index, len(grouped["other"]) - 1))
                other_listbox.selection_set(idx)
                notebook.select(other_tab)
                return

            prefers_pg = hasattr(self, "target_uri") and (target_var is self.target_uri)
            if prefers_pg and grouped["pg"]:
                pg_listbox.selection_set(0)
                notebook.select(pg_tab)
            elif grouped["other"]:
                other_listbox.selection_set(0)
                notebook.select(other_tab)
            elif grouped["pg"]:
                pg_listbox.selection_set(0)
                notebook.select(pg_tab)

        def choose_selected() -> None:
            listbox = _active_listbox()
            selected = listbox.curselection()
            if not selected:
                return
            value = listbox.get(selected[0])
            result["chosen"] = value
            dlg.destroy()

        def delete_selected() -> None:
            listbox = _active_listbox()
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
            select_key = "pg" if self._is_postgres_uri(value) else "other"
            refresh_listbox(select_key=select_key, select_index=idx)

        def on_double_click(_event=None) -> None:
            choose_selected()

        pg_listbox.bind("<Double-Button-1>", on_double_click)
        other_listbox.bind("<Double-Button-1>", on_double_click)
        refresh_listbox()

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

