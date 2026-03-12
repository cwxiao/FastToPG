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

class UiMixin:

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
