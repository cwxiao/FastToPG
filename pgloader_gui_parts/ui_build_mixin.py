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

class UiBuildMixin:

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
