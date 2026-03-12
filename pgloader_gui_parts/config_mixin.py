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


class ConfigMixin:
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

