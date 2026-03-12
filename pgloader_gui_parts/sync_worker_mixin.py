import os
import re
import subprocess
import threading
import time
from collections import deque
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from pgloader_gui_core import *
class SyncWorkerMixin:
    def _worker(self, config: dict, mode: str) -> None:
        try:
            cleanup_datax_logs_by_retention(self.workspace, config.get("datax", {}))
            self.overall_total_tables = 0
            self.overall_processed_tables = 0
            total_dbs = len(config["databases"])
            selected_tables = parse_selected_tables(config.get("selected_tables", []))
            if selected_tables and total_dbs != 1:
                self.queue.put(("failed", "选择单表/多表迁移时，只能选择一个数据库。\n"))
                return
            if mode in ("structure", "full"):
                for db in config["databases"]:
                    if config.get("source", {}).get("type", "mysql") != "mysql":
                        continue
                    mysql_conn = resolve_mysql_conn(config, db)
                    if selected_tables:
                        all_tables = get_mysql_tables(
                            str(mysql_conn.get("container", "")),
                            str(mysql_conn.get("user", "")),
                            str(mysql_conn.get("password", "")),
                            db,
                            host=str(mysql_conn.get("host", "")),
                            port=int(mysql_conn.get("port", 0) or 0),
                        )
                        selected_found, _ = filter_tables_by_selected(all_tables, selected_tables)
                        self.overall_total_tables += len(selected_found)
                    else:
                        self.overall_total_tables += get_total_tables(
                            str(mysql_conn.get("container", "")),
                            str(mysql_conn.get("user", "")),
                            str(mysql_conn.get("password", "")),
                            db,
                            host=str(mysql_conn.get("host", "")),
                            port=int(mysql_conn.get("port", 0) or 0),
                        )
            if mode == "full":
                execution_items = []
                for db in config["databases"]:
                    execution_items.append((db, "structure"))
                    execution_items.append((db, "primary_key"))
                    execution_items.append((db, "view"))
                    execution_items.append((db, "data"))
            elif mode == "structure":
                execution_items = [(db, "structure") for db in config["databases"]]
            elif mode == "view":
                execution_items = [(db, "view") for db in config["databases"]]
            else:
                execution_items = [(db, "data") for db in config["databases"]]
            for db, phase in execution_items:
                idx = config["databases"].index(db) + 1
                if self.stop_event.is_set():
                    self.queue.put(("stopped",))
                    return
                if mode == "full":
                    if phase == "structure":
                        phase_name = "结构同步"
                    elif phase == "primary_key":
                        phase_name = "主键同步"
                    elif phase == "view":
                        phase_name = "视图同步"
                    else:
                        phase_name = "数据同步"
                    self.queue.put(("log", f"\n>>>> 全同步阶段：{db} - {phase_name} <<<<\n"))
                db_start_time = time.time()
                def push_db_history(result: str) -> None:
                    target_db = ""
                    target_uri = config.get("target", {}).get("uri", "")
                    if isinstance(target_uri, str) and target_uri.strip():
                        try:
                            parsed = parse_db_uri(target_uri.replace("{{DB_NAME}}", db))
                            target_db = str(parsed.get("database", "") or "")
                        except Exception:
                            target_db = ""
                    duration_seconds = max(0.0, time.time() - db_start_time)
                    sync_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(db_start_time))
                    self.queue.put((
                        "history",
                        {
                            "source_db": db,
                            "target_db": target_db,
                            "sync_time": sync_time,
                            "result": result,
                            "duration_seconds": duration_seconds,
                        },
                    ))
                if phase == "structure":
                    title = "结构同步"
                elif phase == "primary_key":
                    title = "主键同步"
                elif phase == "view":
                    title = "视图同步"
                else:
                    title = "数据同步"
                self.queue.put(("log", f"===============================\n开始{title}数据库: {db}\n===============================\n"))
                if phase == "structure":
                    if bool(config.get("pgloader", {}).get("clear_public_before_sync", True)):
                        if selected_tables:
                            self.queue.put(("log", f"清理目标库选中表: {db}，tables={len(selected_tables)}\n"))
                        else:
                            self.queue.put(("log", f"清理目标库 public 表: {db}\n"))
                        clear_code, clear_output = clear_target_public_tables(db, config, selected_tables=selected_tables)
                        if clear_output:
                            self.queue.put(("log", clear_output + ("" if clear_output.endswith("\n") else "\n")))
                        if clear_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"清理目标库 public 表失败: {db}\n"))
                            return
                    mysql_conn = resolve_mysql_conn(config, db)
                    source_type = config.get("source", {}).get("type", "mysql")
                    total_tables = 0
                    table_filter_clause = ""
                    if source_type == "mysql":
                        if selected_tables:
                            all_tables = get_mysql_tables(
                                str(mysql_conn.get("container", "")),
                                str(mysql_conn.get("user", "")),
                                str(mysql_conn.get("password", "")),
                                db,
                                host=str(mysql_conn.get("host", "")),
                                port=int(mysql_conn.get("port", 0) or 0),
                            )
                            selected_found, missing_tables = filter_tables_by_selected(all_tables, selected_tables)
                            if missing_tables:
                                self.queue.put(("log", f"选中表在 {db} 中不存在: {', '.join(missing_tables)}\n"))
                            if not selected_found:
                                push_db_history("失败")
                                self.queue.put(("failed", f"未找到可迁移表: {db}\n"))
                                return
                            total_tables = len(selected_found)
                            table_filter_clause = build_pgloader_table_filter_clause(selected_found)
                        else:
                            total_tables = get_total_tables(
                                str(mysql_conn.get("container", "")),
                                str(mysql_conn.get("user", "")),
                                str(mysql_conn.get("password", "")),
                                db,
                                host=str(mysql_conn.get("host", "")),
                                port=int(mysql_conn.get("port", 0) or 0),
                            )
                    source_uri = normalize_db_uri(config.get("source", {}).get("uri", "").replace("{{DB_NAME}}", db))
                    if source_type == "mysql":
                        source_uri = build_mysql_uri_from_conn(mysql_conn, db)
                    target_uri = normalize_db_uri(config.get("target", {}).get("uri", "").replace("{{DB_NAME}}", db))
                    self.queue.put(("log", f"结构同步源URI: {mask_uri_password(source_uri)}\n"))
                    self.queue.put(("log", f"结构同步目标URI: {mask_uri_password(target_uri)}\n"))
                    template_path = os.path.join(self.workspace, config["load_template"])
                    rendered_name = f".pgloader_rendered_{db}.load"
                    rendered_path = os.path.join(self.workspace, rendered_name)
                    cleanup_pgloader_temp = bool(config.get("pgloader", {}).get("cleanup_temp_files", True))
                    if cleanup_pgloader_temp:
                        cleanup_pgloader_rendered_files_for_db(self.workspace, db)
                    render_load_file(
                        template_path,
                        rendered_path,
                        {
                            "DB_NAME": db,
                            "SOURCE_URI": source_uri,
                            "TARGET_URI": target_uri,
                            "TABLE_FILTER": table_filter_clause,
                        },
                    )
                    if mode == "full":
                        patch_rendered_load_for_full_sync(rendered_path)
                        self.queue.put(("log", "全同步结构阶段：已禁用外键创建，按 结构->主键->视图->数据 顺序执行。\n"))
                    cmd = build_pgloader_command(
                        workspace=self.workspace,
                        load_file=rendered_name,
                        image=config["pgloader"]["image"],
                        env=config["pgloader"].get("env", {}),
                    )
                    show_output = bool(config["pgloader"].get("show_output", False))
                    processed = 0
                    tail = deque(maxlen=200)
                    table_line = re.compile(rf"^\s*{re.escape(db)}\.\S+\s+\d+\s+\d+")
                    has_log_error = False
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        cwd=self.workspace,
                    )
                    self.current_process = process
                    try:
                        assert process.stdout is not None
                        for line in process.stdout:
                            if self.stop_event.is_set():
                                try:
                                    process.terminate()
                                except Exception:
                                    pass
                                push_db_history("已停止")
                                self.queue.put(("stopped",))
                                return
                            tail.append(line)
                            if show_output:
                                self.queue.put(("log", line))
                            if is_pgloader_error_log(line):
                                has_log_error = True
                            if table_line.match(line):
                                processed += 1
                                self.overall_processed_tables += 1
                                self.queue.put(("progress", db, processed, total_tables, idx, total_dbs, self.overall_processed_tables, self.overall_total_tables))
                        code = process.wait()
                    finally:
                        if cleanup_pgloader_temp:
                            cleanup_pgloader_rendered_file(rendered_path)
                    if has_log_error:
                        code = 1
                    self.current_process = None
                    if code != 0:
                        push_db_history("失败")
                        self.queue.put(("error", db, list(tail)))
                        return
                    ensure_pk_enabled = bool(config.get("pgloader", {}).get("ensure_primary_keys", True))
                    if ensure_pk_enabled and mode != "full":
                        pk_code, pk_output = ensure_target_primary_keys(db, config, selected_tables=selected_tables)
                        if pk_output:
                            self.queue.put(("log", pk_output + ("" if pk_output.endswith("\n") else "\n")))
                        if pk_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"补主键失败: {db}\n"))
                            return
                    sync_views_enabled = bool(config.get("pgloader", {}).get("sync_views", True))
                    if sync_views_enabled and mode != "full":
                        view_code, view_output = sync_views_for_db(db, config)
                        if view_output:
                            self.queue.put(("log", view_output + ("" if view_output.endswith("\n") else "\n")))
                        if view_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"同步视图失败: {db}\n"))
                            return
                    push_db_history("成功")
                    self.queue.put(("log", f"结构同步成功: {db}\n"))
                    self.queue.put(("phase_done",))
                elif phase == "primary_key":
                    ensure_pk_enabled = bool(config.get("pgloader", {}).get("ensure_primary_keys", True))
                    if ensure_pk_enabled:
                        pk_code, pk_output = ensure_target_primary_keys(db, config, selected_tables=selected_tables)
                        if pk_output:
                            self.queue.put(("log", pk_output + ("" if pk_output.endswith("\n") else "\n")))
                        if pk_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"补主键失败: {db}\n"))
                            return
                    push_db_history("成功")
                    self.queue.put(("log", f"主键同步成功: {db}\n"))
                    self.queue.put(("phase_done",))
                elif phase == "view":
                    if bool(config.get("pgloader", {}).get("clear_public_views_before_view_sync", False)):
                        clear_view_code, clear_view_output = clear_target_public_views(db, config)
                        if clear_view_output:
                            self.queue.put(("log", clear_view_output + ("" if clear_view_output.endswith("\n") else "\n")))
                        if clear_view_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"清理目标库视图失败: {db}\n"))
                            return
                    sync_views_enabled = bool(config.get("pgloader", {}).get("sync_views", True))
                    if sync_views_enabled:
                        view_code, view_output = sync_views_for_db(db, config)
                        if view_output:
                            self.queue.put(("log", view_output + ("" if view_output.endswith("\n") else "\n")))
                        if view_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"同步视图失败: {db}\n"))
                            return
                    push_db_history("成功")
                    self.queue.put(("log", f"视图同步成功: {db}\n"))
                    self.queue.put(("phase_done",))
                else:
                    if bool(config.get("pgloader", {}).get("clear_table_data_before_data_sync", False)):
                        clear_data_code, clear_data_output = clear_target_public_table_data(db, config, selected_tables=None)
                        if clear_data_output:
                            self.queue.put(("log", clear_data_output + ("" if clear_data_output.endswith("\n") else "\n")))
                        if clear_data_code != 0:
                            push_db_history("失败")
                            self.queue.put(("failed", f"清理目标库表数据失败: {db}\n"))
                            return
                    datax_cfg = config.get("datax", {})
                    if not bool(datax_cfg.get("enabled", False)):
                        push_db_history("失败")
                        self.queue.put(("failed", "DataX 未启用，请先勾选 DataX 启用。\n"))
                        return
                    datax_home = resolve_datax_home(self.workspace, datax_cfg)
                    datax_py = os.path.join(datax_home, "bin", "datax.py")
                    if not datax_home or not os.path.isfile(datax_py):
                        push_db_history("失败")
                        self.queue.put((
                            "failed",
                            f"DataX 配置无效，未找到: {datax_py}\n"
                            "请检查 datax.home 配置和当前工作目录。\n",
                        ))
                        return
                    datax_cmd_cfg = dict(datax_cfg)
                    datax_cmd_cfg["home"] = datax_home
                    mysql_conn = resolve_mysql_conn(config, db)
                    source_uri_template = datax_cfg.get("source_uri") or config.get("source", {}).get("uri", "")
                    source_uri = normalize_db_uri(source_uri_template.replace("{{DB_NAME}}", db))
                    target_uri = normalize_db_uri(config.get("target", {}).get("uri", "").replace("{{DB_NAME}}", db))
                    tables = get_mysql_tables(
                        str(mysql_conn.get("container", "")),
                        str(mysql_conn.get("user", "")),
                        str(mysql_conn.get("password", "")),
                        db,
                        host=str(mysql_conn.get("host", "")),
                        port=int(mysql_conn.get("port", 0) or 0),
                    )
                    if selected_tables:
                        tables, missing_tables = filter_tables_by_selected(tables, selected_tables)
                        if missing_tables:
                            self.queue.put(("log", f"选中表在 {db} 中不存在: {', '.join(missing_tables)}\n"))
                    else:
                        exclude_keywords = datax_cfg.get("exclude_table_keywords", [])
                        if not isinstance(exclude_keywords, list):
                            exclude_keywords = []
                        tables = [table for table in tables if not should_skip_table(table, exclude_keywords)]
                    if not tables:
                        self.queue.put(("log", f"DataX skipped: no tables found in {db}\n"))
                    else:
                        cleanup_on_finish = is_cleanup_jobs_on_finish(datax_cfg)
                        if cleanup_on_finish:
                            cleanup_datax_jobs_for_db(self.workspace, db, datax_cfg)
                        self.queue.put(("log", f"DataX start: {db}, tables={len(tables)}\n"))
                        show_output_datax = bool(datax_cfg.get("show_output", False))
                        compact_log = bool(datax_cfg.get("compact_log", True))
                        table_parallelism = max(1, int(datax_cfg.get("table_parallelism", 3)))
                        self.queue.put(("log", f"DataX table parallelism: {table_parallelism}\n"))
                        process_lock = threading.Lock()
                        def run_one_table(t_idx: int, table: str) -> tuple[int, str, str, str]:
                            if self.stop_event.is_set():
                                return 130, table, "", ""
                            columns = get_mysql_columns(
                                str(mysql_conn.get("container", "")),
                                str(mysql_conn.get("user", "")),
                                str(mysql_conn.get("password", "")),
                                db,
                                table,
                                host=str(mysql_conn.get("host", "")),
                                port=int(mysql_conn.get("port", 0) or 0),
                            )
                            if not columns:
                                self.queue.put(("log", f"DataX skipped table: {db}.{table} (no columns)\n"))
                                return 0, table, "", ""
                            split_pk = get_mysql_split_pk(
                                str(mysql_conn.get("container", "")),
                                str(mysql_conn.get("user", "")),
                                str(mysql_conn.get("password", "")),
                                db,
                                table,
                                host=str(mysql_conn.get("host", "")),
                                port=int(mysql_conn.get("port", 0) or 0),
                            )
                            channel = int(datax_cfg.get("channel", 2))
                            batch_size = int(datax_cfg.get("batch_size", 2000))
                            split_pk_disp = split_pk if split_pk else "none"
                            job_file = build_datax_job(self.workspace, db, table, columns, source_uri, target_uri, datax_cfg, split_pk=split_pk)
                            cmd_datax = build_datax_command(job_file, datax_cmd_cfg)
                            self.queue.put(("log", f"DataX [{t_idx}/{len(tables)}] {db}.{table} (channel={channel}, batch={batch_size}, splitPk={split_pk_disp})\n"))
                            cmd_env = os.environ.copy()
                            for key, value in datax_cfg.get("env", {}).items():
                                cmd_env[str(key)] = str(value)
                            process = subprocess.Popen(
                                cmd_datax,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                text=True,
                                encoding="utf-8",
                                errors="replace",
                                cwd=self.workspace,
                                env=cmd_env,
                            )
                            with process_lock:
                                self.active_processes.add(process)
                                self.current_process = process
                            dtail = deque(maxlen=200)
                            try:
                                assert process.stdout is not None
                                for line in process.stdout:
                                    if self.stop_event.is_set():
                                        try:
                                            process.terminate()
                                        except Exception:
                                            pass
                                        break
                                    dtail.append(line)
                                    if show_output_datax:
                                        if compact_log:
                                            if is_datax_key_log(line):
                                                self.queue.put(("log", line))
                                        else:
                                            self.queue.put(("log", line))
                                dcode = process.wait()
                            finally:
                                with process_lock:
                                    self.active_processes.discard(process)
                                    if self.current_process is process:
                                        self.current_process = None
                                if cleanup_on_finish:
                                    cleanup_datax_job_file(job_file)
                            if self.stop_event.is_set():
                                return 130, table, "", job_file
                            if dcode != 0:
                                return dcode, table, "".join(dtail), job_file
                            return 0, table, "", job_file
                        first_error: tuple[int, str, str, str] | None = None
                        with ThreadPoolExecutor(max_workers=table_parallelism) as executor:
                            futures = {
                                executor.submit(run_one_table, t_idx, table): table
                                for t_idx, table in enumerate(tables, start=1)
                            }
                            for future in as_completed(futures):
                                try:
                                    dcode, failed_table, detail, _ = future.result()
                                except CancelledError:
                                    # Other table tasks are cancelled after first failure; ignore these.
                                    continue
                                if dcode == 130:
                                    if first_error is not None:
                                        continue
                                    for pending in futures:
                                        pending.cancel()
                                    push_db_history("已停止")
                                    self.queue.put(("stopped",))
                                    return
                                if dcode != 0 and first_error is None:
                                    first_error = (dcode, failed_table, detail, "")
                                    self.stop_event.set()
                                    for pending in futures:
                                        pending.cancel()
                        if first_error is not None:
                            _, failed_table, detail, _ = first_error
                            if cleanup_on_finish:
                                cleanup_empty_datax_job_dir(self.workspace, datax_cfg)
                            tip = ""
                            if is_datax_jvm_oom(detail):
                                tip = (
                                    "Tip: DataX JVM 内存不足，请降低 DataX 表并行/通道/批大小，"
                                    "并将 JVM 调小（如 -Xms256m -Xmx1024m）。\n"
                                )
                            push_db_history("失败")
                            self.queue.put(("failed", f"\nDataX failed table: {db}.{failed_table}\n--- DataX output (last 200 lines) ---\n{detail}--- end ---\n{tip}"))
                            return
                        if cleanup_on_finish:
                            cleanup_empty_datax_job_dir(self.workspace, datax_cfg)
                        self.queue.put(("log", f"DataX success: {db}\n"))
                    if not self.stop_event.is_set():
                        push_db_history("成功")
                        self.queue.put(("phase_done",))
            self.queue.put(("done",))
        except Exception as exc:
            self.queue.put(("log", f"\n线程异常: {exc}\n"))
            self.queue.put(("stopped",))
