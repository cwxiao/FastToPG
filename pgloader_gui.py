import json
import os
import queue
import re
import subprocess
import threading
import time
import tkinter as tk
from collections import deque
from tkinter import ttk, messagebox, filedialog

DEFAULT_CONFIG = "pgloader_tool.json"
SOURCE_TYPES = ["mysql", "mssql", "sqlite", "pgsql", "redshift", "file"]


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(path: str, config: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def run_command(cmd: list) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


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


def render_load_file(template_path: str, output_path: str, replacements: dict) -> None:
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


class PgloaderGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("FastToPG")
        self.geometry("900x640")
        self.resizable(True, True)

        self.workspace = os.path.abspath(os.path.dirname(__file__))
        self.queue: "queue.Queue[tuple]" = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.current_process: subprocess.Popen | None = None
        self.stop_event = threading.Event()
        self.overall_total_tables = 0
        self.overall_processed_tables = 0
        self.start_time = 0.0
        self.last_progress_time = 0.0
        self.last_progress_count = 0

        self.config_path = tk.StringVar(value=os.path.join(self.workspace, DEFAULT_CONFIG))
        self.load_template = tk.StringVar()
        self.source_type = tk.StringVar(value="mysql")
        self.source_uri = tk.StringVar()
        self.target_uri = tk.StringVar()
        self.mysql_container = tk.StringVar()
        self.mysql_user = tk.StringVar()
        self.mysql_password = tk.StringVar()
        self.pgloader_image = tk.StringVar()
        self.show_output = tk.BooleanVar(value=True)

        self._build_ui()
        self._load_config_safe()
        self._poll_queue()

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=10)
        top.pack(fill=tk.BOTH, expand=True)

        cfg_frame = ttk.LabelFrame(top, text="配置")
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

        mid = ttk.Frame(top)
        mid.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        left = ttk.Frame(mid)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)

        right = ttk.Frame(mid)
        right.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5)

        db_frame = ttk.LabelFrame(left, text="数据库")
        db_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.db_list = tk.Listbox(db_frame, selectmode=tk.EXTENDED)
        self.db_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.db_list.bind("<<ListboxSelect>>", self._on_db_select)
        db_scroll = ttk.Scrollbar(db_frame, command=self.db_list.yview)
        db_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.db_list.configure(yscrollcommand=db_scroll.set)

        db_buttons = ttk.Frame(left)
        db_buttons.pack(fill=tk.X, pady=5)
        self.db_add_btn = ttk.Button(db_buttons, text="新增", command=self._add_db)
        self.db_add_btn.pack(side=tk.LEFT)
        self.db_remove_btn = ttk.Button(db_buttons, text="删除", command=self._remove_db)
        self.db_remove_btn.pack(side=tk.LEFT, padx=5)

        settings = ttk.LabelFrame(right, text="设置")
        settings.pack(fill=tk.BOTH, expand=True, pady=5)

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
        row += 1

        ttk.Label(settings, text="目标连接 URI").grid(row=row, column=0, sticky="w")
        self.target_uri_entry = ttk.Entry(settings, textvariable=self.target_uri)
        self.target_uri_entry.grid(row=row, column=1, sticky="we", padx=5, pady=2)
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

        # Always show pgloader output; no toggle needed.

        settings.columnconfigure(1, weight=1)

        bottom = ttk.Frame(top)
        bottom.pack(fill=tk.X, padx=5, pady=5)

        self.run_button = ttk.Button(bottom, text="开始同步", command=self._run_selected)
        self.run_button.pack(side=tk.LEFT)
        self.stop_button = ttk.Button(bottom, text="停止", command=self._stop_run, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        self.progress_label = ttk.Label(bottom, text="空闲")
        self.progress_label.pack(side=tk.LEFT, padx=10)

        self.progress = ttk.Progressbar(bottom, length=300, mode="indeterminate")
        self.progress.pack(side=tk.LEFT, padx=5)

        self.eta_label = ttk.Label(bottom, text="预计剩余: --")
        self.eta_label.pack(side=tk.LEFT, padx=10)

        self.size_label = ttk.Label(bottom, text="已选数据库大小: --")
        self.size_label.pack(side=tk.LEFT, padx=10)

        log_frame = ttk.LabelFrame(top, text="日志")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.log_text = tk.Text(log_frame, height=12)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=log_scroll.set)

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

        self.db_list.delete(0, tk.END)
        for db in config.get("databases", []):
            self.db_list.insert(tk.END, db)
        self.size_label.configure(text="已选数据库大小: --")

        self.load_template.set(config.get("load_template", "mysql_to_pg.load"))
        source_cfg = config.get("source", {})
        target_cfg = config.get("target", {})
        self.source_type.set(source_cfg.get("type", "mysql"))
        self.source_uri.set(source_cfg.get("uri", ""))
        self.target_uri.set(target_cfg.get("uri", ""))
        mysql_cfg = config.get("mysql", {})
        self.mysql_container.set(mysql_cfg.get("container", "mysql8"))
        self.mysql_user.set(mysql_cfg.get("user", "root"))
        self.mysql_password.set(mysql_cfg.get("password", ""))

        pgloader_cfg = config.get("pgloader", {})
        self.pgloader_image.set(pgloader_cfg.get("image", "dimitri/pgloader"))
        self.show_output.set(True)
        env = pgloader_cfg.get("env", {})
        self.env_text.delete("1.0", tk.END)
        self.env_text.insert(tk.END, json.dumps(env, ensure_ascii=False, indent=2))

        self._reload_template()

    def _save_config_safe(self) -> None:
        try:
            env = json.loads(self.env_text.get("1.0", tk.END).strip() or "{}")
        except json.JSONDecodeError as exc:
            messagebox.showerror("错误", f"环境变量 JSON 无效: {exc}")
            return

        config = {
            "databases": list(self.db_list.get(0, tk.END)),
            "load_template": self.load_template.get().strip(),
            "source": {
                "type": self.source_type.get().strip(),
                "uri": self.source_uri.get().strip(),
            },
            "target": {
                "uri": self.target_uri.get().strip(),
            },
            "mysql": {
                "container": self.mysql_container.get().strip(),
                "user": self.mysql_user.get().strip(),
                "password": self.mysql_password.get(),
            },
            "pgloader": {
                "image": self.pgloader_image.get().strip(),
                "env": env,
                "show_output": True,
            },
        }

        try:
            save_config(self.config_path.get(), config)
        except Exception as exc:
            messagebox.showerror("错误", f"保存配置失败: {exc}")
            return

        messagebox.showinfo("已保存", "配置已保存。")

    def _add_db(self) -> None:
        db = tk.simpledialog.askstring("新增数据库", "数据库名称:")
        if db:
            self.db_list.insert(tk.END, db.strip())

    def _remove_db(self) -> None:
        for idx in reversed(self.db_list.curselection()):
            self.db_list.delete(idx)

    def _run_selected(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("运行中", "当前正在同步，请先完成或停止。")
            return

        dbs = [self.db_list.get(i) for i in self.db_list.curselection()]
        if not dbs:
            messagebox.showinfo("请选择", "请至少选择一个数据库。")
            return

        try:
            env = json.loads(self.env_text.get("1.0", tk.END).strip() or "{}")
        except json.JSONDecodeError as exc:
            messagebox.showerror("错误", f"环境变量 JSON 无效: {exc}")
            return

        config = {
            "databases": dbs,
            "load_template": self.load_template.get().strip(),
            "source": {
                "type": self.source_type.get().strip(),
                "uri": self.source_uri.get().strip(),
            },
            "target": {
                "uri": self.target_uri.get().strip(),
            },
            "mysql": {
                "container": self.mysql_container.get().strip(),
                "user": self.mysql_user.get().strip(),
                "password": self.mysql_password.get(),
            },
            "pgloader": {
                "image": self.pgloader_image.get().strip(),
                "env": env,
                "show_output": True,
            },
        }

        self.run_button.configure(state=tk.DISABLED)
        self.stop_button.configure(state=tk.NORMAL)
        self.progress.configure(value=0)
        self.progress_label.configure(text="启动中...")
        self.eta_label.configure(text="预计剩余: --")
        self.size_label.configure(text="已选数据库大小: --")
        self.log_text.delete("1.0", tk.END)
        self.stop_event.clear()
        self.overall_total_tables = 0
        self.overall_processed_tables = 0
        self.start_time = time.time()
        self.last_progress_time = self.start_time
        self.last_progress_count = 0

        self._set_controls_running(True)
        self.progress.start(80)

        self.worker_thread = threading.Thread(target=self._worker, args=(config,), daemon=True)
        self.worker_thread.start()

    def _stop_run(self) -> None:
        self.stop_event.set()
        if self.current_process and self.current_process.poll() is None:
            try:
                self.current_process.terminate()
            except Exception:
                pass
        self.queue.put(("log", "\n已请求停止。\n"))
        if not self.current_process:
            self.queue.put(("stopped",))

    def _worker(self, config: dict) -> None:
        try:
            self.overall_total_tables = 0
            self.overall_processed_tables = 0
            total_dbs = len(config["databases"])
            for db in config["databases"]:
                source_type = config.get("source", {}).get("type", "mysql")
                if source_type == "mysql":
                    mysql_cfg = config["mysql"]
                    self.overall_total_tables += get_total_tables(
                        mysql_cfg["container"],
                        mysql_cfg["user"],
                        mysql_cfg["password"],
                        db,
                    )

            for idx, db in enumerate(config["databases"], start=1):
                if self.stop_event.is_set():
                    self.queue.put(("stopped",))
                    return
                self.queue.put(("log", f"===============================\n开始同步数据库: {db}\n===============================\n"))

                mysql_cfg = config["mysql"]
                source_type = config.get("source", {}).get("type", "mysql")
                total_tables = 0
                if source_type == "mysql":
                    total_tables = get_total_tables(
                        mysql_cfg["container"],
                        mysql_cfg["user"],
                        mysql_cfg["password"],
                        db,
                    )

                source_uri = config.get("source", {}).get("uri", "").replace("{{DB_NAME}}", db)
                target_uri = config.get("target", {}).get("uri", "").replace("{{DB_NAME}}", db)

                template_path = os.path.join(self.workspace, config["load_template"])
                rendered_name = f".pgloader_rendered_{db}.load"
                rendered_path = os.path.join(self.workspace, rendered_name)
                render_load_file(
                    template_path,
                    rendered_path,
                    {
                        "DB_NAME": db,
                        "SOURCE_URI": source_uri,
                        "TARGET_URI": target_uri,
                    },
                )

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

                assert process.stdout is not None
                for line in process.stdout:
                    if self.stop_event.is_set():
                        try:
                            process.terminate()
                        except Exception:
                            pass
                        self.queue.put(("stopped",))
                        return
                    tail.append(line)
                    if show_output:
                        self.queue.put(("log", line))
                    if table_line.match(line):
                        processed += 1
                        self.overall_processed_tables += 1
                        self.queue.put(("progress", db, processed, total_tables, idx, total_dbs, self.overall_processed_tables, self.overall_total_tables))

                code = process.wait()
                self.current_process = None
                if code != 0:
                    self.queue.put(("error", db, list(tail)))
                    return
                self.queue.put(("log", f"同步成功: {db}\n"))

            self.queue.put(("done",))
        except Exception as exc:
            self.queue.put(("log", f"\n线程异常: {exc}\n"))
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
            self.log_text.see(tk.END)
        elif kind == "progress":
            db, processed, total, idx, total_dbs, overall_done, overall_total = msg[1:]
            if total > 0:
                percent = min(100, int((processed * 100) / total))
                self.progress.configure(value=percent)
                self.progress_label.configure(text=f"{db}: {percent}% ({processed}/{total}) [DB {idx}/{total_dbs}]")
            else:
                self.progress_label.configure(text=f"{db}: {processed} tables [DB {idx}/{total_dbs}]")

            self._update_eta(overall_done, overall_total)
        elif kind == "error":
            db, tail = msg[1], msg[2]
            self.log_text.insert(tk.END, f"\n同步失败: {db}\n")
            self.log_text.insert(tk.END, "--- pgloader 输出 (最后 200 行) ---\n")
            self.log_text.insert(tk.END, "".join(tail))
            self.log_text.insert(tk.END, "--- 结束 ---\n")
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

    def _set_idle(self) -> None:
        self.progress.stop()
        self.progress.configure(value=0)
        self.progress_label.configure(text="空闲")
        self.eta_label.configure(text="预计剩余: --")
        self.size_label.configure(text="已选数据库大小: --")
        self.run_button.configure(state=tk.NORMAL)
        self.stop_button.configure(state=tk.DISABLED)
        self.current_process = None
        self.stop_event.clear()
        self._set_controls_running(False)

    def _update_eta(self, done: int, total: int) -> None:
        if total <= 0:
            self.eta_label.configure(text="ETA: --")
            return

        now = time.time()
        if done <= 0:
            self.eta_label.configure(text="ETA: --")
            return

        elapsed = now - self.start_time
        if elapsed <= 0:
            self.eta_label.configure(text="ETA: --")
            return

        rate = done / elapsed
        if rate <= 0:
            self.eta_label.configure(text="ETA: --")
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

    def _set_controls_running(self, running: bool) -> None:
        state = tk.DISABLED if running else tk.NORMAL
        self.run_button.configure(state=tk.DISABLED if running else tk.NORMAL)
        self.stop_button.configure(state=tk.NORMAL if running else tk.DISABLED)

        self.db_list.configure(state=state)
        self.db_add_btn.configure(state=state)
        self.db_remove_btn.configure(state=state)

        self.config_entry.configure(state=state)
        self.config_browse_btn.configure(state=state)
        self.config_load_btn.configure(state=state)
        self.config_save_btn.configure(state=state)

        self.load_template_entry.configure(state=state)
        self.source_type_combo.configure(state=state)
        self.source_uri_entry.configure(state=state)
        self.target_uri_entry.configure(state=state)
        self.mysql_container_entry.configure(state=state)
        self.mysql_user_entry.configure(state=state)
        self.mysql_password_entry.configure(state=state)
        self.pgloader_image_entry.configure(state=state)
        self.env_text.configure(state=state)
        self.load_text.configure(state=state)
        self.load_reload_btn.configure(state=state)
        self.load_save_btn.configure(state=state)
        # always on

    def _on_db_select(self, _event=None) -> None:
        dbs = [self.db_list.get(i) for i in self.db_list.curselection()]
        if not dbs:
            self.size_label.configure(text="已选数据库大小: --")
            return
        threading.Thread(target=self._compute_size_async, args=(dbs,), daemon=True).start()

    def _compute_size_async(self, dbs: list[str]) -> None:
        if self.source_type.get().strip() != "mysql":
            self.queue.put(("size", 0))
            return
        mysql_container = self.mysql_container.get().strip()
        mysql_user = self.mysql_user.get().strip()
        mysql_password = self.mysql_password.get()

        total_bytes = 0
        for db in dbs:
            cmd = [
                "docker",
                "exec",
                mysql_container,
                "mysql",
                f"-u{mysql_user}",
                f"-p{mysql_password}",
                "-N",
                "-e",
                (
                    "SELECT COALESCE(SUM(data_length + index_length), 0) "
                    f"FROM information_schema.tables WHERE table_schema='{db}';"
                ),
            ]
            result = run_command(cmd)
            if result.returncode != 0:
                continue
            output = (result.stdout or "").strip().splitlines()
            if output:
                try:
                    total_bytes += int(output[0].strip())
                except ValueError:
                    pass

        self.queue.put(("size", total_bytes))

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

    def _format_size(self, num_bytes: int) -> str:
        if num_bytes < 1024:
            return f"{num_bytes} B"
        for unit in ["KB", "MB", "GB", "TB"]:
            num_bytes /= 1024
            if num_bytes < 1024:
                return f"{num_bytes:.2f} {unit}"
        return f"{num_bytes:.2f} PB"


if __name__ == "__main__":
    app = PgloaderGUI()
    app.mainloop()
