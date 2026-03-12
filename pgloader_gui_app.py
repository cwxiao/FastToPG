import os
import queue
import subprocess
import threading
import tkinter as tk

from pgloader_gui_core import DEFAULT_CONFIG, SYNC_HISTORY_FILE, URI_HISTORY_FILE
from pgloader_gui_parts.config_mixin import ConfigMixin
from pgloader_gui_parts.data_mixin import DataMixin
from pgloader_gui_parts.history_mixin import HistoryMixin
from pgloader_gui_parts.ui_build_mixin import UiBuildMixin
from pgloader_gui_parts.ui_mixin import UiMixin
from pgloader_gui_parts.sync_worker_mixin import SyncWorkerMixin
from pgloader_gui_parts.worker_mixin import WorkerMixin


class PgloaderGUI(ConfigMixin, DataMixin, HistoryMixin, UiBuildMixin, UiMixin, SyncWorkerMixin, WorkerMixin, tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("FastDBConvert")
        self.geometry("1680x980")
        self.minsize(1400, 820)
        self.resizable(True, True)

        self.workspace = os.path.abspath(os.path.dirname(__file__))
        self.queue: "queue.Queue[tuple]" = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.current_process: subprocess.Popen | None = None
        self.active_processes: set[subprocess.Popen] = set()
        self.stop_event = threading.Event()
        self.overall_total_tables = 0
        self.overall_processed_tables = 0
        self.start_time = 0.0
        self.last_progress_time = 0.0
        self.last_progress_count = 0
        self.max_log_lines = 4000
        self.selected_dbs: list[str] = []
        self.selected_tables: list[str] = []
        self.table_all_items: list[str] = []
        self.info_refresh_after_id: str | None = None
        self.fallback_databases: list[str] = []
        self.full_sync_dbs: list[str] = []
        self.sync_history_path = os.path.join(self.workspace, SYNC_HISTORY_FILE)
        self.sync_history_records: list[dict] = []
        self.uri_history_path = os.path.join(self.workspace, URI_HISTORY_FILE)
        self.uri_history_records: list[str] = []
        self.current_mode = ""
        self.eta_task_total = 0
        self.eta_task_done = 0

        self.config_path = tk.StringVar(value=os.path.join(self.workspace, DEFAULT_CONFIG))
        self.load_template = tk.StringVar()
        self.source_type = tk.StringVar(value="mysql")
        self.source_uri = tk.StringVar()
        self.target_uri = tk.StringVar()
        self.target_psql_container = tk.StringVar(value="postgres16")
        self.mysql_container = tk.StringVar()
        self.mysql_user = tk.StringVar()
        self.mysql_password = tk.StringVar()
        self.pgloader_image = tk.StringVar()
        self.show_output = tk.BooleanVar(value=True)
        self.datax_enabled = tk.BooleanVar(value=False)
        self.datax_home = tk.StringVar()
        self.datax_python = tk.StringVar(value="python")
        self.datax_source_uri = tk.StringVar()
        self.datax_channel = tk.StringVar(value="2")
        self.datax_batch_size = tk.StringVar(value="2000")
        self.datax_table_parallelism = tk.StringVar(value="30")
        self.datax_log_retention_days = tk.StringVar(value="7")
        self.datax_verbose_log = tk.BooleanVar(value=False)
        self.table_filter_keyword = tk.StringVar(value="")

        self._build_ui()
        self._load_uri_history_records()
        self._load_sync_history_records()
        self._load_config_safe()
        self._poll_queue()


if __name__ == "__main__":
    app = PgloaderGUI()
    app.mainloop()
