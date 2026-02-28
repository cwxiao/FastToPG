# FastDBConvert

一个基于 pgloader 的多源数据库 → PostgreSQL 迁移小工具，提供命令行与简易图形界面两种使用方式。

## 目录结构说明

- pgloader_tool.py
  - 命令行入口脚本。用于读取配置、组装并调用 pgloader 执行迁移。
- pgloader_gui.py
  - 图形界面入口脚本。提供可视化填写配置并触发迁移。
- pgloader_tool.json
  - 工具的配置文件（示例/默认配置）。包含源库、目标库、映射与迁移参数。
- mysql_to_pg.load
  - pgloader 的 load 脚本模板或示例，用于定义迁移规则。
- run_gui.ps1
  - Windows PowerShell 启动脚本，快速运行 GUI。

## 使用前提

1. Docker 可用（`docker` 命令可执行）。
2. 源 MySQL 容器可用（默认容器名 `mysql8`），并具备读取权限。
3. 目标 PostgreSQL 可访问（本项目示例为 `192.168.1.121:5432`），并具备建表/写入权限。
4. 目标 PostgreSQL 清理容器可用（默认容器名 `postgres16`，容器内需可执行 `psql`）。
5. Python 可用（建议 3.10+，用于运行本工具与 DataX 启动脚本）。
6. JDK 可用（DataX 运行依赖 Java，建议 JDK 8）。
7. DataX 工具包已下载并解压，`{DATAX_HOME}/bin/datax.py` 可执行。
8. 具备源/目标数据库连接信息与账号权限（读源库、写目标库）。

## 支持的数据源与目标（pgloader 能力）

### 源数据库

- MySQL
- MS SQL Server
- SQLite
- PostgreSQL
- Amazon Redshift（PostgreSQL 协议）

### 目标数据库

- PostgreSQL
- Citus（PostgreSQL 分布式扩展）

### 文件类数据源

- CSV
- Fixed-width（定长文件）
- dBase/DBF
- IBM IXF
- 归档文件（zip/tar/gzip）
- HTTP(S) 远程文件

## 组件版本（建议）

- Python：3.10+
- Docker：20.10+
- pgloader：3.6.x（建议固定镜像标签，例如 dimitri/pgloader:3.6.9）
- MySQL：8.0+
- PostgreSQL：14+

## 快速使用

### 方式一：图形界面

1. 运行 run_gui.ps1。
2. 主页三栏：左侧“源数据库”、中间“日志”、右侧“目标数据库”（支持拖拽调整宽度）。
3. 打开窗口时会自动查询并加载源数据库列表，同时查询目标数据库列表。
4. 点击“刷新数据库”可再次查询源库与目标库并刷新界面。
5. 配置：所有连接与同步参数都在“配置”页签中维护。
6. 点击“同步结构”时会先弹出确认框：提示“将清空目标库 public 下所有表，请先备份”。
7. 点击“我已备份，开始同步”后，才会先清空目标库 `public` 下已有表，再执行 pgloader（建表/索引等结构）。
8. 结构成功后，再点击“同步数据”执行 DataX。

### 方式二：命令行

1. 根据实际环境修改 pgloader_tool.json 与 mysql_to_pg.load。
2. 结构同步：`python pgloader_tool.py --action structure --db iucp_normal`
3. 数据同步：`python pgloader_tool.py --action data --db iucp_normal`

## DataX 工具包联动（仅工具包方式）

- 支持流程：先用 pgloader 迁移结构，再用 DataX 迁移数据（两步分开执行，不自动串联）。
- 当你手工执行“同步数据”时，调用本机 DataX 工具包。
- `pgloader_tool.json` 中 `datax` 关键配置：
  - `home`：DataX 解压目录（例如 `D:\tools\datax`）。
  - `python`：Python 命令（默认 `python`）。
  - `source_uri`：DataX 专用源库连接（可与 `source.uri` 不同，例如本机工具包使用 `127.0.0.1`）。
  - `mysql_jdbc_params`：附加到 DataX MySQL JDBC 的参数（默认 `useSSL=false`，用于消除 SSL 警告日志）。
  - `target_table_lowercase`：写入 PostgreSQL 时目标表名转小写（默认 `true`，适配 PG 全小写命名）。
  - `target_column_lowercase`：写入 PostgreSQL 时目标字段名转小写（默认 `true`，适配 PG 全小写字段）。
  - `table_parallelism`：多表并行度（默认 `30`，表示同时跑 30 张表）。
  - `exclude_table_keywords`：跳过表名关键字（默认 `log`、`copy`）。
  - `compact_log`：精简 DataX 日志输出（默认 `true`）。
  - `show_output`：是否输出 DataX 运行日志（默认 `true`）。
  - `jvm`：DataX JVM 参数（建议 `-Xms2g -Xmx6g -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError`）。
  - `loglevel`：DataX 日志级别（建议 `warn`，降低日志压力）。
  - `channel`：并发通道数。
  - `batch_size`：写入批大小。
  - `job_dir`：自动生成的 DataX job 目录。
  - `cleanup_jobs_on_finish`：DataX job 文件在任务结束后立即清理（成功/失败都会清理）。
  - `log_retention_days`：DataX 日志保留天数，超过天数自动清理。
  - `log_dirs`：需要做保留期清理的日志目录列表（默认 `datax/datax/log`、`datax/datax/log_perf`）。

## 结构同步前清理（pgloader）

- 结构同步前默认自动清理目标库 `public` schema 下已有表，避免“表已存在”导致建表失败。
- 对应配置：`pgloader.clear_public_before_sync`（默认 `true`）。
- pgloader 渲染产生的临时 load 文件会在每次结构同步后自动清理（成功/失败都会清理）。
- 对应配置：`pgloader.cleanup_temp_files`（默认 `true`）。

## 稳定性建议（大库/长任务）

- 提速建议：适当提高 `channel`（例如 2~8）和 `batch_size`（例如 5000~20000）。
- 多表并行提速：提高 `table_parallelism`（例如 8~30），让多个表同时执行。
- 本工具会对“单列数值主键”表自动设置 DataX `splitPk`，可显著提升该类表同步速度。
- 默认日志建议：`show_output=true` + `compact_log=true`（简略日志，便于观察进度）。
- 排障时可切换详细日志：`show_output=true` + `compact_log=false`（GUI 可直接勾选 DataX 详细日志）。
- DataX 日志会按 `log_retention_days` 自动清理过期文件，避免长期堆积。

## DataX 工具包独立运行

- 直接下载 DataX 工具包并解压到本地目录。
- 进入 `bin` 目录运行：
  - `cd {YOUR_DATAX_HOME}/bin`
  - `python datax.py {YOUR_JOB.json}`
- 自检命令：
  - `python {YOUR_DATAX_HOME}/bin/datax.py {YOUR_DATAX_HOME}/job/job.json`
- 已提供脚本：`run_datax.ps1`
  - 自检：`./run_datax.ps1 -DataXHome "你的DataX目录"`
  - 运行任务：`./run_datax.ps1 -DataXHome "你的DataX目录" -JobFile "你的job.json路径"`

## 配置说明（概要）

- pgloader_tool.json：
  - 存放数据库连接信息、迁移参数与可选规则。
  - `source.uri` 与 `target.uri` 支持 `{{DB_NAME}}` 占位符。
- mysql_to_pg.load：
  - pgloader 脚本，用于指定迁移规则（表映射、数据清洗、类型转换等）。
  - 支持 `{{SOURCE_URI}}` 与 `{{TARGET_URI}}` 占位符。

## 常见问题

- 无法连接数据库：请检查数据库地址、端口、防火墙与账号权限。
- 找不到 pgloader：请确认已安装 pgloader 并已加入 PATH。
- DataX 出现 MySQL SSL WARN：通常不影响同步结果；默认配置已设置 `mysql_jdbc_params=useSSL=false` 以抑制该警告。

## 后续规划

- 增值功能：
  - 数据质量检查、迁移前评估报告、迁移后核对与差异报告。
  - 大库分批迁移、断点续传与自动重试。
  - 多数据库类型支持（如 SQL Server/Oracle → PostgreSQL）。
- 版本策略：
  - 免费版：基础迁移能力。
  - 专业版：可视化规则、批量任务、日志分析。