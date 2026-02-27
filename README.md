# FastToPG

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

1. 已安装 PostgreSQL，并能连接目标数据库。
2. 已安装 MySQL，并能连接源数据库。
3. 具备源/目标数据库的连接信息与账号权限（读源库、写目标库）。
4. 运行方式二选一：
  - Docker 模式：需要安装 Docker，并确保可访问 Docker。
  - 本地模式：需要安装 pgloader 并加入 PATH（或在界面里填写路径）。

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
2. 在“配置文件”处选择或加载 pgloader_tool.json。
3. 选择“源类型”，填写“源连接 URI”和“目标连接 URI”。
  - URI 可包含 `{{DB_NAME}}` 占位符。
4. 在“高级设置”选择 pgloader 运行方式（Docker/本地）。
5. 选择/编辑“模板文件”，确认或修改“Load 脚本内容”。
  - 脚本内可使用 `{{SOURCE_URI}}` 与 `{{TARGET_URI}}` 占位符。
6. 可点击“测试源连接/测试目标连接”进行连通性检查。
7. 在左侧列表中选择要迁移的数据库（可多选）。
8. 点击“开始同步”，观察日志与进度，日志文件会保存到 logs 目录。

示例（MySQL → PostgreSQL）：

- 源连接 URI：`mysql://root:123456@host.docker.internal:3306/{{DB_NAME}}`
- 目标连接 URI：`postgresql://fasttopg:FastToPG123@host.docker.internal:5432/{{DB_NAME}}`

### 方式二：命令行

1. 根据实际环境修改 pgloader_tool.json 与 mysql_to_pg.load：
  - `source.type`：源类型（mysql/mssql/sqlite/pgsql/redshift/file）
  - `source.uri`：源连接 URI（支持 `{{DB_NAME}}`）
  - `target.uri`：目标连接 URI（支持 `{{DB_NAME}}`）
  - `load_template`：load 脚本文件名
2. 执行：
  - 迁移配置中所有数据库：运行 `pgloader_tool.py`
  - 仅迁移指定库：运行 `pgloader_tool.py --db your_db_name`（可重复传入多个 `--db`）

示例（仅迁移一个库）：

- `pgloader_tool.py --db iucp_data`

## 配置说明（概要）

- pgloader_tool.json：
  - 存放数据库连接信息、迁移参数与可选规则。
  - `source.uri` 与 `target.uri` 支持 `{{DB_NAME}}` 占位符。
  - `target_user` 可选自动创建目标库用户（需要目标库管理员权限）。
  - 建议目标用户密码使用字母/数字，避免特殊字符导致工具解析差异。
  - 启用 `target_user.auto_create` 时，需要提供 `target.admin_uri`（管理员连接）。
  - `pgloader` 支持 Docker 与本地两种运行方式。
- mysql_to_pg.load：
  - pgloader 脚本，用于指定迁移规则（表映射、数据清洗、类型转换等）。
  - 支持 `{{SOURCE_URI}}` 与 `{{TARGET_URI}}` 占位符。

## 其他功能

- 模板示例：内置常见源类型模板，可一键应用再微调。
- 配置档案：自动列出工作目录内的 JSON 配置文件，快速切换。
- 日志文件：每次运行都会在 logs 目录生成日志文件。
- 版本信息：界面顶部显示版本号，可一键检查更新。
- 目标用户：可在“高级设置”里勾选自动创建并授权目标库用户。
  - 自动创建依赖 PG 管理镜像（默认 postgres:16.11）。
- 镜像管理：可在“高级设置”一键检查 Docker 并拉取 pgloader/PG 管理镜像。

## 常见问题

- 无法连接数据库：请检查数据库地址、端口、防火墙与账号权限。
- 找不到 pgloader：请确认已安装 pgloader 并已加入 PATH。
- host.docker.internal 无法测试：该地址仅在容器内可用，请在界面勾选“容器内测试”。

## 后续规划

- 增值功能：
  - 数据质量检查、迁移前评估报告、迁移后核对与差异报告。
  - 大库分批迁移、断点续传与自动重试。
  - 多数据库类型支持（如 SQL Server/Oracle → PostgreSQL）。
- 版本策略：
  - 免费版：基础迁移能力。
  - 专业版：可视化规则、批量任务、日志分析。