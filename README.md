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

1. 已安装 PostgreSQL，并能连接目标数据库。
2. 已安装 MySQL，并能连接源数据库。
3. 已安装 pgloader，并能在命令行中直接调用。
4. Windows 下建议已配置好 PATH，以便脚本可直接调用 pgloader。
5. 具备源/目标数据库的连接信息与账号权限（读源库、写目标库）。

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
2. 在界面中填写源库、目标库、表/库映射与迁移参数。
3. 点击开始迁移。

### 方式二：命令行

1. 根据实际环境修改 pgloader_tool.json 与 mysql_to_pg.load。
2. 运行 pgloader_tool.py。

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

## 后续规划

- 增值功能：
  - 数据质量检查、迁移前评估报告、迁移后核对与差异报告。
  - 大库分批迁移、断点续传与自动重试。
  - 多数据库类型支持（如 SQL Server/Oracle → PostgreSQL）。
- 版本策略：
  - 免费版：基础迁移能力。
  - 专业版：可视化规则、批量任务、日志分析。