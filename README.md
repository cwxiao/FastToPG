# FastDBConvert v3.0.0

FastDBConvert 是一个 MySQL → PostgreSQL 迁移工具：
- 结构同步使用 `pgloader`
- 数据同步使用 DataX 工具包
- 结构/数据分开执行（先结构，后数据）

> 重要：从 v3.0.0 开始，`datax/` 目录不再作为代码仓库内容提交，改为通过 GitHub Release 附件提供。

## 1. 版本与依赖（精确）

以下版本为本项目 v3.0.0 的基线：

- Python：`3.10+`（建议 `3.12.x`）
- JDK：`8`（建议 `8u202+`）
- Docker Engine：`20.10+`
- pgloader 镜像：`dimitri/pgloader:v3.6.7`
- MySQL 镜像（源库容器场景）：`mysql:8.0.36`
- PostgreSQL 镜像（psql 清理容器场景）：`postgres:16.4`
- DataX 工具包：`3.0.0`（通过 Release 附件分发）

## 2. 镜像准备（精确命令）

先拉取迁移所需镜像：

```powershell
docker pull dimitri/pgloader:v3.6.7
docker pull mysql:8.0.36
docker pull postgres:16.4
```

## 3. DataX 附件下载与目录结构

### 3.1 下载方式

从 GitHub Release 的附件下载 DataX 工具包压缩文件（例如：`datax.tar.gz`，版本 `3.0.0`）。

### 3.2 解压目标目录

在项目根目录下解压，要求最终可执行路径为：

```text
datax/datax/bin/datax.py
```

示例（PowerShell）：

```powershell
New-Item -ItemType Directory -Force datax | Out-Null
tar -xzf .\datax.tar.gz -C .\datax
```

如果解压后层级不一致，请手动调整到如下结构：

```text
datax/
  datax/
    bin/datax.py
    conf/core.json
    plugin/
    log/
    log_perf/
```

## 4. 前提准备清单

执行迁移前必须满足：

1. `python`、`java`、`docker` 命令可用。
2. 源 MySQL 可访问（示例容器名：`mysql8`）。
3. 目标 PostgreSQL 可访问（可远程）。
4. 用于清理目标库 public 表的 psql 执行环境可用（示例容器名：`postgres16`）。
5. `pgloader_tool.json` 中连接信息已按环境填写。
6. DataX 附件已解压并满足 `datax/datax/bin/datax.py` 路径。

## 5. 配置文件关键项

编辑 `pgloader_tool.json`：

- `pgloader.image` 必须是 `dimitri/pgloader:v3.6.7`
- `target.psql_container` 默认 `postgres16`
- `datax.home` 默认 `datax/datax`
- `datax.source_uri` 建议本机模式使用 `127.0.0.1`
- `datax.table_parallelism`、`datax.channel`、`datax.batch_size` 按机器性能调优
- `datax.cleanup_jobs_on_finish=true`：无论成功/失败都清理 DataX job 临时文件
- `datax.log_retention_days` 与 `datax.log_dirs`：自动清理过期日志
- `pgloader.cleanup_temp_files=true`：自动清理 `.pgloader_rendered_*.load` 临时文件

## 6. 部署与运行全流程（推荐）

### 步骤 A：准备项目与附件

1. 获取代码（或下载源码包）
2. 下载并解压 DataX 附件到 `datax/datax`
3. 按实际环境修改 `pgloader_tool.json`

### 步骤 B：启动 GUI（推荐）

```powershell
./run_gui.ps1
```

GUI 操作顺序：

1. 在“配置”页签确认连接与参数
2. 在主页选择数据库
3. 点击“同步结构”（会弹窗确认备份）
4. 结构完成后点击“同步数据”

### 步骤 C：命令行（可选）

结构同步：

```powershell
python .\pgloader_tool.py --action structure --db iucp_normal
```

数据同步：

```powershell
python .\pgloader_tool.py --action data --db iucp_normal
```

多库执行示例：

```powershell
python .\pgloader_tool.py --action structure --db iucp_data --db iucp_normal
python .\pgloader_tool.py --action data --db iucp_data --db iucp_normal
```

## 7. 容器示例（本地演示场景）

如果你在本地演示环境中需要快速起容器，可参考：

```powershell
docker run -d --name mysql8 -e MYSQL_ROOT_PASSWORD=123456 -p 3306:3306 mysql:8.0.36
docker run -d --name postgres16 -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16.4
```

> 生产环境请按实际安全规范设置强密码、网络策略和持久化卷。

## 8. 日志与临时文件策略（v3.0.0）

- DataX job 文件：任务结束后自动清理（成功/失败都清理）
- pgloader 渲染临时文件：任务结束后自动清理（成功/失败都清理）
- DataX 日志目录：按 `log_retention_days` 自动清理过期日志

## 9. 常见问题

1. **DataX 未找到**
   - 检查 `datax.home` 与 `datax/datax/bin/datax.py` 路径是否一致。
2. **结构同步失败（表已存在）**
   - 确认 `clear_public_before_sync=true` 且目标库可执行 `DROP TABLE`。
3. **推送仓库过慢或失败**
   - 不要提交 `datax/` 目录及压缩包，DataX 通过 Release 附件分发。