# PixWatch DL

PixWatch DL 持续同步指定 Pixiv 账号的公共收藏，基于 [`gallery-dl`](https://github.com/mikf/gallery-dl) 完成图片落盘与断点续传。首次启动时执行全量回灌，随后每 10 分钟做增量巡检，确保只抓取新增公共收藏并避免重复下载。

## 功能特性

- **全量 + 增量同步**：缺失或空的下载归档文件将触发一次全量同步；之后使用相同的 `--download-archive` 做增量补齐。
- **幂等与防重复**：所有 `gallery-dl` 任务共享同一归档文件，并保留 `.part` 临时文件以支持断点续传。
- **稳健性**：文件锁避免重入，执行超时会强制终止子进程。网络异常（含 429、超时等）自动指数退避最多 3 次。
- **资源守护**：在检测到磁盘剩余空间低于阈值时跳过本轮并输出告警日志。
- **观测性**：结构化 JSON 日志包含阶段、耗时、重试次数及新增/跳过/失败统计。最近一次任务状态写入健康检查文件，可供外部探针读取。
- **Telegram 推送**：所有新增下载写入持久队列，由后台 Sender Worker 发送至指定聊天，支持媒体组合并、限流退避、文档降级与发送幂等。

## 安装与运行

1. **准备配置**：
   - `gallery-dl oauth:pixiv` 已预先生成 OAuth 凭据，并由本程序通过环境变量或配置文件读取。
   - 提供以下字段（可写入 TOML 并通过 `PIXWATCH_CONFIG_FILE` 指定，或使用 `PIXWATCH_<NAME>` 环境变量覆盖）：
     - `user_id`：Pixiv 用户 ID。
     - `download_directory`：图片下载目录。
     - `archive_path`：`gallery-dl --download-archive` 路径。
     - `lock_path`：互斥锁文件路径。
     - `health_path`：健康检查状态文件路径。
     - `gallery_dl_path`（可选）：`gallery-dl` 可执行程序。
     - `concurrency`、`sleep`、`sleep_request`、`retries`：控制 `gallery-dl` 并发与限速。
     - `timeout_seconds`、`max_run_seconds`：单次任务与整体周期的最长运行时间。
     - `poll_interval_seconds`：增量同步间隔（默认 600 秒）。
     - `disk_free_threshold_bytes`：磁盘剩余空间阈值，低于则跳过。
     - `backoff_initial_seconds`、`backoff_multiplier`、`backoff_max_seconds`：网络退避策略。
     - `proxy`（可选）：HTTP/HTTPS 代理地址。
     - `extra_gallery_args`（可选）：追加给 `gallery-dl` 的参数序列。
     - `telegram_bot_token`、`telegram_chat_ids`（可选）：启用 Telegram 推送所需的 Bot Token 与目标 Chat ID（可多值，空格/逗号分隔）。
     - `telegram_queue_path`（可选）：Telegram 发送队列 SQLite 文件路径；未配置时默认放置于下载目录。
     - `telegram_caption_template`、`telegram_caption_max_length`、`telegram_include_tags`：自定义消息模板、长度上限与标签附加策略。
     - `telegram_send_media_group`、`telegram_worker_poll_seconds`、`telegram_max_attempts`、`telegram_retry_backoff_*`：发送批量策略、轮询频率与退避参数。
     - `telegram_document_fallback`、`telegram_disable_notifications`、`telegram_proxy`：文档降级、通知开关与独立代理配置。

2. **安装依赖**：`pip install -e .[dev]`（开发环境需 `pytest`）。

3. **运行方式**：
   - 常驻模式：`python -m pixwatch_dl.main`
   - 单次任务：`python -m pixwatch_dl.once`

首次运行会自动触发全量同步，之后每次只下载新增收藏。运行日志将打印到标准输出（JSON 格式），健康检查文件包含最近一次同步的状态与时间戳。

## 运维集成

- **systemd**：提供一个 `.service` 与 `.timer` 单元（需自行创建文件并指向 `python -m pixwatch_dl.once`）。关键字段：服务需定义 `WorkingDirectory`、`Environment`（或 `EnvironmentFile`）加载配置，定时器设定 `OnBootSec=10min`、`OnUnitActiveSec=10min` 保证 10 分钟巡检。确保 `RequiresMountsFor` 覆盖下载目录所在挂载点。
- **容器化（可选）**：如需容器部署，可自建 `Dockerfile`，要求将配置文件与 OAuth 凭据挂载为只读卷，并在容器入口执行 `python -m pixwatch_dl.main`。
- **健康检查**：读取配置中 `health_path` 指向的 JSON 文件获取 `status`、`timestamp`、`added`、`skipped`、`failed` 等字段，可由外部监控系统或 `/health` 静态探针读取。启用 Telegram 后会额外暴露 `tg_queue`、`tg_sent`、`tg_failed`、`tg_retries`、`tg_rate_limited`、`tg_last_sent` 指标。

## 项目结构

- `.gitignore`：忽略 Python 编译产物和虚拟环境目录，保持仓库整洁。
- `pyproject.toml`：声明包元数据、依赖、可选开发组件及 pytest 配置。
- `README.md`：提供功能概述、配置方法、运行模式、运维整合说明与文件索引。
- `pixwatch_dl/config.py`：加载配置文件与环境变量，校验字段并确保必要目录存在。
- `pixwatch_dl/logging_setup.py`：初始化结构化 JSON 日志格式与全局日志级别。
- `pixwatch_dl/gallery.py`：构造 `gallery-dl` 调用参数，处理重试、限速、超时与统计解析。
- `pixwatch_dl/backfill.py`：在归档缺失时执行公共收藏全量回灌流程。
- `pixwatch_dl/poller.py`：实现文件锁、磁盘阈值检测、健康状态写入与增量同步调度，并在成功下载后入队 Telegram 推送任务。
- `pixwatch_dl/main.py`：常驻守护入口，负责初始化并循环执行轮询任务。
- `pixwatch_dl/once.py`：单次任务入口，适合 systemd timer 调度。
- `pixwatch_dl/http_client.py`：简易 HTTP multipart 客户端，支持代理并供 Telegram 发送使用。
- `pixwatch_dl/telegram.py`：封装持久队列、Telegram API 客户端与后台 Sender Worker，确保发送重试、速率控制与幂等。
- `pixwatch_dl/tests/test_core.py`：pytest 单测覆盖命令构建、回灌判定、锁与健康检查逻辑。

## 测试

运行单元测试：

```bash
pytest
```

测试覆盖命令构建、统计解析、回灌判定、文件锁与磁盘限额等关键逻辑。
