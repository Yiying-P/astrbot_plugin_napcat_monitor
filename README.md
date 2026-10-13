# AstrBot NapCat 断线监控 + 自动重连插件

针对 AstrBot 的 **aiocqhttp（OneBot v11 / NapCat）** 平台适配器开发：定时监控反向 WebSocket 连接状态，断线时记录日志、发送通知，并自动尝试重连。

## 功能

- **断线监控**：按固定间隔（默认 30 秒）检测 NapCat 反向 WS 连接是否存活，双重判定（连接集合 + `get_status` API 探测）。
- **自动重连**：
  - 适配器进程异常（状态 ERROR / 运行任务结束）时，自动通过 `PlatformManager.reload()` 重启适配器；
  - 适配器正常但 NapCat 客户端断开时，等待 NapCat 侧自动重连，插件持续监控并在恢复后上报。
- **通知**：可配置断线 / 恢复时向指定 QQ 群或用户发送通知（断线瞬间通道不可用时，发送失败仅记录日志，恢复后会补发恢复通知）。
- **人工干预指令**：`/napcat status` 查看状态与最近事件；`/napcat reconnect` 手动触发检测与重连（仅管理员）。
- **防循环保护**：连续重启次数上限、重启冷却时间，防止异常死循环。

## 工作原理（简要）

AstrBot 的 aiocqhttp 适配器以**反向 WebSocket 服务器**方式运行（默认监听 `0.0.0.0:6199/ws`），NapCat 作为 WebSocket 客户端主动连接上来。`aiocqhttp` 库在 `CQHttp` 对象上维护连接集合 `_wsr_api_clients` / `_wsr_event_clients`，NapCat 断开时连接会自动从集合中移除。插件据此判断在线状态：

1. 连接集合为空 → 判定断线；
2. 有连接但 `get_status` 调用超时/无响应 → 判定异常；
3. 有连接且 API 正常响应 → 判定在线。

## 安装

### 方式一：WebUI 上传 ZIP（推荐）

1. 下载本插件打包好的 `astrbot_plugin_napcat_monitor.zip`；
2. 打开 AstrBot WebUI → **插件** → 右上角 **安装插件** → 选择 **上传 ZIP 安装**；
3. 上传后插件出现在插件列表，点击启用。

### 方式二：手动放置目录

1. 将整个 `astrbot_plugin_napcat_monitor` 文件夹复制到 AstrBot 的 `data/plugins/` 目录下：
   ```
   AstrBot/
   └── data/
       └── plugins/
           └── astrbot_plugin_napcat_monitor/   <- 插件根目录
               ├── metadata.yaml
               ├── main.py
               ├── _conf_schema.json
               └── README.md
   ```
2. 重启 AstrBot，或在 WebUI **插件** 页面点击 **重载插件**。

> 插件要求 AstrBot >= v4.16，且已启用 OneBot v11 (QQ 个人号等) 平台适配器并成功连接 NapCat（NapCat 配置反向 WS 地址 `ws://<AstrBot主机>:6199/ws`）。

## 配置

在 WebUI **插件** 页点击本插件的**配置**按钮修改：

| 配置项 | 说明 | 默认值 |
| --- | --- | --- |
| `check_interval` | 检测间隔（秒） | 30 |
| `api_timeout` | `get_status` 调用超时（秒） | 10 |
| `enable_auto_reconnect` | 是否自动重连 | true |
| `max_restart_attempts` | 连续重启适配器最大次数 | 3 |
| `restart_cooldown` | 两次重启最小间隔（秒） | 300 |
| `notify_group_id` | 通知群号（留空不通知） | 空 |
| `notify_user_id` | 通知 QQ 号（留空不通知） | 空 |

## 指令

| 指令 | 权限 | 说明 |
| --- | --- | --- |
| `/napcat status` | 所有人 | 查看各适配器进程状态、连接状态、断线时间、连续重启计数与最近事件 |
| `/napcat reconnect` | 管理员 | 手动执行一次检测与重连流程 |

## 注意事项

- 反向 WS 模式下，**NapCat 侧**通常自带自动重连（NapCat WebUI 网络配置中可调整重连间隔）。本插件负责监控、告警，以及在 AstrBot 适配器异常时重启适配器。
- 断线瞬间 QQ 通道不可用，此时发送通知会失败（插件会记录日志）；连接恢复后会补发"连接已恢复"通知。
- 若连续重启达到上限，插件会停止自动重启并记录日志，需要人工检查 AstrBot / NapCat 日志。
- 事件记录保存在插件进程内存中（最多 50 条），重启 AstrBot 后清空；完整历史请查看 AstrBot WebUI 日志。
