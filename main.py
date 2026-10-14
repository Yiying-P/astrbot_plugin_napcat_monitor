"""
NapCat (OneBot v11) 断线监控与自动重连插件 for AstrBot v2.0.0-beta

功能：
1. 定时检测 aiocqhttp 适配器（NapCat 反向 WebSocket）的连接状态；
2. 断线时记录日志、发送通知（可配置目标群/用户）；
3. 检测到适配器本身异常（状态 ERROR / 运行任务已结束）时，
   自动通过 PlatformManager 重启适配器，实现"自动重连"；
4. 提供 /napcat status、/napcat reconnect 指令方便人工查看与干预；
5. 【v2.0.0-beta】断连统计：累计断连/恢复/重启次数持久化到 data/plugins/ 下，
   并在 AstrBot WebUI 提供插件页面（views/dashboard）展示统计与事件历史。

原理说明：
- AstrBot 的 aiocqhttp 适配器以"反向 WebSocket 服务器"运行（默认 0.0.0.0:6199/ws），
  NapCat 作为 WebSocket 客户端主动连接上来。
- aiocqhttp 库在 CQHttp 对象上维护了两个连接集合：
    bot._wsr_api_clients  (dict,  X-Self-ID -> websocket)
    bot._wsr_event_clients (set,  事件通道 websocket)
  当 NapCat 断开时，对应连接会自动从集合中移除。
- 因此插件通过"连接集合是否为空 + 调用 get_status API 是否成功"双重判定在线状态。
- "自动重连"分两种情况：
    a. 适配器进程正常（WS 服务器还在监听）-> NapCat 客户端断开，等待 NapCat 侧自动重连，
       插件持续监控并等待恢复；
    b. 适配器进程异常（run task 结束 / 状态为 ERROR）-> 调用 PlatformManager.reload()
       重新加载适配器，让 WS 服务器重新启动。

插件页面（WebUI -> 插件详情 -> NapCat 断连监控）：
- 后端路由通过 context.register_web_api() 注册：
    /astrbot_plugin_napcat_monitor/stats        GET  返回断连统计
    /astrbot_plugin_napcat_monitor/stats/reset  POST 重置统计
- 前端 views/dashboard/index.html 通过 window.AstrBotPluginView 桥接调用。
"""

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from aiocqhttp.exceptions import ActionFailed, ApiNotAvailable, NetworkError

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star

# PlatformStatus 枚举: pending / running / error / stopped
from astrbot.core.platform.platform import PlatformStatus

# 插件 Web API 响应/请求（AstrBot 4.20+ 提供；旧版本自动降级兼容）
try:  # pragma: no cover - 由具体 AstrBot 版本决定分支
    from astrbot.api.web import error_response, json_response, request  # noqa: F401
except ImportError:  # AstrBot < 4.20 兜底
    from quart import jsonify, request  # type: ignore  # noqa: F401

    def json_response(data: Any = None, *, status_code: int = 200, **kw: Any):
        return jsonify({} if data is None else data), status_code

    def error_response(message: str, *, status_code: int = 400, **kw: Any):
        return jsonify({"status": "error", "message": message}), status_code


# 插件数据目录（data/plugins/astrbot_plugin_napcat_monitor/）
try:  # pragma: no cover
    from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path
except ImportError:  # pragma: no cover
    def get_astrbot_plugin_data_path() -> str:
        return str(Path("data") / "plugins")


PLUGIN_NAME = "astrbot_plugin_napcat_monitor"

# 单平台检测返回: (是否在线, 描述)
_CHECK_RESULT = tuple[bool, str]

_EVENT_LABELS = {
    "offline": "断线",
    "online": "恢复",
    "restart": "重启",
    "restart_give_up": "放弃重启",
}


class StatsStore:
    """断连统计持久化存储。

    数据保存在 <AstrBot data>/plugins/astrbot_plugin_napcat_monitor/stats.json，
    跨重启保留。字段：
      total_disconnects  累计断线次数
      total_reconnects   累计恢复次数
      restarts_total     累计适配器重启次数
      current_status     当前状态: unknown / online / offline
      last_disconnect    最近一次断线时间(字符串)
      last_disconnect_ts 最近一次断线时间戳
      last_reconnect     最近一次恢复时间(字符串)
      last_reconnect_ts  最近一次恢复时间戳
      last_restart       最近一次重启时间(字符串)
      first_seen         首次记录时间
      events             事件历史（按时间正序，最多 max_events 条）
    """

    def __init__(self, data_dir: str | Path, max_events: int = 100) -> None:
        self.path = Path(data_dir) / "stats.json"
        self.max_events = int(max_events) if int(max_events) > 0 else 100
        self.data: dict[str, Any] = {
            "total_disconnects": 0,
            "total_reconnects": 0,
            "restarts_total": 0,
            "current_status": "unknown",
            "last_disconnect": "",
            "last_disconnect_ts": 0,
            "last_reconnect": "",
            "last_reconnect_ts": 0,
            "last_restart": "",
            "first_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "events": [],
        }
        self._load()

    # ------------------------------------------------------------------
    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for key in (
                    "total_disconnects",
                    "total_reconnects",
                    "restarts_total",
                    "current_status",
                    "last_disconnect",
                    "last_disconnect_ts",
                    "last_reconnect",
                    "last_reconnect_ts",
                    "last_restart",
                    "first_seen",
                    "events",
                ):
                    if key in raw:
                        self.data[key] = raw[key]
        except Exception as e:  # noqa: BLE001
            logger.error(f"[NapCat监控] 读取统计文件失败(将重新初始化): {e}")

    def save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps(self.data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(self.path)
        except Exception as e:  # noqa: BLE001
            logger.error(f"[NapCat监控] 写入统计文件失败: {e}")

    # ------------------------------------------------------------------
    def record(self, event_type: str, desc: str) -> None:
        """记录一条事件并更新对应计数（断线/恢复/重启）。"""
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        ts = now.timestamp()

        if event_type == "offline":
            self.data["total_disconnects"] += 1
            self.data["current_status"] = "offline"
            self.data["last_disconnect"] = now_str
            self.data["last_disconnect_ts"] = ts
        elif event_type == "online":
            self.data["total_reconnects"] += 1
            self.data["current_status"] = "online"
            self.data["last_reconnect"] = now_str
            self.data["last_reconnect_ts"] = ts
        elif event_type == "restart":
            self.data["restarts_total"] += 1
            self.data["last_restart"] = now_str

        self.data["events"].append(
            {"time": now_str, "ts": ts, "type": event_type, "desc": desc}
        )
        events = self.data["events"]
        if len(events) > self.max_events:
            self.data["events"] = events[-self.max_events :]
        self.save()

    def set_status(self, status: str) -> None:
        self.data["current_status"] = status
        self.save()

    def reset(self) -> None:
        """清空统计（保留 first_seen 与当前状态）。"""
        status = self.data.get("current_status", "unknown")
        self.data = {
            "total_disconnects": 0,
            "total_reconnects": 0,
            "restarts_total": 0,
            "current_status": status,
            "last_disconnect": "",
            "last_disconnect_ts": 0,
            "last_reconnect": "",
            "last_reconnect_ts": 0,
            "last_restart": "",
            "first_seen": self.data.get("first_seen", ""),
            "events": [],
        }
        self.save()

    def to_dict(self) -> dict[str, Any]:
        return dict(self.data)


class NapCatMonitorPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None) -> None:
        super().__init__(context)
        self.config: dict[str, Any] = config if config is not None else {}

        self._stop = False
        self._monitor_task: asyncio.Task | None = None

        # ---- 运行状态 ----
        self._online: bool | None = None  # 上次检测结论（None=尚未检测）
        self._offline_since: float | None = None  # 断线起始时间戳
        self._last_desc: str = ""  # 最近一次检测的描述
        self._last_error: str = ""  # 最近一次 API 错误
        self._restart_count = 0  # 连续重启次数（恢复后清零）
        self._last_restart_ts = 0.0  # 上次重启时间戳

        # ---- 断连统计（v2.0.0-beta）----
        self._stats_enabled = bool(self.config.get("stats_enabled", True))
        self._stats = StatsStore(
            Path(get_astrbot_plugin_data_path()) / PLUGIN_NAME,
            max_events=int(self.config.get("stats_max_events", 100) or 100),
        )

        # ---- 插件页面 Web API（v2.0.0-beta）----
        if self._stats_enabled and getattr(context, "register_web_api", None):
            try:
                context.register_web_api(
                    f"/{PLUGIN_NAME}/stats",
                    self.api_stats,
                    ["GET"],
                    "NapCat 断连统计",
                )
                context.register_web_api(
                    f"/{PLUGIN_NAME}/stats/reset",
                    self.api_reset_stats,
                    ["POST"],
                    "重置断连统计",
                )
            except Exception as e:  # noqa: BLE001
                logger.error(f"[NapCat监控] 注册插件页面 API 失败: {e}")

        self._monitor_task = asyncio.create_task(self._monitor_loop())

    # ======================================================================
    # 插件页面 Web API
    # ======================================================================
    def _stats_payload(self) -> dict[str, Any]:
        payload = self._stats.to_dict()
        payload["monitor"] = {
            "online": self._online,
            "last_desc": self._last_desc,
            "restart_count": self._restart_count,
            "offline_since": (
                datetime.fromtimestamp(self._offline_since).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if self._offline_since
                else ""
            ),
        }
        return payload

    async def api_stats(self) -> Any:
        """GET /astrbot_plugin_napcat_monitor/stats -> 断连统计 JSON"""
        return json_response(self._stats_payload())

    async def api_reset_stats(self) -> Any:
        """POST /astrbot_plugin_napcat_monitor/stats/reset -> 重置统计"""
        self._stats.reset()
        logger.info(f"[NapCat监控] 统计已由 {request.username or '未知用户'} 重置")
        return json_response({"ok": True, "stats": self._stats.to_dict()})

    # ======================================================================
    # 平台实例获取
    # ======================================================================
    def _get_platforms(self) -> list[Any]:
        """获取当前加载的所有 aiocqhttp 平台适配器实例（支持同时配置多个）。"""
        ret: list[Any] = []
        try:
            insts = self.context.platform_manager.get_insts()
        except Exception as e:  # noqa: BLE001
            self.logger.error(f"获取平台实例列表失败: {e}")
            return ret
        for inst in insts:
            try:
                if inst.meta().name == "aiocqhttp":
                    ret.append(inst)
            except Exception:  # noqa: BLE001
                continue
        return ret

    # ======================================================================
    # 单平台状态检测
    # ======================================================================
    async def _check_platform(self, platform: Any) -> _CHECK_RESULT:
        """检测单个 aiocqhttp 平台实例的在线状态。

        判定规则：
        1. 反向 WS 连接集合为空 -> 离线（NapCat 客户端断开）；
        2. 有连接但调用 get_status 无响应 -> 异常（协议端卡死等）；
        3. 有连接且 API 正常响应（含 ActionFailed，说明通道已通）-> 在线。
        """
        try:
            bot = platform.get_client()
        except Exception as e:  # noqa: BLE001
            return False, f"无法获取客户端对象: {e}"

        api_clients = getattr(bot, "_wsr_api_clients", None)
        event_clients = getattr(bot, "_wsr_event_clients", None)
        n_api = len(api_clients) if isinstance(api_clients, dict) else 0
        n_event = len(event_clients) if isinstance(event_clients, set) else 0

        if n_api == 0 and n_event == 0:
            return False, "无活跃 WebSocket 连接"

        timeout = float(self.config.get("api_timeout", 10) or 10)
        # 单个 API 连接时补上 self_id，多个连接时交给库按唯一连接处理
        kwargs: dict[str, Any] = {}
        if isinstance(api_clients, dict) and len(api_clients) == 1:
            kwargs["self_id"] = next(iter(api_clients.keys()))

        try:
            await asyncio.wait_for(
                bot.call_action("get_status", **kwargs), timeout=timeout
            )
            return True, f"在线 (api={n_api}, event={n_event})"
        except (ApiNotAvailable, NetworkError, asyncio.TimeoutError) as e:
            self._last_error = f"{type(e).__name__}: {e}"
            return False, f"有连接但 API 无响应 ({self._last_error})"
        except ActionFailed:
            # 协议端已收到请求并正常响应（只是该 API 执行失败），通道是通的
            return True, f"在线 (api={n_api}, event={n_event}, get_status 执行失败但通道正常)"
        except Exception as e:  # noqa: BLE001
            self._last_error = f"{type(e).__name__}: {e}"
            return False, f"检测异常 ({self._last_error})"

    # ======================================================================
    # 监控主循环
    # ======================================================================
    async def _monitor_loop(self) -> None:
        # 等待平台适配器初始化完成（AstrBot 启动后平台加载需要时间）
        for _ in range(12):  # 最多等 60 秒
            if self._stop:
                return
            if self._get_platforms():
                break
            await asyncio.sleep(5)

        while not self._stop:
            try:
                await self._check_once()
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                self.logger.error(f"监控循环异常: {e}")
            interval = float(self.config.get("check_interval", 30) or 30)
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

    async def _check_once(self) -> None:
        """执行一轮检测，并根据状态变化触发对应动作。"""
        platforms = self._get_platforms()
        if not platforms:
            if self._online is True:
                # 平台实例消失（如适配器被停用）
                self._handle_state_change(False, "未找到 aiocqhttp 平台适配器实例")
            return

        all_online = True
        descs: list[str] = []
        for platform in platforms:
            ok, desc = await self._check_platform(platform)
            descs.append(f"{platform.meta().id}: {desc}")
            if not ok:
                all_online = False

        self._handle_state_change(all_online, "; ".join(descs))

    def _handle_state_change(self, online: bool, desc: str) -> None:
        """状态变化处理（在线 <-> 离线）。"""
        if online == self._online:
            # 状态未变化：离线时持续刷新描述，方便 status 指令查看
            if not online:
                self._last_desc = desc
            return

        self._online = online
        if online:
            self._on_recovered(desc)
        else:
            self._on_offline(desc)

    # ----------------------------------------------------------------------
    # 断线 / 恢复动作
    # ----------------------------------------------------------------------
    def _on_offline(self, desc: str) -> None:
        self._offline_since = time.time()
        self._last_desc = desc
        self._record_event("offline", desc)
        self.logger.warning(f"[NapCat监控] 检测到断线: {desc}")

        # 尽力通知（QQ 通道已断时发送会失败，仅记录日志）
        asyncio.create_task(
            self._notify(
                "[NapCat监控] 检测到断线\n"
                f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"详情: {desc}\n"
                f"累计断线次数: {self._stats.data['total_disconnects']}\n"
                "已进入自动重连流程，请留意恢复通知。"
            )
        )

        if self.config.get("enable_auto_reconnect", True):
            asyncio.create_task(self._try_reconnect())

    def _on_recovered(self, desc: str) -> None:
        duration = 0.0
        if self._offline_since:
            duration = time.time() - self._offline_since
        self._offline_since = None
        self._last_desc = desc
        self._restart_count = 0  # 恢复后重置重启计数
        self._record_event("online", desc)
        self.logger.info(
            f"[NapCat监控] 连接已恢复 (断线时长 {duration:.0f} 秒): {desc}"
        )
        asyncio.create_task(
            self._notify(
                "[NapCat监控] 连接已恢复\n"
                f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"断线时长: {duration:.0f} 秒\n"
                f"累计恢复次数: {self._stats.data['total_reconnects']}\n"
                f"详情: {desc}"
            )
        )

    # ======================================================================
    # 自动重连
    # ======================================================================
    async def _try_reconnect(self) -> None:
        """断线后的自动重连尝试。

        - 若适配器 run task 已结束或状态为 ERROR：重启适配器（受冷却与次数限制）；
        - 否则：适配器正常，等待 NapCat 客户端自动重连。
        """
        platforms = self._get_platforms()
        if not platforms:
            self.logger.warning("自动重连：未找到 aiocqhttp 平台实例，跳过")
            return

        pm = self.context.platform_manager
        for platform in platforms:
            try:
                need_restart = False
                reason = ""

                if getattr(platform, "status", None) == PlatformStatus.ERROR:
                    need_restart = True
                    reason = "平台状态为 ERROR"

                tasks = getattr(pm, "_platform_tasks", {})
                pt = tasks.get(platform.client_self_id)
                if pt is not None and getattr(pt.run, "done", lambda: False)():
                    need_restart = True
                    reason = "适配器运行任务已结束"

                if not need_restart:
                    self.logger.info(
                        "[NapCat监控] 适配器进程正常，等待 NapCat 客户端自动重连..."
                    )
                    continue

                now = time.time()
                cooldown = float(self.config.get("restart_cooldown", 300) or 300)
                if now - self._last_restart_ts < cooldown:
                    self.logger.info(
                        "[NapCat监控] 处于重启冷却期，跳过本次自动重启"
                    )
                    continue

                max_attempts = int(self.config.get("max_restart_attempts", 3) or 3)
                if self._restart_count >= max_attempts:
                    self.logger.error(
                        f"[NapCat监控] 连续重启已达上限({max_attempts})，"
                        "已停止自动重启，请人工检查 AstrBot / NapCat"
                    )
                    self._record_event(
                        "restart_give_up",
                        f"连续重启达到上限 {max_attempts} 次",
                    )
                    continue

                self._restart_count += 1
                self._last_restart_ts = now
                self.logger.warning(
                    f"[NapCat监控] 尝试重启 aiocqhttp 适配器 (第 {self._restart_count} 次, 原因: {reason})"
                )
                self._record_event(
                    "restart",
                    f"第 {self._restart_count} 次重启适配器，原因: {reason}",
                )

                cfg = dict(platform.config)
                await pm.reload(cfg)
                self.logger.info("[NapCat监控] 适配器重启完成")
            except Exception as e:  # noqa: BLE001
                self.logger.error(f"[NapCat监控] 自动重连过程出错: {e}")

    # ======================================================================
    # 通知
    # ======================================================================
    async def _notify(self, text: str) -> None:
        """向配置的目标群/用户发送通知。断线时通道不可用，失败仅记录日志。"""
        group_id = str(self.config.get("notify_group_id", "") or "").strip()
        user_id = str(self.config.get("notify_user_id", "") or "").strip()
        if not group_id and not user_id:
            return

        platforms = self._get_platforms()
        if not platforms:
            self.logger.warning("[NapCat监控] 通知失败：未找到 aiocqhttp 平台实例")
            return
        platform_id = platforms[0].meta().id
        chain = MessageChain().message(text)

        try:
            if group_id:
                umo = f"{platform_id}:GroupMessage:{group_id}"
                await self.context.send_message(umo, chain)
                self.logger.info(f"[NapCat监控] 已发送通知到群 {group_id}")
        except Exception as e:  # noqa: BLE001
            self.logger.warning(f"[NapCat监控] 发送群通知失败(通道可能已断): {e}")

        try:
            if user_id:
                umo = f"{platform_id}:FriendMessage:{user_id}"
                await self.context.send_message(umo, chain)
                self.logger.info(f"[NapCat监控] 已发送通知到用户 {user_id}")
        except Exception as e:  # noqa: BLE001
            self.logger.warning(f"[NapCat监控] 发送私聊通知失败(通道可能已断): {e}")

    # ======================================================================
    # 事件记录（统一写入持久化统计）
    # ======================================================================
    def _record_event(self, event_type: str, desc: str) -> None:
        if self._stats_enabled:
            self._stats.record(event_type, desc)
        else:
            # 统计关闭时仍保留内存事件供 status 指令使用
            if not hasattr(self, "_memory_history"):
                self._memory_history: list[dict[str, Any]] = []
            self._memory_history.append(
                {
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "type": event_type,
                    "desc": desc,
                }
            )
            if len(self._memory_history) > 50:
                self._memory_history = self._memory_history[-50:]

    def _event_list(self) -> list[dict[str, Any]]:
        if self._stats_enabled:
            return list(self._stats.data.get("events", []))
        return getattr(self, "_memory_history", [])

    def _fmt_history(self, limit: int = 8) -> list[str]:
        lines = []
        for h in self._event_list()[-limit:]:
            label = _EVENT_LABELS.get(h.get("type", ""), h.get("type", ""))
            lines.append(f"[{h.get('time', '')}] {label}: {h.get('desc', '')}")
        return lines

    # ======================================================================
    # 指令
    # ======================================================================
    @filter.command_group("napcat")
    def napcat(self) -> None:
        """NapCat 连接状态相关指令"""

    @napcat.command("status")
    async def napcat_status(self, event: AstrMessageEvent) -> Any:
        """查看 NapCat 连接状态 /napcat status"""
        platforms = self._get_platforms()
        lines: list[str] = []
        if not platforms:
            yield event.plain_result(
                "未找到 aiocqhttp 平台适配器实例。\n"
                "请确认：1) 已在 WebUI 启用 OneBot v11 (QQ 个人号等) 平台；"
                "2) NapCat 已按文档配置反向 WS 并成功连接。"
            )
            return

        lines.append("===== NapCat 监控状态 =====")
        for platform in platforms:
            ok, desc = await self._check_platform(platform)
            status_label = {
                PlatformStatus.PENDING: "待启动",
                PlatformStatus.RUNNING: "运行中",
                PlatformStatus.ERROR: "错误",
                PlatformStatus.STOPPED: "已停止",
            }.get(getattr(platform, "status", None), "未知")
            lines.append(
                f"适配器 [{platform.meta().id}] 进程状态: {status_label}\n"
                f"  连接状态: {desc}"
            )

        if self._online is None:
            lines.append("监控结论: 尚未完成首次检测")
        else:
            lines.append(f"监控结论: {'在线' if self._online else '离线'}")
            if not self._online and self._offline_since:
                lines.append(
                    "断线开始: "
                    + datetime.fromtimestamp(self._offline_since).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                )

        if self._last_desc:
            lines.append(f"最近检测: {self._last_desc}")
        lines.append(f"连续重启计数: {self._restart_count}")
        lines.append(f"检测间隔: {self.config.get('check_interval', 30)} 秒")
        lines.append(f"自动重连: {'开启' if self.config.get('enable_auto_reconnect', True) else '关闭'}")

        if self._stats_enabled:
            s = self._stats.data
            lines.append("----- 断连统计 (v2.0.0-beta) -----")
            lines.append(f"累计断线: {s['total_disconnects']} 次")
            lines.append(f"累计恢复: {s['total_reconnects']} 次")
            lines.append(f"累计重启: {s['restarts_total']} 次")
            if s["last_disconnect"]:
                lines.append(f"最近断线: {s['last_disconnect']}")
            if s["last_reconnect"]:
                lines.append(f"最近恢复: {s['last_reconnect']}")

        events = self._event_list()
        if events:
            lines.append("----- 最近事件 -----")
            lines.extend(self._fmt_history())
        else:
            lines.append("暂无事件记录")

        yield event.plain_result("\n".join(lines))

    @napcat.command("reconnect")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def napcat_reconnect(self, event: AstrMessageEvent) -> Any:
        """手动触发一次检测与重连 /napcat reconnect (仅管理员)"""
        yield event.plain_result("正在执行检测与重连流程，请稍候...")
        await self._check_once()
        # 若当前处于断线状态，则触发自动重连逻辑
        if self._online is False:
            await self._try_reconnect()
            await asyncio.sleep(2)
            await self._check_once()
            yield event.plain_result(
                f"重连流程已执行。当前状态: {'在线' if self._online else '仍离线'}\n"
                f"详情: {self._last_desc}"
            )
        else:
            yield event.plain_result(
                f"当前状态正常: {'在线' if self._online else '未知'}。\n"
                f"详情: {self._last_desc or '尚未检测'}"
            )

    # ======================================================================
    # 生命周期
    # ======================================================================
    async def terminate(self) -> None:
        """插件被卸载/停用时取消监控任务。"""
        self._stop = True
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
