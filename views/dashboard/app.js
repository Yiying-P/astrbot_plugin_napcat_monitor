/* NapCat 断连统计插件页面逻辑（v2.0.0-beta） */
const bridge = window.AstrBotPluginView;

const TYPE_LABELS = {
  offline: "断线",
  online: "恢复",
  restart: "重启",
  restart_give_up: "放弃重启",
};
const TYPE_CLASS = {
  offline: "offline",
  online: "online",
  restart: "restart",
  restart_give_up: "give-up",
};

const $ = (id) => document.getElementById(id);
const fmt = (s) => s || "–";

let refreshTimer = null;

async function loadStats() {
  const data = await bridge.apiGet("stats");
  render(data);
}

function render(data) {
  // 徽章：在线/离线/未知
  const badge = $("status-badge");
  const online = data.monitor && data.monitor.online;
  badge.textContent =
    online === true ? "在线" : online === false ? "离线" : "未知";
  badge.className = "badge " + (online === true ? "ok" : online === false ? "off" : "unknown");

  $("total-disconnects").textContent = fmt(data.total_disconnects);
  $("total-reconnects").textContent = fmt(data.total_reconnects);
  $("restarts-total").textContent = fmt(data.restarts_total);
  $("last-disconnect").textContent = fmt(data.last_disconnect);
  $("last-reconnect").textContent = fmt(data.last_reconnect);

  const now = new Date().toLocaleTimeString();
  $("updated-at").textContent = "更新于 " + now;

  // 实时状态
  const m = data.monitor || {};
  const lines = [
    `监控结论: ${online === true ? "在线" : online === false ? "离线" : "未知"}`,
    `最近检测: ${fmt(m.last_desc)}`,
    `连续重启计数: ${fmt(m.restart_count)}`,
    `断线开始: ${fmt(m.offline_since)}`,
    `首次记录: ${fmt(data.first_seen)}`,
  ];
  $("monitor-box").textContent = lines.join("\n");

  // 事件历史
  const events = data.events || [];
  $("event-count").textContent = events.length ? `最近 ${events.length} 条` : "";
  const body = $("event-body");
  if (!events.length) {
    body.innerHTML = '<tr><td colspan="3" class="muted">暂无事件</td></tr>';
    return;
  }
  body.innerHTML = events
    .slice()
    .reverse()
    .map((e) => {
      const label = TYPE_LABELS[e.type] || e.type;
      const cls = TYPE_CLASS[e.type] || "";
      return (
        `<tr>` +
        `<td class="muted">${escapeHtml(e.time || "")}</td>` +
        `<td><span class="tag ${cls}">${escapeHtml(label)}</span></td>` +
        `<td>${escapeHtml(e.desc || "")}</td>` +
        `</tr>`
      );
    })
    .join("");
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

async function resetStats() {
  const ok = window.confirm("确定要清空全部断连统计吗？此操作不可撤销。");
  if (!ok) return;
  await bridge.apiPost("stats/reset");
  await loadStats();
}

async function main() {
  // 等待 WebUI 注入上下文（含插件名/页面名/主题）
  await bridge.ready();

  $("btn-refresh").addEventListener("click", loadStats);
  $("btn-reset").addEventListener("click", resetStats);

  await loadStats();
  refreshTimer = setInterval(loadStats, 10000);
  window.addEventListener("beforeunload", () => {
    if (refreshTimer) clearInterval(refreshTimer);
  });
}

main().catch((err) => {
  const box = $("monitor-box");
  if (box) box.textContent = "加载失败: " + (err && err.message ? err.message : err);
});
