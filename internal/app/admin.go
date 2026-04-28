package app

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const adminHTML = `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>llm-api-gateway admin</title>
<style>
:root {
  color-scheme: light;
  --bg: #f6f8fb;
  --panel: rgba(255,255,255,0.92);
  --panel-solid: #ffffff;
  --text: #111827;
  --muted: #6b7280;
  --line: #e5e7eb;
  --brand: #2563eb;
  --brand-weak: #dbeafe;
  --ok: #059669;
  --ok-weak: #d1fae5;
  --warn: #d97706;
  --warn-weak: #fef3c7;
  --bad: #dc2626;
  --bad-weak: #fee2e2;
  --shadow: 0 18px 45px rgba(15, 23, 42, 0.08);
  --radius: 18px;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  min-height: 100vh;
  color: var(--text);
  background:
    radial-gradient(circle at 10% 0%, rgba(37, 99, 235, 0.14), transparent 28rem),
    radial-gradient(circle at 85% 8%, rgba(5, 150, 105, 0.12), transparent 24rem),
    var(--bg);
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
a { color: var(--brand); text-decoration: none; }
a:hover { text-decoration: underline; }
code, pre {
  border: 1px solid var(--line);
  border-radius: 10px;
  background: #f9fafb;
}
code { padding: 2px 6px; }
pre {
  margin: 0;
  max-width: 42rem;
  overflow: auto;
  padding: 10px;
  color: #374151;
  font-size: 12px;
}
.shell { width: min(1440px, calc(100% - 40px)); margin: 0 auto; padding: 28px 0 48px; }
.hero {
  display: flex;
  gap: 20px;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 22px;
  padding: 22px;
  border: 1px solid rgba(255,255,255,0.78);
  border-radius: 26px;
  background: linear-gradient(135deg, rgba(255,255,255,0.96), rgba(255,255,255,0.76));
  box-shadow: var(--shadow);
  backdrop-filter: blur(14px);
}
.title-row { display: flex; gap: 14px; align-items: center; }
.app-icon {
  display: inline-flex;
  width: 52px;
  height: 52px;
  align-items: center;
  justify-content: center;
  border-radius: 18px;
  color: #ffffff;
  background: linear-gradient(135deg, #2563eb, #7c3aed);
  box-shadow: 0 14px 30px rgba(37, 99, 235, 0.24);
}
h1 { margin: 0; font-size: 28px; letter-spacing: -0.03em; }
.subtitle { margin: 6px 0 0; color: var(--muted); font-size: 14px; }
.toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; justify-content: flex-end; }
.button {
  display: inline-flex;
  gap: 8px;
  align-items: center;
  height: 38px;
  padding: 0 14px;
  border: 1px solid var(--line);
  border-radius: 12px;
  color: #374151;
  background: #ffffff;
  cursor: pointer;
  font: inherit;
}
.button.primary { border-color: var(--brand); color: #ffffff; background: var(--brand); }
.button:hover { filter: brightness(0.98); text-decoration: none; }
.icon { width: 18px; height: 18px; stroke: currentColor; fill: none; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
.grid { display: grid; gap: 16px; }
.stat-grid { grid-template-columns: repeat(6, minmax(0, 1fr)); margin-bottom: 16px; }
.card {
  border: 1px solid rgba(229,231,235,0.86);
  border-radius: var(--radius);
  background: var(--panel);
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
  overflow: hidden;
}
.card.pad { padding: 18px; }
.stat {
  min-height: 126px;
  padding: 16px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}
.stat-head { display: flex; justify-content: space-between; gap: 12px; color: var(--muted); font-size: 13px; }
.stat-icon {
  display: inline-flex;
  width: 34px;
  height: 34px;
  align-items: center;
  justify-content: center;
  border-radius: 12px;
  color: var(--brand);
  background: var(--brand-weak);
}
.stat-value { margin-top: 14px; font-size: 30px; font-weight: 760; letter-spacing: -0.04em; }
.stat-note { margin-top: 4px; color: var(--muted); font-size: 12px; }
.two-col { grid-template-columns: minmax(0, 1.3fr) minmax(360px, 0.7fr); align-items: start; }
.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 16px 18px;
  border-bottom: 1px solid var(--line);
}
.section-title { display: flex; align-items: center; gap: 10px; margin: 0; font-size: 16px; }
.section-title .stat-icon { width: 32px; height: 32px; }
.panel-body { padding: 16px 18px 18px; }
.account-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }
.account-card { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-solid); padding: 14px; }
.account-top { display: flex; justify-content: space-between; gap: 14px; align-items: start; }
.account-name { font-weight: 720; }
.account-host { margin-top: 4px; color: var(--muted); font-size: 12px; }
.mini-stats { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; margin-top: 14px; }
.mini { border-radius: 12px; background: #f9fafb; padding: 10px; }
.mini-label { color: var(--muted); font-size: 11px; }
.mini-value { margin-top: 4px; font-weight: 720; }
.badges { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 12px; }
.badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 24px;
  border-radius: 999px;
  padding: 3px 9px;
  color: #374151;
  background: #f3f4f6;
  font-size: 12px;
  white-space: nowrap;
}
.badge.ok { color: var(--ok); background: var(--ok-weak); }
.badge.warn { color: var(--warn); background: var(--warn-weak); }
.badge.bad { color: var(--bad); background: var(--bad-weak); }
.table-wrap { width: 100%; overflow: auto; }
table { border-collapse: separate; border-spacing: 0; width: 100%; min-width: 760px; font-size: 13px; }
th, td { padding: 11px 12px; text-align: left; border-bottom: 1px solid var(--line); vertical-align: top; }
th { position: sticky; top: 0; z-index: 1; color: var(--muted); background: #f9fafb; font-weight: 650; }
tr:hover td { background: #fcfcfd; }
.empty { color: var(--muted); padding: 20px; border: 1px dashed var(--line); border-radius: 14px; background: #fbfdff; }
.error { margin-top: 16px; border-color: var(--bad-weak); color: var(--bad); background: #fff7f7; }
.meta { color: var(--muted); font-size: 12px; }
.stack { display: grid; gap: 16px; }
@media (max-width: 1180px) {
  .stat-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .two-col { grid-template-columns: 1fr; }
}
@media (max-width: 760px) {
  .shell { width: min(100% - 20px, 1440px); padding-top: 12px; }
  .hero { align-items: flex-start; flex-direction: column; }
  .toolbar { justify-content: flex-start; }
  .stat-grid, .account-grid { grid-template-columns: 1fr; }
  .mini-stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
</style>
</head>
<body>
<div class="shell">
  <header class="hero">
    <div class="title-row">
      <div class="app-icon" data-icon="gateway"></div>
      <div>
        <h1>llm-api-gateway admin</h1>
        <p class="subtitle">Routing authority, replay fallback, account load, and recent failures.</p>
      </div>
    </div>
    <div class="toolbar">
      <span class="badge" id="captured">loading</span>
      <button class="button" onclick="main()"><span data-icon="refresh"></span>Refresh</button>
      <a class="button primary" href="/metrics"><span data-icon="chart"></span>Prometheus</a>
    </div>
  </header>

  <section class="grid stat-grid" id="stats"></section>

  <div class="grid two-col">
    <main class="stack">
      <section class="card">
        <div class="section-head">
          <h2 class="section-title"><span class="stat-icon" data-icon="server"></span>Accounts</h2>
          <span class="meta">active window: <span id="active-window">-</span></span>
        </div>
        <div class="panel-body" id="accounts"></div>
      </section>

      <section class="card">
        <div class="section-head">
          <h2 class="section-title"><span class="stat-icon" data-icon="timeline"></span>Lineage detail</h2>
          <span class="meta">use <code>?lineage=lineage_...</code></span>
        </div>
        <div class="panel-body" id="lineage"></div>
      </section>
    </main>

    <aside class="stack">
      <section class="card">
        <div class="section-head">
          <h2 class="section-title"><span class="stat-icon" data-icon="alert"></span>Recent routing failures</h2>
          <span class="meta">latest 20</span>
        </div>
        <div class="panel-body" id="events"></div>
      </section>

      <section class="card">
        <div class="section-head">
          <h2 class="section-title"><span class="stat-icon" data-icon="activity"></span>Raw monitoring</h2>
        </div>
        <div class="panel-body" id="monitoring"></div>
      </section>
    </aside>
  </div>
</div>
<script>
const icons = {
  gateway: '<svg class="icon" viewBox="0 0 24 24"><path d="M4 7h16M4 17h16M7 4l-3 3 3 3M17 14l3 3-3 3M9 12h6"/></svg>',
  refresh: '<svg class="icon" viewBox="0 0 24 24"><path d="M21 12a9 9 0 0 1-15 6.7L3 16M3 21v-5h5M3 12a9 9 0 0 1 15-6.7L21 8M21 3v5h-5"/></svg>',
  chart: '<svg class="icon" viewBox="0 0 24 24"><path d="M4 19V5M4 19h16M8 16v-5M12 16V8M16 16v-8"/></svg>',
  server: '<svg class="icon" viewBox="0 0 24 24"><rect x="4" y="5" width="16" height="6" rx="2"/><rect x="4" y="13" width="16" height="6" rx="2"/><path d="M8 8h.01M8 16h.01M12 8h4M12 16h4"/></svg>',
  timeline: '<svg class="icon" viewBox="0 0 24 24"><path d="M6 4v16M18 4v16M6 8h8l4 4-4 4H6"/></svg>',
  alert: '<svg class="icon" viewBox="0 0 24 24"><path d="M12 9v4M12 17h.01M10.3 4.7 2.8 18a2 2 0 0 0 1.7 3h15a2 2 0 0 0 1.7-3L13.7 4.7a2 2 0 0 0-3.4 0Z"/></svg>',
  activity: '<svg class="icon" viewBox="0 0 24 24"><path d="M3 12h4l3 8 4-16 3 8h4"/></svg>',
  users: '<svg class="icon" viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8ZM22 21v-2a4 4 0 0 0-3-3.9M16 3.1a4 4 0 0 1 0 7.8"/></svg>',
  link: '<svg class="icon" viewBox="0 0 24 24"><path d="M10 13a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-1.7 1.7M14 11a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l1.7-1.7"/></svg>',
  replay: '<svg class="icon" viewBox="0 0 24 24"><path d="M17 1v6h-6M7 23v-6h6M20.5 11A8.5 8.5 0 0 0 6 5.1L3.5 7.5M3.5 13A8.5 8.5 0 0 0 18 18.9l2.5-2.4"/></svg>',
  shield: '<svg class="icon" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10Z"/></svg>'
};
function mountIcons(root) {
  for (const el of root.querySelectorAll('[data-icon]')) el.innerHTML = icons[el.dataset.icon] || '';
}
function escapeHTML(value) {
  return String(value).replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
}
async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(path + ': ' + res.status);
  return await res.json();
}
function fmt(n) { return Number(n || 0).toLocaleString(); }
function fmtDuration(ms) {
  const minutes = Math.round(Number(ms || 0) / 60000);
  if (minutes < 60) return minutes + 'm';
  const hours = Math.round(minutes / 60);
  return hours + 'h';
}
function renderTable(rows) {
  if (!rows || !rows.length) return '<div class="empty">empty</div>';
  const cols = Object.keys(rows[0]);
  let html = '<div class="table-wrap"><table><thead><tr>' + cols.map(c => '<th>' + escapeHTML(c) + '</th>').join('') + '</tr></thead><tbody>';
  for (const row of rows) html += '<tr>' + cols.map(c => '<td>' + formatCell(c, row[c]) + '</td>').join('') + '</tr>';
  return html + '</tbody></table></div>';
}
function formatCell(key, value) {
  if (Array.isArray(value)) return renderCounts(value);
  if (value && typeof value === 'object') return '<pre>' + escapeHTML(JSON.stringify(value, null, 2)) + '</pre>';
  if (key === 'lineage_session_id' && value) return '<a href="?lineage=' + encodeURIComponent(value) + '">' + escapeHTML(value) + '</a>';
  if (key === 'route_mode' && value) return '<span class="badge ' + (value === 'strict' ? 'ok' : 'warn') + '">' + escapeHTML(value) + '</span>';
  if (key === 'reason_code' && value) return '<span class="badge bad">' + escapeHTML(value) + '</span>';
  return value === null || value === undefined || value === '' ? '' : escapeHTML(value);
}
function renderCounts(xs) {
  if (!xs || !xs.length) return '';
  return '<div class="badges">' + xs.map(x => '<span class="badge">' + escapeHTML(x.label || '') + ' ' + fmt(x.count) + '</span>').join('') + '</div>';
}
function statCard(label, value, note, icon, tone) {
  const cls = tone ? ' ' + tone : '';
  return '<article class="card stat"><div class="stat-head"><span>' + escapeHTML(label) + '</span><span class="stat-icon' + cls + '" data-icon="' + icon + '"></span></div><div><div class="stat-value">' + fmt(value) + '</div><div class="stat-note">' + escapeHTML(note || '') + '</div></div></article>';
}
function renderStats(snapshot) {
  const g = snapshot.global || {};
  return [
    statCard('Enabled accounts', g.enabled_accounts, 'routing pool', 'server'),
    statCard('Active sessions', g.active_sessions, 'last active window', 'users'),
    statCard('Active carriers', g.active_carriers, 'strict authority', 'link'),
    statCard('Recent replays', g.recent_replays, 'last 24h', 'replay'),
    statCard('Recent turns', g.recent_turns, 'last 24h', 'activity'),
    statCard('Failures', g.recent_routing_failures, 'last 24h', 'alert')
  ].join('');
}
function renderAccounts(accounts) {
  if (!accounts || !accounts.length) return '<div class="empty">empty</div>';
  return '<div class="account-grid">' + accounts.map(a => {
    const failures = Number(a.recent_failures || 0);
    const status = failures > 0 ? '<span class="badge bad">failures ' + fmt(failures) + '</span>' : '<span class="badge ok">no failures</span>';
    const recent = (a.recent_turns || []).slice(0, 4).map(t => '<a class="badge" href="?lineage=' + encodeURIComponent(t.lineage_session_id) + '">' + escapeHTML(t.route_mode) + ':' + escapeHTML(t.lineage_session_id) + '</a>').join('');
    return '<article class="account-card"><div class="account-top"><div><div class="account-name">' + escapeHTML(a.account_id) + '</div><div class="account-host">' + escapeHTML(a.downstream_host) + ':' + escapeHTML(a.downstream_port) + '</div></div>' + status + '</div>' +
      '<div class="mini-stats"><div class="mini"><div class="mini-label">sessions</div><div class="mini-value">' + fmt(a.active_sessions) + '</div></div><div class="mini"><div class="mini-label">carriers</div><div class="mini-value">' + fmt(a.active_carriers) + '</div></div><div class="mini"><div class="mini-label">replays</div><div class="mini-value">' + fmt(a.recent_replays) + '</div></div><div class="mini"><div class="mini-label">turns</div><div class="mini-value">' + fmt(a.recent_turns) + '</div></div></div>' +
      '<div class="badges">' + renderBadgeGroup('route', a.route_modes) + renderBadgeGroup('carrier', a.carrier_kinds) + renderBadgeGroup('replay', a.replay_reasons) + recent + '</div></article>';
  }).join('') + '</div>';
}
function mergeAccounts(metricsAccounts, overviewAccounts) {
  const byID = new Map((overviewAccounts || []).map(a => [a.account_id, a]));
  return (metricsAccounts || []).map(a => Object.assign({}, byID.get(a.account_id) || {}, a));
}
function renderBadgeGroup(prefix, xs) {
  if (!xs || !xs.length) return '';
  return xs.map(x => '<span class="badge">' + escapeHTML(prefix) + ':' + escapeHTML(x.label) + ' ' + fmt(x.count) + '</span>').join('');
}
function renderMonitoring(snapshot) {
  const global = snapshot.global || {};
  const rows = [{
    enabled_accounts: global.enabled_accounts,
    retained_lineages: global.retained_lineages,
    active_lineages: global.active_lineages,
    inactive_lineages: global.inactive_lineages,
    active_sessions: global.active_sessions,
    active_carriers: global.active_carriers,
    recent_replays: global.recent_replays,
    recent_turns: global.recent_turns,
    recent_routing_failures: global.recent_routing_failures
  }];
  return renderTable(rows);
}
async function main() {
  const monitoring = await fetchJSON('/admin/api/metrics');
  document.getElementById('captured').textContent = 'captured ' + new Date(Number(monitoring.captured_at_ms || 0)).toLocaleString();
  document.getElementById('active-window').textContent = fmtDuration(monitoring.active_session_window_ms);
  document.getElementById('stats').innerHTML = renderStats(monitoring);
  document.getElementById('monitoring').innerHTML = renderMonitoring(monitoring);
  const accounts = await fetchJSON('/admin/api/accounts');
  document.getElementById('accounts').innerHTML = renderAccounts(mergeAccounts(monitoring.accounts || [], accounts.accounts || []));
  const events = await fetchJSON('/admin/api/events?limit=20');
  document.getElementById('events').innerHTML = renderTable(events.failures || []);
  const params = new URLSearchParams(location.search);
  const lineage = params.get('lineage');
  if (!lineage) {
    document.getElementById('lineage').innerHTML = '<div class="empty">no lineage selected</div>';
    mountIcons(document);
    return;
  }
  const detail = await fetchJSON('/admin/api/lineages/' + encodeURIComponent(lineage));
  let html = '';
  if (detail.binding) html += '<h3>Binding</h3>' + renderTable([detail.binding]);
  html += '<h3>Carriers</h3>' + renderTable(detail.carriers || []);
  html += '<h3>Turns</h3>' + renderTable(detail.turns || []);
  html += '<h3>Replay events</h3>' + renderTable(detail.replay_events || []);
  html += '<h3>Failures</h3>' + renderTable(detail.failures || []);
  document.getElementById('lineage').innerHTML = html;
  mountIcons(document);
}
mountIcons(document);
main().catch(err => {
  document.body.insertAdjacentHTML('beforeend', '<div class="shell"><pre class="error">' + escapeHTML(err.message) + '</pre></div>');
});
</script>
</body>
</html>`

func (a *App) registerAdminRoutes() {
	a.mux.HandleFunc("/admin", a.withAccess(a.handleAdminUI))
	a.mux.HandleFunc("/admin/", a.withAccess(a.handleAdminUI))
	a.mux.HandleFunc("/admin/api/accounts", a.withAccess(a.handleAdminAccounts))
	a.mux.HandleFunc("/admin/api/accounts/", a.withAccess(a.handleAdminAccountOverview))
	a.mux.HandleFunc("/admin/api/metrics", a.withAccess(a.handleAdminMetrics))
	a.mux.HandleFunc("/admin/api/lineages/", a.withAccess(a.handleAdminLineage))
	a.mux.HandleFunc("/admin/api/events", a.withAccess(a.handleAdminEvents))
}

func (a *App) handleAdminUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(adminHTML))
}

func (a *App) handleAdminAccounts(w http.ResponseWriter, r *http.Request) {
	accounts, err := a.sqlite.ListAccountOverviews(r.Context(), time.Now().UTC(), metricsLookback)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "admin_query_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"accounts": accounts})
}

func (a *App) handleAdminAccountOverview(w http.ResponseWriter, r *http.Request) {
	accountID := strings.TrimPrefix(r.URL.Path, "/admin/api/accounts/")
	if accountID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_account_id", "missing account id")
		return
	}
	overview, err := a.sqlite.GetAccountOverview(r.Context(), accountID, time.Now().UTC(), metricsLookback, 20)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "admin_query_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, overview)
}

func (a *App) handleAdminMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot, err := a.sqlite.MonitoringSnapshot(r.Context(), time.Now().UTC(), metricsLookback)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "admin_query_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (a *App) handleAdminLineage(w http.ResponseWriter, r *http.Request) {
	lineageID := strings.TrimPrefix(r.URL.Path, "/admin/api/lineages/")
	if lineageID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_lineage_id", "missing lineage id")
		return
	}
	detail, err := a.sqlite.GetLineageDetail(r.Context(), lineageID, 100)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "admin_query_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (a *App) handleAdminEvents(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 && parsed <= 500 {
			limit = parsed
		}
	}
	failures, err := a.sqlite.ListRoutingFailures(r.Context(), "", r.URL.Query().Get("reason_code"), limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "admin_query_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"failures": failures})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
