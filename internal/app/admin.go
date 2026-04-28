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
  --card: #ffffff;
  --card-soft: rgba(255,255,255,0.78);
  --text: #111827;
  --muted: #6b7280;
  --line: #e5e7eb;
  --blue: #2563eb;
  --blue-soft: #dbeafe;
  --green: #059669;
  --green-soft: #d1fae5;
  --amber: #d97706;
  --amber-soft: #fef3c7;
  --red: #dc2626;
  --red-soft: #fee2e2;
  --shadow: 0 18px 42px rgba(15, 23, 42, 0.08);
}
* { box-sizing: border-box; }
body {
  margin: 0;
  min-height: 100vh;
  color: var(--text);
  background: radial-gradient(circle at 8% 0%, rgba(37,99,235,.14), transparent 28rem), radial-gradient(circle at 88% 8%, rgba(5,150,105,.11), transparent 24rem), var(--bg);
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
a { color: var(--blue); text-decoration: none; }
a:hover { text-decoration: underline; }
code, pre { border: 1px solid var(--line); border-radius: 10px; background: #f9fafb; }
code { padding: 2px 6px; }
pre { margin: 0; max-width: 44rem; overflow: auto; padding: 10px; color: #374151; font-size: 12px; }
.shell { width: min(1440px, calc(100% - 40px)); margin: 0 auto; padding: 28px 0 48px; }
.hero {
  display: flex; justify-content: space-between; align-items: center; gap: 20px;
  margin-bottom: 18px; padding: 22px; border: 1px solid rgba(255,255,255,.8); border-radius: 26px;
  background: linear-gradient(135deg, rgba(255,255,255,.96), rgba(255,255,255,.76)); box-shadow: var(--shadow); backdrop-filter: blur(14px);
}
.title-row { display: flex; align-items: center; gap: 14px; }
.app-icon, .section-icon, .stat-icon { display: inline-flex; align-items: center; justify-content: center; }
.app-icon { width: 52px; height: 52px; border-radius: 18px; color: white; background: linear-gradient(135deg, #2563eb, #7c3aed); box-shadow: 0 14px 30px rgba(37,99,235,.24); }
.section-icon, .stat-icon { width: 34px; height: 34px; border-radius: 12px; color: var(--blue); background: var(--blue-soft); }
.icon { width: 18px; height: 18px; stroke: currentColor; fill: none; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
h1 { margin: 0; font-size: 28px; letter-spacing: -0.03em; }
h2 { margin: 0; font-size: 16px; }
h3 { margin: 16px 0 10px; font-size: 14px; }
.subtitle, .meta, .muted { color: var(--muted); }
.subtitle { margin: 6px 0 0; font-size: 14px; }
.toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; justify-content: flex-end; }
.button { display: inline-flex; align-items: center; gap: 8px; height: 38px; padding: 0 14px; border: 1px solid var(--line); border-radius: 12px; color: #374151; background: white; cursor: pointer; font: inherit; }
.button.primary { border-color: var(--blue); color: white; background: var(--blue); }
.grid { display: grid; gap: 16px; }
.stat-grid { grid-template-columns: repeat(6, minmax(0, 1fr)); margin-bottom: 16px; }
.layout { grid-template-columns: minmax(0, 1.35fr) minmax(360px, .65fr); align-items: start; }
.stack { display: grid; gap: 16px; }
.card { border: 1px solid rgba(229,231,235,.9); border-radius: 18px; background: var(--card-soft); box-shadow: 0 10px 30px rgba(15,23,42,.05); overflow: hidden; }
.stat { min-height: 124px; padding: 16px; background: var(--card); display: flex; flex-direction: column; justify-content: space-between; }
.stat-head { display: flex; justify-content: space-between; gap: 10px; color: var(--muted); font-size: 13px; }
.stat-value { margin-top: 14px; font-size: 30px; font-weight: 760; letter-spacing: -0.04em; }
.stat-note { margin-top: 4px; color: var(--muted); font-size: 12px; }
.section-head { display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 16px 18px; border-bottom: 1px solid var(--line); }
.section-title { display: flex; align-items: center; gap: 10px; }
.panel-body { padding: 16px 18px 18px; }
.load-card { margin-bottom: 16px; }
.load-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
.load-row { display: grid; grid-template-columns: minmax(130px, .7fr) minmax(180px, 1.3fr) auto; gap: 12px; align-items: center; padding: 12px; border: 1px solid var(--line); border-radius: 14px; background: white; }
.load-name { font-weight: 720; }
.load-sub { margin-top: 3px; color: var(--muted); font-size: 12px; }
.load-bars { display: grid; gap: 7px; }
.load-bar { display: grid; grid-template-columns: 70px 1fr 42px; gap: 8px; align-items: center; color: var(--muted); font-size: 12px; }
.load-track { position: relative; height: 18px; border-radius: 999px; background: #eef2ff; overflow: hidden; }
.load-fill { height: 100%; border-radius: inherit; background: linear-gradient(90deg, #2563eb, #7c3aed); }
.load-fill.replay { background: linear-gradient(90deg, #059669, #10b981); }
.load-percent { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: #1f2937; font-size: 11px; font-weight: 760; text-shadow: 0 1px 0 rgba(255,255,255,.72); }
.load-score { min-width: 52px; text-align: right; font-weight: 760; }
.account-grid { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 14px; }
.account-card { border: 1px solid var(--line); border-radius: 16px; background: white; padding: 14px; }
.account-top { display: flex; justify-content: space-between; gap: 12px; align-items: start; }
.account-name { font-weight: 760; }
.account-host { margin-top: 4px; color: var(--muted); font-size: 12px; }
.mini-stats { display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 8px; margin-top: 14px; }
.mini { border-radius: 12px; background: #f9fafb; padding: 10px; }
.mini-label { color: var(--muted); font-size: 11px; }
.mini-value { margin-top: 4px; font-weight: 720; }
.badges { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 12px; }
.badge { display: inline-flex; align-items: center; gap: 6px; min-height: 24px; border-radius: 999px; padding: 3px 9px; color: #374151; background: #f3f4f6; font-size: 12px; white-space: nowrap; }
.badge.ok { color: var(--green); background: var(--green-soft); }
.badge.warn { color: var(--amber); background: var(--amber-soft); }
.badge.bad { color: var(--red); background: var(--red-soft); }
.table-wrap { width: 100%; overflow: auto; }
table { border-collapse: separate; border-spacing: 0; width: 100%; min-width: 740px; font-size: 13px; }
th, td { padding: 11px 12px; text-align: left; border-bottom: 1px solid var(--line); vertical-align: top; }
th { position: sticky; top: 0; z-index: 1; color: var(--muted); background: #f9fafb; font-weight: 650; }
tr:hover td { background: #fcfcfd; }
.empty, .error { padding: 18px; border-radius: 14px; }
.empty { color: var(--muted); border: 1px dashed var(--line); background: #fbfdff; }
.error { margin-top: 16px; color: var(--red); border: 1px solid var(--red-soft); background: #fff7f7; }
@media (max-width: 1180px) { .stat-grid { grid-template-columns: repeat(3, minmax(0,1fr)); } .layout, .load-grid { grid-template-columns: 1fr; } }
@media (max-width: 760px) { .shell { width: min(100% - 20px, 1440px); padding-top: 12px; } .hero { align-items: flex-start; flex-direction: column; } .toolbar { justify-content: flex-start; } .stat-grid, .account-grid { grid-template-columns: 1fr; } .mini-stats { grid-template-columns: repeat(2, minmax(0,1fr)); } .load-row { grid-template-columns: 1fr; } .load-score { text-align: left; } }
</style>
</head>
<body>
<div class="shell">
  <header class="hero">
    <div class="title-row">
      <div class="app-icon" data-icon="gateway"></div>
      <div>
        <h1>llm-api-gateway admin</h1>
        <p class="subtitle">Strict routing, replay fallback, account load, and failures.</p>
      </div>
    </div>
    <div class="toolbar">
      <span class="badge" id="captured">loading</span>
      <button class="button" id="refresh"><span data-icon="refresh"></span>Refresh</button>
      <a class="button primary" href="/metrics"><span data-icon="chart"></span>Prometheus</a>
    </div>
  </header>

  <section class="grid stat-grid" id="stats"></section>

  <section class="card load-card">
    <div class="section-head">
      <div class="section-title"><span class="section-icon" data-icon="activity"></span><h2>Account load</h2></div>
      <span class="meta">score = sessions x10 + carriers x3 + turns + replays x2 + failures x5</span>
    </div>
    <div class="panel-body" id="load"></div>
  </section>

  <div class="grid layout">
    <main class="stack">
      <section class="card">
        <div class="section-head">
          <div class="section-title"><span class="section-icon" data-icon="server"></span><h2>Accounts</h2></div>
          <span class="meta">active window: <span id="active-window">-</span></span>
        </div>
        <div class="panel-body" id="accounts"></div>
      </section>

      <section class="card">
        <div class="section-head">
          <div class="section-title"><span class="section-icon" data-icon="timeline"></span><h2>Lineage detail</h2></div>
          <span class="meta">query: <code>?lineage=lineage_...</code></span>
        </div>
        <div class="panel-body" id="lineage"></div>
      </section>
    </main>

    <aside class="stack">
      <section class="card">
        <div class="section-head">
          <div class="section-title"><span class="section-icon" data-icon="alert"></span><h2>Recent routing failures</h2></div>
          <span class="meta">latest 20</span>
        </div>
        <div class="panel-body" id="events"></div>
      </section>

      <section class="card">
        <div class="section-head">
          <div class="section-title"><span class="section-icon" data-icon="activity"></span><h2>Raw monitoring</h2></div>
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
  replay: '<svg class="icon" viewBox="0 0 24 24"><path d="M17 1v6h-6M7 23v-6h6M20.5 11A8.5 8.5 0 0 0 6 5.1L3.5 7.5M3.5 13A8.5 8.5 0 0 0 18 18.9l2.5-2.4"/></svg>'
};
const qs = s => document.querySelector(s);
const byId = id => document.getElementById(id);
const arr = x => Array.isArray(x) ? x : [];
const num = x => Number.isFinite(Number(x)) ? Number(x) : 0;
const str = x => x === null || x === undefined ? '' : String(x);
function escapeHTML(value) {
  return str(value).replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
}
function mountIcons(root = document) {
  root.querySelectorAll('[data-icon]').forEach(el => { el.innerHTML = icons[el.dataset.icon] || ''; });
}
async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(path + ': ' + res.status);
  return res.json();
}
function fmt(n) { return num(n).toLocaleString(); }
function fmtDuration(ms) {
  const minutes = Math.round(num(ms) / 60000);
  return minutes < 60 ? minutes + 'm' : Math.round(minutes / 60) + 'h';
}
function badge(text, tone) {
  return '<span class="badge ' + escapeHTML(tone || '') + '">' + escapeHTML(text) + '</span>';
}
function renderLabelCounts(xs, prefix) {
  return arr(xs).map(x => badge((prefix ? prefix + ':' : '') + str(x.label) + ' ' + fmt(x.count))).join('');
}
function formatCell(key, value) {
  if (Array.isArray(value)) return '<div class="badges">' + renderLabelCounts(value, '') + '</div>';
  if (value && typeof value === 'object') return '<pre>' + escapeHTML(JSON.stringify(value, null, 2)) + '</pre>';
  if (key === 'lineage_session_id' && value) return '<a href="?lineage=' + encodeURIComponent(value) + '">' + escapeHTML(value) + '</a>';
  if (key === 'route_mode' && value) return badge(value, value === 'strict' ? 'ok' : 'warn');
  if (key === 'reason_code' && value) return badge(value, 'bad');
  return value === null || value === undefined || value === '' ? '' : escapeHTML(value);
}
function renderTable(rows) {
  rows = arr(rows);
  if (!rows.length) return '<div class="empty">empty</div>';
  const cols = Object.keys(rows[0]);
  return '<div class="table-wrap"><table><thead><tr>' + cols.map(c => '<th>' + escapeHTML(c) + '</th>').join('') + '</tr></thead><tbody>' +
    rows.map(row => '<tr>' + cols.map(c => '<td>' + formatCell(c, row[c]) + '</td>').join('') + '</tr>').join('') +
    '</tbody></table></div>';
}
function statCard(label, value, note, icon) {
  return '<article class="card stat"><div class="stat-head"><span>' + escapeHTML(label) + '</span><span class="stat-icon" data-icon="' + escapeHTML(icon) + '"></span></div><div><div class="stat-value">' + fmt(value) + '</div><div class="stat-note">' + escapeHTML(note) + '</div></div></article>';
}
function normalizeAccounts(metricsAccounts, overviewAccounts) {
  const byID = new Map();
  arr(overviewAccounts).forEach(o => byID.set(o.account_id, { overview: o, metrics: null }));
  arr(metricsAccounts).forEach(m => {
    const row = byID.get(m.account_id) || { overview: null, metrics: null };
    row.metrics = m;
    byID.set(m.account_id, row);
  });
  return Array.from(byID.values()).map(({overview, metrics}) => {
    overview = overview || {};
    metrics = metrics || {};
    return {
      account_id: str(metrics.account_id || overview.account_id),
      display_name: str(metrics.display_name || overview.display_name || metrics.account_id || overview.account_id),
      downstream_host: str(metrics.downstream_host || overview.downstream_host),
      downstream_port: num(metrics.downstream_port || overview.downstream_port),
      active_sessions: num(metrics.active_sessions ?? overview.active_session_count),
      active_carriers: num(metrics.active_carriers ?? overview.active_carrier_count),
      recent_replay_count: num(metrics.recent_replays ?? overview.recent_replay_count),
      recent_turn_count: num(metrics.recent_turns),
      recent_failure_count: num(metrics.recent_failures),
      route_modes: arr(metrics.route_modes),
      carrier_kinds: arr(metrics.carrier_kinds),
      replay_reasons: arr(metrics.replay_reasons),
      failure_reasons: arr(metrics.failure_reasons),
      recent_turn_items: arr(overview.recent_turns)
    };
  }).sort((a, b) => a.downstream_port - b.downstream_port || a.account_id.localeCompare(b.account_id));
}
function renderStats(snapshot) {
  const g = snapshot.global || {};
  byId('stats').innerHTML = [
    statCard('Enabled accounts', g.enabled_accounts, 'routing pool', 'server'),
    statCard('Active sessions', g.active_sessions, 'last active window', 'users'),
    statCard('Active carriers', g.active_carriers, 'strict authority', 'link'),
    statCard('Recent replays', g.recent_replays, 'last 24h', 'replay'),
    statCard('Recent turns', g.recent_turns, 'last 24h', 'activity'),
    statCard('Failures', g.recent_routing_failures, 'last 24h', 'alert')
  ].join('');
}
function loadScore(a) {
  return a.active_sessions * 10 + a.active_carriers * 3 + a.recent_turn_count + a.recent_replay_count * 2 + a.recent_failure_count * 5;
}
function pct(value, max) {
  if (max <= 0) return 0;
  return Math.max(0, Math.min(100, Math.round(value * 100 / max)));
}
function loadTrack(value, max, cls) {
  const p = pct(value, max);
  return '<div class="load-track"><div class="load-fill ' + escapeHTML(cls || '') + '" style="width:' + p + '%"></div><div class="load-percent">' + p + '%</div></div>';
}
function renderLoad(accounts) {
  if (!accounts.length) return '<div class="empty">empty</div>';
  const maxScore = Math.max(1, ...accounts.map(loadScore));
  const maxTurns = Math.max(1, ...accounts.map(a => a.recent_turn_count));
  const maxReplays = Math.max(1, ...accounts.map(a => a.recent_replay_count));
  const sorted = accounts.slice().sort((a, b) => loadScore(b) - loadScore(a) || a.downstream_port - b.downstream_port);
  return '<div class="load-grid">' + sorted.map(a => {
    const score = loadScore(a);
    return '<div class="load-row"><div><div class="load-name">' + escapeHTML(a.display_name) + '</div><div class="load-sub">' + escapeHTML(a.account_id) + ' / :' + escapeHTML(a.downstream_port) + '</div></div>' +
      '<div class="load-bars">' +
        '<div class="load-bar"><span>score</span>' + loadTrack(score, maxScore, '') + '<span>' + fmt(score) + '</span></div>' +
        '<div class="load-bar"><span>turns</span>' + loadTrack(a.recent_turn_count, maxTurns, '') + '<span>' + fmt(a.recent_turn_count) + '</span></div>' +
        '<div class="load-bar"><span>replay</span>' + loadTrack(a.recent_replay_count, maxReplays, 'replay') + '<span>' + fmt(a.recent_replay_count) + '</span></div>' +
      '</div><div class="load-score">' + badge('sessions ' + fmt(a.active_sessions), a.active_sessions > 0 ? 'warn' : '') + '</div></div>';
  }).join('') + '</div>';
}
function renderAccounts(accounts) {
  if (!accounts.length) return '<div class="empty">empty</div>';
  return '<div class="account-grid">' + accounts.map(a => {
    const status = a.recent_failure_count > 0 ? badge('failures ' + fmt(a.recent_failure_count), 'bad') : badge('no failures', 'ok');
    const recentTurns = a.recent_turn_items.slice(0, 4).map(t => '<a class="badge" href="?lineage=' + encodeURIComponent(t.lineage_session_id) + '">' + escapeHTML(t.route_mode) + ':' + escapeHTML(t.lineage_session_id) + '</a>').join('');
    const badges = renderLabelCounts(a.route_modes, 'route') + renderLabelCounts(a.carrier_kinds, 'carrier') + renderLabelCounts(a.replay_reasons, 'replay') + renderLabelCounts(a.failure_reasons, 'failure') + recentTurns;
    return '<article class="account-card"><div class="account-top"><div><div class="account-name">' + escapeHTML(a.display_name) + '</div><div class="account-host">' + escapeHTML(a.account_id) + ' -> ' + escapeHTML(a.downstream_host) + ':' + escapeHTML(a.downstream_port) + '</div></div>' + status + '</div>' +
      '<div class="mini-stats"><div class="mini"><div class="mini-label">sessions</div><div class="mini-value">' + fmt(a.active_sessions) + '</div></div><div class="mini"><div class="mini-label">carriers</div><div class="mini-value">' + fmt(a.active_carriers) + '</div></div><div class="mini"><div class="mini-label">replays</div><div class="mini-value">' + fmt(a.recent_replay_count) + '</div></div><div class="mini"><div class="mini-label">turns</div><div class="mini-value">' + fmt(a.recent_turn_count) + '</div></div></div>' +
      '<div class="badges">' + badges + '</div></article>';
  }).join('') + '</div>';
}
function renderMonitoring(snapshot) {
  const g = snapshot.global || {};
  byId('monitoring').innerHTML = renderTable([{
    enabled_accounts: g.enabled_accounts,
    retained_lineages: g.retained_lineages,
    active_lineages: g.active_lineages,
    inactive_lineages: g.inactive_lineages,
    active_sessions: g.active_sessions,
    active_carriers: g.active_carriers,
    recent_replays: g.recent_replays,
    recent_turns: g.recent_turns,
    recent_routing_failures: g.recent_routing_failures
  }]);
}
async function renderLineage() {
  const lineage = new URLSearchParams(location.search).get('lineage');
  if (!lineage) {
    byId('lineage').innerHTML = '<div class="empty">no lineage selected</div>';
    return;
  }
  const detail = await fetchJSON('/admin/api/lineages/' + encodeURIComponent(lineage));
  byId('lineage').innerHTML = (detail.binding ? '<h3>Binding</h3>' + renderTable([detail.binding]) : '') +
    '<h3>Carriers</h3>' + renderTable(detail.carriers) +
    '<h3>Turns</h3>' + renderTable(detail.turns) +
    '<h3>Replay events</h3>' + renderTable(detail.replay_events) +
    '<h3>Failures</h3>' + renderTable(detail.failures);
}
async function main() {
  try {
    const [monitoring, accountPayload, eventPayload] = await Promise.all([
      fetchJSON('/admin/api/metrics'),
      fetchJSON('/admin/api/accounts'),
      fetchJSON('/admin/api/events?limit=20')
    ]);
    byId('captured').textContent = 'captured ' + new Date(num(monitoring.captured_at_ms)).toLocaleString();
    byId('active-window').textContent = fmtDuration(monitoring.active_session_window_ms);
    const accounts = normalizeAccounts(monitoring.accounts, accountPayload.accounts);
    renderStats(monitoring);
    renderMonitoring(monitoring);
    byId('load').innerHTML = renderLoad(accounts);
    byId('accounts').innerHTML = renderAccounts(accounts);
    byId('events').innerHTML = renderTable(eventPayload.failures);
    await renderLineage();
    mountIcons();
  } catch (err) {
    byId('accounts').innerHTML = '<pre class="error">' + escapeHTML(err && err.message ? err.message : err) + '</pre>';
  }
}
byId('refresh').addEventListener('click', main);
mountIcons();
main();
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
