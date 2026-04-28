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
<title>llm-api-gateway admin</title>
<style>
body { font-family: sans-serif; margin: 24px; }
code, pre { background: #f4f4f4; padding: 2px 4px; }
section { margin-bottom: 32px; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
tr:nth-child(even) { background: #fafafa; }
a { color: #0645ad; text-decoration: none; }
a:hover { text-decoration: underline; }
</style>
</head>
<body>
<h1>llm-api-gateway admin</h1>
<p>Use <code>?lineage=lin_...</code> to inspect one lineage.</p>
<section>
  <h2>Accounts</h2>
  <div id="accounts"></div>
</section>
<section>
  <h2>Recent routing failures</h2>
  <div id="events"></div>
</section>
<section>
  <h2>Lineage detail</h2>
  <div id="lineage"></div>
</section>
<script>
async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(path + ': ' + res.status);
  return await res.json();
}
function renderTable(rows) {
  if (!rows.length) return '<p>empty</p>';
  const cols = Object.keys(rows[0]);
  let html = '<table><thead><tr>' + cols.map(c => '<th>' + c + '</th>').join('') + '</tr></thead><tbody>';
  for (const row of rows) {
    html += '<tr>' + cols.map(c => '<td>' + formatCell(c, row[c]) + '</td>').join('') + '</tr>';
  }
  html += '</tbody></table>';
  return html;
}
function formatCell(key, value) {
  if (Array.isArray(value)) return '<pre>' + JSON.stringify(value, null, 2) + '</pre>';
  if (value && typeof value === 'object') return '<pre>' + JSON.stringify(value, null, 2) + '</pre>';
  if (key === 'lineage_session_id' && value) return '<a href="?lineage=' + encodeURIComponent(value) + '">' + value + '</a>';
  return value === null || value === undefined || value === '' ? '' : String(value);
}
async function main() {
  const accounts = await fetchJSON('/admin/api/accounts');
  document.getElementById('accounts').innerHTML = renderTable(accounts.accounts || []);
  const events = await fetchJSON('/admin/api/events?limit=20');
  document.getElementById('events').innerHTML = renderTable(events.failures || []);
  const params = new URLSearchParams(location.search);
  const lineage = params.get('lineage');
  if (!lineage) {
    document.getElementById('lineage').innerHTML = '<p>no lineage selected</p>';
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
}
main().catch(err => {
  document.body.insertAdjacentHTML('beforeend', '<pre>' + err.message + '</pre>');
});
</script>
</body>
</html>`

func (a *App) registerAdminRoutes() {
	a.mux.HandleFunc("/admin", a.handleAdminUI)
	a.mux.HandleFunc("/admin/", a.handleAdminUI)
	a.mux.HandleFunc("/admin/api/accounts", a.handleAdminAccounts)
	a.mux.HandleFunc("/admin/api/accounts/", a.handleAdminAccountOverview)
	a.mux.HandleFunc("/admin/api/lineages/", a.handleAdminLineage)
	a.mux.HandleFunc("/admin/api/events", a.handleAdminEvents)
}

func (a *App) handleAdminUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(adminHTML))
}

func (a *App) handleAdminAccounts(w http.ResponseWriter, r *http.Request) {
	accounts, err := a.sqlite.ListAccountOverviews(r.Context(), time.Now().UTC(), 24*time.Hour)
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
	overview, err := a.sqlite.GetAccountOverview(r.Context(), accountID, time.Now().UTC(), 24*time.Hour, 20)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "admin_query_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, overview)
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
