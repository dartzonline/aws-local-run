"""Web dashboard for LocalRun — inline HTML/JS/CSS."""

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LocalRun Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,monospace;background:#0f172a;color:#e2e8f0;min-height:100vh}
a{color:#38bdf8;text-decoration:none}

/* Header */
#header{background:#1e293b;border-bottom:2px solid #0ea5e9;padding:0 24px;display:flex;align-items:center;gap:0;position:sticky;top:0;z-index:100}
#logo{font-size:1.15rem;font-weight:700;color:#38bdf8;letter-spacing:-.5px;padding:14px 24px 14px 0;border-right:1px solid #334155;margin-right:8px;white-space:nowrap}
#logo span{color:#94a3b8;font-weight:400}
#status-dot{width:9px;height:9px;border-radius:50%;background:#22c55e;display:inline-block;margin-right:6px;box-shadow:0 0 6px #22c55e}
#status-dot.red{background:#ef4444;box-shadow:0 0 6px #ef4444}
#status-text{font-size:.78rem;color:#94a3b8;margin-right:auto}
#nav{display:flex;height:100%}
#nav button{background:transparent;border:none;color:#94a3b8;padding:16px 18px;cursor:pointer;font-size:.875rem;font-family:inherit;border-bottom:3px solid transparent;transition:color .15s,border-color .15s}
#nav button:hover{color:#e2e8f0}
#nav button.active{color:#38bdf8;border-bottom-color:#38bdf8}
#refresh-btn{background:#1e3a5f;border:1px solid #0ea5e9;color:#38bdf8;padding:6px 14px;border-radius:5px;cursor:pointer;font-size:.8rem;font-family:inherit;margin-left:16px}
#refresh-btn:hover{background:#0c4a6e}
#auto-refresh{display:flex;align-items:center;gap:6px;font-size:.78rem;color:#64748b;margin-left:10px}
#auto-refresh input{accent-color:#38bdf8}

/* Tabs */
.tab{display:none;padding:24px}
.tab.active{display:block;animation:fadein .15s}
@keyframes fadein{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}

/* Overview */
#stats-row{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap}
.stat-card{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:14px 20px;min-width:140px;flex:1}
.stat-card .label{font-size:.73rem;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.stat-card .value{font-size:1.6rem;font-weight:700;color:#38bdf8}
.stat-card .sub{font-size:.75rem;color:#94a3b8;margin-top:2px}

.svc-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px}
.svc-card{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:14px 16px;cursor:pointer;transition:border-color .15s}
.svc-card:hover{border-color:#0ea5e9}
.svc-card.expanded{border-color:#38bdf8}
.svc-card .svc-name{font-size:.85rem;font-weight:600;color:#e2e8f0;display:flex;align-items:center;gap:8px;margin-bottom:8px}
.svc-card .svc-name .icon{font-size:1rem}
.svc-card .svc-stat{font-size:.78rem;color:#94a3b8;margin-bottom:2px}
.svc-card .svc-stat strong{color:#38bdf8}
.svc-detail{display:none;margin-top:10px;border-top:1px solid #334155;padding-top:10px}
.svc-card.expanded .svc-detail{display:block}
.item-list{list-style:none;max-height:140px;overflow-y:auto}
.item-list li{font-size:.76rem;color:#94a3b8;padding:3px 0;border-bottom:1px solid #1e293b;display:flex;justify-content:space-between}
.item-list li span{color:#38bdf8}
.badge{display:inline-block;background:#0c4a6e;color:#38bdf8;border-radius:3px;font-size:.68rem;padding:1px 5px}

/* Table */
.table-wrap{overflow-x:auto;border-radius:8px;border:1px solid #334155}
table{width:100%;border-collapse:collapse;font-size:.83rem}
th{text-align:left;padding:10px 14px;background:#1e293b;color:#64748b;font-weight:500;font-size:.75rem;text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid #334155;white-space:nowrap}
td{padding:9px 14px;border-bottom:1px solid #1a2540;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#1e293b55}
.ok{color:#22c55e}.err{color:#ef4444}.warn{color:#f59e0b}

/* Toolbar */
.toolbar{display:flex;gap:8px;align-items:center;margin-bottom:14px;flex-wrap:wrap}
input,select{background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:7px 10px;border-radius:5px;font-family:inherit;font-size:.83rem;outline:none}
input:focus,select:focus{border-color:#0ea5e9}
.btn{background:#0ea5e9;border:none;color:#0f172a;padding:7px 14px;border-radius:5px;cursor:pointer;font-weight:600;font-size:.83rem;font-family:inherit}
.btn:hover{background:#38bdf8}
.btn-ghost{background:#1e293b;border:1px solid #334155;color:#94a3b8;padding:7px 14px;border-radius:5px;cursor:pointer;font-size:.83rem;font-family:inherit}
.btn-ghost:hover{border-color:#0ea5e9;color:#e2e8f0}
.btn-danger{background:#7f1d1d;border:1px solid #ef4444;color:#ef4444;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:.77rem;font-family:inherit}
.btn-danger:hover{background:#ef4444;color:#fff}

/* Pill tag */
.tag{display:inline-block;padding:2px 7px;border-radius:10px;font-size:.72rem;font-weight:600}
.tag-green{background:#14532d;color:#4ade80}
.tag-red{background:#450a0a;color:#f87171}
.tag-blue{background:#0c2a4d;color:#60a5fa}
.tag-gray{background:#1e293b;color:#94a3b8}

/* Misc */
.section-title{font-size:.95rem;font-weight:600;color:#e2e8f0;margin-bottom:14px}
.empty{color:#64748b;font-size:.85rem;padding:24px;text-align:center}
.code{background:#0f172a;border:1px solid #334155;border-radius:4px;padding:10px;font-size:.77rem;white-space:pre-wrap;overflow-x:auto;max-height:200px;font-family:monospace}
.loading{color:#64748b;font-size:.85rem;padding:32px;text-align:center}
.pulse{animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

/* Fault form */
.fault-form{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px;margin-bottom:16px}
.fault-form .row{display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end}
.fault-form label{display:block;font-size:.73rem;color:#64748b;margin-bottom:4px}
.fault-form .field{display:flex;flex-direction:column}

/* Config */
.config-section{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px;margin-bottom:12px}
.config-section h3{font-size:.83rem;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px}
.config-kv{display:flex;gap:0;margin-bottom:6px;align-items:baseline}
.config-k{color:#64748b;font-size:.83rem;min-width:180px}
.config-v{color:#38bdf8;font-size:.83rem;font-family:monospace}

/* Scrollbar */
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:#0f172a}
::-webkit-scrollbar-thumb{background:#334155;border-radius:3px}
</style>
</head>
<body>

<div id="header">
  <div id="logo">LocalRun <span>Dashboard</span></div>
  <div id="nav">
    <button id="tab-overview" class="active">Overview</button>
    <button id="tab-resources">Resources</button>
    <button id="tab-requests">Requests</button>
    <button id="tab-faults">Faults</button>
    <button id="tab-config">Config</button>
  </div>
  <div style="display:flex;align-items:center;margin-left:auto;padding:0 0 0 16px;gap:4px">
    <span id="status-dot" class="red"></span>
    <span id="status-text">Connecting...</span>
    <button id="refresh-btn">&#8635; Refresh</button>
    <label id="auto-refresh"><input type="checkbox" id="auto-check" checked> Auto</label>
  </div>
</div>

<!-- OVERVIEW -->
<div id="pane-overview" class="tab active">
  <div id="stats-row"></div>
  <div class="section-title">Services</div>
  <div id="svc-grid" class="svc-grid"><div class="loading pulse">Loading services...</div></div>
</div>

<!-- RESOURCES -->
<div id="pane-resources" class="tab">
  <div class="toolbar">
    <input id="res-search" placeholder="&#128269; Filter by name or service..." style="width:280px">
    <select id="res-svc-filter"><option value="">All services</option></select>
    <button id="res-refresh" class="btn-ghost">&#8635; Refresh</button>
    <span id="res-count" style="color:#64748b;font-size:.8rem;margin-left:auto"></span>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Service</th><th>Type</th><th>Name / ID</th><th>Info</th></tr></thead>
      <tbody id="res-body"><tr><td colspan="4" class="loading pulse">Loading...</td></tr></tbody>
    </table>
  </div>
</div>

<!-- REQUESTS -->
<div id="pane-requests" class="tab">
  <div class="toolbar">
    <select id="req-svc"><option value="">All services</option></select>
    <select id="req-status"><option value="">All statuses</option><option value="ok">2xx / 3xx</option><option value="err">4xx / 5xx</option></select>
    <input id="req-search" placeholder="Filter action..." style="width:200px">
    <button id="req-refresh" class="btn-ghost">&#8635; Refresh</button>
    <span id="req-count" style="color:#64748b;font-size:.8rem;margin-left:auto"></span>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Time</th><th>Service</th><th>Action</th><th>Method</th><th>Status</th><th>Duration</th></tr></thead>
      <tbody id="req-body"><tr><td colspan="6" class="loading pulse">Loading...</td></tr></tbody>
    </table>
  </div>
</div>

<!-- FAULTS -->
<div id="pane-faults" class="tab">
  <div class="fault-form">
    <div class="section-title" style="margin-bottom:12px">Inject Fault</div>
    <div class="row">
      <div class="field"><label>Service</label><input id="f-svc" placeholder="s3, sqs, dynamodb..." style="width:140px"></div>
      <div class="field"><label>Action</label><input id="f-action" placeholder="GetObject, Send..." style="width:170px"></div>
      <div class="field"><label>Type</label><select id="f-type"><option value="error">Error</option><option value="latency">Latency</option></select></div>
      <div class="field"><label>Status Code</label><input id="f-status" value="500" style="width:90px"></div>
      <div class="field"><label>Probability</label><input id="f-prob" value="1.0" style="width:80px"></div>
      <div class="field"><label>Delay ms</label><input id="f-delay" value="2000" style="width:90px"></div>
      <div class="field"><label>&nbsp;</label><button id="add-fault-btn" class="btn">+ Add Fault</button></div>
    </div>
  </div>
  <div class="section-title">Active Faults</div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>ID</th><th>Service</th><th>Action</th><th>Type</th><th>Probability</th><th>Details</th><th></th></tr></thead>
      <tbody id="faults-body"><tr><td colspan="7" class="loading pulse">Loading...</td></tr></tbody>
    </table>
  </div>
</div>

<!-- CONFIG -->
<div id="pane-config" class="tab">
  <div id="config-body"><div class="loading pulse">Loading...</div></div>
  <div style="margin-top:16px;display:flex;gap:8px">
    <button id="reset-btn" class="btn-ghost" style="border-color:#ef4444;color:#ef4444">&#9888; Reset All State</button>
  </div>
</div>

<script>
(function() {
  'use strict';

  // Auto-detect base URL so fetches always hit the right server
  var BASE = window.location.protocol + '//' + window.location.host;

  var allResources = [];
  var allRequests = [];
  var autoTimer = null;

  // ── Tab routing ──────────────────────────────────────────────────────────────
  var tabs = {
    overview: { btn: 'tab-overview', pane: 'pane-overview', load: loadOverview },
    resources: { btn: 'tab-resources', pane: 'pane-resources', load: loadResources },
    requests: { btn: 'tab-requests', pane: 'pane-requests', load: loadRequests },
    faults: { btn: 'tab-faults', pane: 'pane-faults', load: loadFaults },
    config: { btn: 'tab-config', pane: 'pane-config', load: loadConfig }
  };
  var currentTab = 'overview';

  function showTab(name) {
    currentTab = name;
    Object.keys(tabs).forEach(function(k) {
      var t = tabs[k];
      document.getElementById(t.pane).classList.toggle('active', k === name);
      document.getElementById(t.btn).classList.toggle('active', k === name);
    });
    tabs[name].load();
  }

  Object.keys(tabs).forEach(function(k) {
    document.getElementById(tabs[k].btn).addEventListener('click', function() { showTab(k); });
  });

  document.getElementById('refresh-btn').addEventListener('click', function() { tabs[currentTab].load(); });

  // ── Auto-refresh ─────────────────────────────────────────────────────────────
  function startAuto() {
    clearInterval(autoTimer);
    autoTimer = setInterval(function() {
      if (document.getElementById('auto-check').checked) {
        tabs[currentTab].load();
      }
    }, 4000);
  }
  startAuto();

  // ── Health polling ───────────────────────────────────────────────────────────
  function checkHealth() {
    get('/health').then(function(d) {
      document.getElementById('status-dot').className = 'green';
      document.getElementById('status-text').textContent = 'v' + (d.version || '?') + ' · running';
    }).catch(function() {
      document.getElementById('status-dot').className = 'red';
      document.getElementById('status-text').textContent = 'Server unreachable';
    });
  }
  checkHealth();
  setInterval(checkHealth, 8000);

  // ── HTTP helpers ─────────────────────────────────────────────────────────────
  function get(path) {
    return fetch(BASE + path).then(function(r) { return r.json(); });
  }
  function post(path, body) {
    return fetch(BASE + path, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    }).then(function(r) { return r.json(); });
  }
  function del(path) {
    return fetch(BASE + path, { method: 'DELETE' });
  }
  function esc(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  // ── SERVICE ICONS ────────────────────────────────────────────────────────────
  var icons = {
    s3:'🪣', sqs:'📨', dynamodb:'🗄', sns:'📣', lambda:'λ', iam:'🔑',
    logs:'📋', cloudwatch:'📊', sts:'🎟', secretsmanager:'🔐', ssm:'⚙',
    events:'⚡', cloudformation:'🏗', rds:'💾', apigateway:'🔀',
    opensearch:'🔍', kinesis:'🌊', stepfunctions:'🔗', kms:'🗝',
    ec2:'💻', acm:'📜', route53:'🌐', ses:'✉', cognito:'👤'
  };

  // ── OVERVIEW ─────────────────────────────────────────────────────────────────
  var svcDetailData = {};

  function loadOverview() {
    Promise.all([
      get('/health'),
      get('/_localrun/api/state'),
      get('/_localrun/requests?limit=200')
    ]).then(function(results) {
      var health = results[0];
      var state = results[1];
      var reqData = results[2];

      // Stats row
      var svcs = health.services || {};
      var totalRes = 0;
      Object.keys(state).forEach(function(k) { totalRes += (state[k] || []).length; });
      var reqs = reqData.requests || [];
      var errCount = reqs.filter(function(r) { return r.status >= 400; }).length;
      var recentMs = reqs.slice(-20).map(function(r) { return r.duration_ms || 0; });
      var avgMs = recentMs.length ? Math.round(recentMs.reduce(function(a,b){return a+b;},0)/recentMs.length) : 0;

      document.getElementById('stats-row').innerHTML =
        stat('Total Services', Object.keys(svcs).length, 'active') +
        stat('Resources', totalRes, 'created') +
        stat('Total Requests', reqs.length, 'served') +
        stat('Errors', errCount, errCount > 0 ? '⚠ check faults tab' : 'clean') +
        stat('Avg Latency', avgMs + 'ms', 'last 20 requests');

      // Service cards
      var html = '';
      Object.keys(svcs).sort().forEach(function(name) {
        var info = svcs[name];
        var stateItems = state[name] || state[name + ' logs'] || state['secrets manager'] || [];
        // find matching state key
        var stateKey = name;
        if (name === 'logs') stateKey = 'cloudwatch logs';
        if (name === 'secretsmanager') stateKey = 'secrets manager';
        var items = state[stateKey] || [];

        var stats = '';
        if (typeof info === 'object') {
          Object.keys(info).forEach(function(k) {
            stats += '<div class="svc-stat"><strong>' + info[k] + '</strong> ' + k + '</div>';
          });
        } else {
          stats = '<div class="svc-stat"><span class="tag tag-green">' + esc(info) + '</span></div>';
        }

        var detailHtml = '';
        if (items.length) {
          detailHtml = '<ul class="item-list">' + items.map(function(item) {
            var label = item.name || item.function_name || item.id || '?';
            var detail = '';
            if (item.messages !== undefined) detail = item.messages + ' msgs';
            else if (item.objects !== undefined) detail = item.objects + ' objects';
            else if (item.items !== undefined) detail = item.items + ' items';
            else if (item.subscriptions !== undefined) detail = item.subscriptions + ' subs';
            else if (item.streams !== undefined) detail = item.streams + ' streams';
            else if (item.runtime) detail = item.runtime;
            return '<li>' + esc(label) + (detail ? '<span>' + esc(detail) + '</span>' : '') + '</li>';
          }).join('') + '</ul>';
        } else {
          detailHtml = '<div style="color:#475569;font-size:.75rem">No resources yet</div>';
        }

        html += '<div class="svc-card" data-svc="' + esc(name) + '">' +
          '<div class="svc-name"><span class="icon">' + (icons[name]||'☁') + '</span>' + esc(name) + '</div>' +
          stats +
          '<div class="svc-detail">' + detailHtml + '</div>' +
          '</div>';
      });

      document.getElementById('svc-grid').innerHTML = html || '<div class="empty">No services.</div>';

      document.querySelectorAll('.svc-card').forEach(function(card) {
        card.addEventListener('click', function() { card.classList.toggle('expanded'); });
      });

    }).catch(function(e) {
      document.getElementById('svc-grid').innerHTML = '<div class="empty">⚠ Cannot reach server at ' + BASE + '</div>';
    });
  }

  function stat(label, value, sub) {
    return '<div class="stat-card"><div class="label">' + esc(label) + '</div><div class="value">' + esc(value) + '</div><div class="sub">' + esc(sub) + '</div></div>';
  }

  // ── RESOURCES ────────────────────────────────────────────────────────────────
  var resFilter = document.getElementById('res-search');
  var resSvcFilter = document.getElementById('res-svc-filter');

  function loadResources() {
    get('/_localrun/resources').then(function(data) {
      allResources = data.resources || [];
      // populate service filter
      var svcs = {};
      allResources.forEach(function(r) { if (r.service) svcs[r.service] = 1; });
      var opts = '<option value="">All services</option>';
      Object.keys(svcs).sort().forEach(function(s) { opts += '<option>' + esc(s) + '</option>'; });
      resSvcFilter.innerHTML = opts;
      renderResources();
    }).catch(function() {
      document.getElementById('res-body').innerHTML = '<tr><td colspan="4" class="empty">Error loading resources</td></tr>';
    });
  }

  function renderResources() {
    var q = resFilter.value.toLowerCase();
    var svc = resSvcFilter.value;
    var list = allResources.filter(function(r) {
      var match = !q || (r.name||'').toLowerCase().indexOf(q) >= 0 || (r.service||'').toLowerCase().indexOf(q) >= 0 || (r.type||'').toLowerCase().indexOf(q) >= 0;
      var svcMatch = !svc || r.service === svc;
      return match && svcMatch;
    });
    document.getElementById('res-count').textContent = list.length + ' / ' + allResources.length + ' resources';
    if (!list.length) {
      document.getElementById('res-body').innerHTML = '<tr><td colspan="4" class="empty">No resources found</td></tr>';
      return;
    }
    var html = '';
    list.forEach(function(r, i) {
      var info = '';
      if (r.messages !== undefined) info += '<span class="badge">' + r.messages + ' msgs</span> ';
      if (r.objects !== undefined) info += '<span class="badge">' + r.objects + ' objs</span> ';
      if (r.items !== undefined) info += '<span class="badge">' + r.items + ' items</span> ';
      if (r.runtime) info += '<span class="badge">' + esc(r.runtime) + '</span> ';
      html += '<tr onclick="toggleRes(this)" style="cursor:pointer">' +
        '<td>' + (icons[r.service]||'') + ' ' + esc(r.service||'') + '</td>' +
        '<td><span class="tag tag-blue">' + esc(r.type||'') + '</span></td>' +
        '<td>' + esc(r.name||r.id||'') + '</td>' +
        '<td>' + (info || '<span style="color:#475569">—</span>') + '</td>' +
        '</tr>' +
        '<tr class="detail-row" style="display:none"><td colspan="4"><div class="code">' + esc(JSON.stringify(r, null, 2)) + '</div></td></tr>';
    });
    document.getElementById('res-body').innerHTML = html;
  }

  window.toggleRes = function(row) {
    var next = row.nextElementSibling;
    if (next && next.classList.contains('detail-row')) {
      next.style.display = next.style.display === 'none' ? '' : 'none';
    }
  };

  resFilter.addEventListener('input', renderResources);
  resSvcFilter.addEventListener('change', renderResources);
  document.getElementById('res-refresh').addEventListener('click', loadResources);

  // ── REQUESTS ─────────────────────────────────────────────────────────────────
  var reqSvcEl = document.getElementById('req-svc');
  var reqStatusEl = document.getElementById('req-status');
  var reqSearchEl = document.getElementById('req-search');

  function loadRequests() {
    var svc = reqSvcEl.value;
    var url = '/_localrun/requests?limit=200' + (svc ? '&service=' + encodeURIComponent(svc) : '');
    get(url).then(function(data) {
      allRequests = (data.requests || []).slice().reverse();
      renderRequests();
    }).catch(function() {
      document.getElementById('req-body').innerHTML = '<tr><td colspan="6" class="empty">Error loading requests</td></tr>';
    });
  }

  function renderRequests() {
    var q = reqSearchEl.value.toLowerCase();
    var statusFilter = reqStatusEl.value;
    var list = allRequests.filter(function(r) {
      var matchQ = !q || (r.action||r.path||'').toLowerCase().indexOf(q) >= 0;
      var matchS = !statusFilter ||
        (statusFilter === 'ok' && r.status < 400) ||
        (statusFilter === 'err' && r.status >= 400);
      return matchQ && matchS;
    });
    document.getElementById('req-count').textContent = list.length + ' requests';
    if (!list.length) {
      document.getElementById('req-body').innerHTML = '<tr><td colspan="6" class="empty">No requests yet.</td></tr>';
      return;
    }
    var html = '';
    list.forEach(function(r) {
      var t = r.timestamp ? new Date(r.timestamp * 1000).toLocaleTimeString() : '—';
      var cls = r.status >= 500 ? 'err' : r.status >= 400 ? 'warn' : 'ok';
      var dur = (r.duration_ms || 0);
      var durCls = dur > 500 ? 'warn' : dur > 100 ? '' : 'ok';
      html += '<tr>' +
        '<td style="color:#64748b">' + esc(t) + '</td>' +
        '<td>' + (icons[r.service]||'') + ' ' + esc(r.service||'—') + '</td>' +
        '<td style="font-family:monospace">' + esc(r.action||r.path||'—') + '</td>' +
        '<td><span class="tag tag-gray">' + esc(r.method||'—') + '</span></td>' +
        '<td class="' + cls + ' " style="font-weight:600">' + esc(r.status||'—') + '</td>' +
        '<td class="' + durCls + '">' + dur.toFixed(1) + 'ms</td>' +
        '</tr>';
    });
    document.getElementById('req-body').innerHTML = html;
  }

  reqSvcEl.addEventListener('change', loadRequests);
  reqStatusEl.addEventListener('change', renderRequests);
  reqSearchEl.addEventListener('input', renderRequests);
  document.getElementById('req-refresh').addEventListener('click', loadRequests);

  // populate service dropdowns once
  get('/health').then(function(d) {
    var names = Object.keys(d.services || {}).sort();
    var opts = '<option value="">All services</option>' + names.map(function(n) {
      return '<option>' + esc(n) + '</option>';
    }).join('');
    reqSvcEl.innerHTML = opts;
  });

  // ── FAULTS ───────────────────────────────────────────────────────────────────
  function loadFaults() {
    get('/_localrun/faults').then(function(data) {
      var faults = data.faults || [];
      if (!faults.length) {
        document.getElementById('faults-body').innerHTML = '<tr><td colspan="7" class="empty">No active faults. Server behaves normally.</td></tr>';
        return;
      }
      var html = '';
      faults.forEach(function(f) {
        html += '<tr>' +
          '<td style="font-family:monospace;font-size:.73rem;color:#64748b">' + esc((f.id||'').substring(0,8)) + '…</td>' +
          '<td>' + esc(f.service||'*') + '</td>' +
          '<td style="font-family:monospace">' + esc(f.action||'*') + '</td>' +
          '<td><span class="tag ' + (f.type==='error'?'tag-red':'tag-blue') + '">' + esc(f.type||'') + '</span></td>' +
          '<td>' + ((f.probability||1)*100).toFixed(0) + '%</td>' +
          '<td style="color:#94a3b8;font-size:.78rem">' + (f.type==='error' ? 'HTTP '+esc(f.status_code||500) : esc(f.delay_ms||0)+'ms delay') + '</td>' +
          '<td><button class="btn-danger" data-id="' + esc(f.id) + '">Delete</button></td>' +
          '</tr>';
      });
      document.getElementById('faults-body').innerHTML = html;
      document.querySelectorAll('#faults-body .btn-danger').forEach(function(btn) {
        btn.addEventListener('click', function() {
          del('/_localrun/faults?id=' + encodeURIComponent(btn.dataset.id)).then(loadFaults);
        });
      });
    }).catch(function() {
      document.getElementById('faults-body').innerHTML = '<tr><td colspan="7" class="empty">Error loading faults</td></tr>';
    });
  }

  document.getElementById('add-fault-btn').addEventListener('click', function() {
    var body = {
      type: document.getElementById('f-type').value,
      probability: parseFloat(document.getElementById('f-prob').value) || 1.0,
      status_code: parseInt(document.getElementById('f-status').value) || 500,
      delay_ms: parseInt(document.getElementById('f-delay').value) || 2000
    };
    var svc = document.getElementById('f-svc').value.trim();
    var act = document.getElementById('f-action').value.trim();
    if (svc) body.service = svc;
    if (act) body.action = act;
    post('/_localrun/faults', body).then(function() {
      loadFaults();
      document.getElementById('f-svc').value = '';
      document.getElementById('f-action').value = '';
    }).catch(function(e) { alert('Failed to add fault: ' + e); });
  });

  // ── CONFIG ───────────────────────────────────────────────────────────────────
  function loadConfig() {
    Promise.all([get('/health'), get('/_localrun/terraform')]).then(function(results) {
      var d = results[0];
      var tf = results[1];
      var svcs = Object.keys(d.services || {});
      var html =
        '<div class="config-section"><h3>Server</h3>' +
        kv('Version', d.version || '?') +
        kv('Status', d.status || '?') +
        kv('Endpoint', BASE) +
        kv('Region', tf.region || '?') +
        kv('Services', svcs.length + ' active') +
        '</div>' +
        '<div class="config-section"><h3>Service List</h3><div style="display:flex;flex-wrap:wrap;gap:6px">' +
        svcs.sort().map(function(s) { return '<span class="tag tag-blue">' + (icons[s]||'') + ' ' + esc(s) + '</span>'; }).join('') +
        '</div></div>' +
        '<div class="config-section"><h3>Terraform Provider Block</h3><div class="code">' +
        esc(buildTfBlock(tf)) + '</div></div>';
      document.getElementById('config-body').innerHTML = html;
    }).catch(function() {
      document.getElementById('config-body').innerHTML = '<div class="empty">Error loading config</div>';
    });
  }

  function kv(k, v) {
    return '<div class="config-kv"><span class="config-k">' + esc(k) + '</span><span class="config-v">' + esc(v) + '</span></div>';
  }

  function buildTfBlock(tf) {
    var ep = tf.endpoint || BASE;
    var svcs = tf.services || {};
    var lines = ['provider "aws" {',
      '  region                      = "' + (tf.region||'us-east-1') + '"',
      '  access_key                  = "test"',
      '  secret_key                  = "test"',
      '  skip_credentials_validation = true',
      '  skip_metadata_api_check     = true',
      '  skip_requesting_account_id  = true',
      '',
      '  endpoints {'];
    Object.keys(svcs).sort().forEach(function(s) {
      lines.push('    ' + s.padEnd(20) + ' = "' + ep + '"');
    });
    lines.push('  }', '}');
    return lines.join('\n');
  }

  document.getElementById('reset-btn').addEventListener('click', function() {
    if (!confirm('Reset ALL LocalRun state? All resources will be deleted.')) return;
    post('/_localrun/reset', {}).then(function() {
      alert('State reset. Reloading overview...');
      showTab('overview');
    }).catch(function(e) { alert('Reset failed: ' + e); });
  });

  // ── Init ─────────────────────────────────────────────────────────────────────
  loadOverview();
})();
</script>
</body>
</html>"""
