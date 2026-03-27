"""Web dashboard for LocalRun — inline HTML/JS/CSS."""

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LocalRun Dashboard</title>
<style>
  body { font-family: monospace; background: #0f172a; color: #e2e8f0; margin: 0; padding: 0; }
  header { background: #1e293b; padding: 12px 24px; border-bottom: 1px solid #334155; display: flex; align-items: center; gap: 16px; }
  header h1 { margin: 0; font-size: 1.2rem; color: #38bdf8; }
  nav { display: flex; gap: 8px; }
  nav button { background: #334155; border: none; color: #e2e8f0; padding: 6px 14px; border-radius: 4px; cursor: pointer; font-family: monospace; }
  nav button.active { background: #38bdf8; color: #0f172a; }
  .tab { display: none; padding: 24px; }
  .tab.active { display: block; }
  .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; margin-bottom: 24px; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 16px; }
  .card h3 { margin: 0 0 8px 0; font-size: 0.9rem; color: #94a3b8; }
  .card .count { font-size: 1.4rem; font-weight: bold; color: #38bdf8; }
  .card .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #22c55e; margin-left: 6px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th { text-align: left; padding: 8px 12px; background: #1e293b; color: #94a3b8; border-bottom: 1px solid #334155; }
  td { padding: 8px 12px; border-bottom: 1px solid #1e293b; }
  tr:hover { background: #1e293b; }
  input, select { background: #1e293b; border: 1px solid #334155; color: #e2e8f0; padding: 6px 10px; border-radius: 4px; font-family: monospace; margin-right: 8px; }
  button.btn { background: #38bdf8; border: none; color: #0f172a; padding: 6px 14px; border-radius: 4px; cursor: pointer; font-family: monospace; font-weight: bold; }
  button.btn-danger { background: #ef4444; color: #fff; }
  .detail { background: #0f172a; border: 1px solid #334155; border-radius: 4px; padding: 12px; margin-top: 8px; white-space: pre; font-size: 0.78rem; overflow-x: auto; }
  .status-ok { color: #22c55e; }
  .status-err { color: #ef4444; }
  .form-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 12px; }
  h2 { margin: 0 0 16px 0; font-size: 1.1rem; color: #38bdf8; }
  .config-row { display: flex; gap: 16px; margin-bottom: 8px; }
  .config-key { color: #94a3b8; min-width: 160px; }
  .config-val { color: #38bdf8; }
</style>
</head>
<body>
<header>
  <h1>LocalRun Dashboard</h1>
  <nav>
    <button class="active" onclick="showTab('overview', this)">Overview</button>
    <button onclick="showTab('resources', this)">Resources</button>
    <button onclick="showTab('requests', this)">Requests</button>
    <button onclick="showTab('faults', this)">Faults</button>
    <button onclick="showTab('config', this)">Config</button>
  </nav>
</header>

<div id="overview" class="tab active">
  <h2>Services</h2>
  <div class="cards" id="svc-cards">Loading...</div>
</div>

<div id="resources" class="tab">
  <h2>Resources</h2>
  <input id="res-filter" placeholder="Filter by name..." oninput="filterResources()" style="margin-bottom:12px;width:260px;">
  <table id="res-table">
    <thead><tr><th>Service</th><th>Type</th><th>Name</th><th>Details</th></tr></thead>
    <tbody id="res-body"></tbody>
  </table>
</div>

<div id="requests" class="tab">
  <h2>Recent Requests</h2>
  <div class="form-row">
    <select id="req-svc-filter" onchange="loadRequests()">
      <option value="">All services</option>
      <option>s3</option><option>sqs</option><option>dynamodb</option><option>sns</option>
      <option>lambda</option><option>iam</option><option>logs</option>
    </select>
    <button class="btn" onclick="loadRequests()">Refresh</button>
  </div>
  <table>
    <thead><tr><th>Time</th><th>Service</th><th>Action</th><th>Status</th><th>Duration</th></tr></thead>
    <tbody id="req-body"></tbody>
  </table>
</div>

<div id="faults" class="tab">
  <h2>Active Faults</h2>
  <div class="form-row">
    <input id="f-service" placeholder="service (e.g. s3)" style="width:120px;">
    <input id="f-action" placeholder="action (e.g. GetObject)" style="width:160px;">
    <select id="f-type"><option>error</option><option>latency</option></select>
    <input id="f-status" placeholder="status (500)" style="width:80px;" value="500">
    <input id="f-prob" placeholder="prob (1.0)" style="width:70px;" value="1.0">
    <button class="btn" onclick="addFault()">Add Fault</button>
  </div>
  <table>
    <thead><tr><th>ID</th><th>Service</th><th>Action</th><th>Type</th><th>Probability</th><th></th></tr></thead>
    <tbody id="faults-body"></tbody>
  </table>
</div>

<div id="config" class="tab">
  <h2>Configuration</h2>
  <div id="config-body"></div>
</div>

<script>
var allResources = [];

function showTab(id, btn) {
  document.querySelectorAll('.tab').forEach(function(t) { t.classList.remove('active'); });
  document.querySelectorAll('nav button').forEach(function(b) { b.classList.remove('active'); });
  document.getElementById(id).classList.add('active');
  btn.classList.add('active');
  if (id === 'overview') loadOverview();
  if (id === 'resources') loadResources();
  if (id === 'requests') loadRequests();
  if (id === 'faults') loadFaults();
  if (id === 'config') loadConfig();
}

function loadOverview() {
  fetch('/health').then(function(r) { return r.json(); }).then(function(data) {
    var svcs = data.services || {};
    var html = '';
    Object.keys(svcs).forEach(function(name) {
      var info = svcs[name];
      var count = '';
      if (typeof info === 'object') {
        var parts = [];
        Object.keys(info).forEach(function(k) { parts.push(info[k] + ' ' + k); });
        count = parts.join(', ') || '0';
      } else {
        count = info;
      }
      html += '<div class="card"><h3>' + name + '<span class="dot"></span></h3><div class="count">' + count + '</div></div>';
    });
    document.getElementById('svc-cards').innerHTML = html || 'No services.';
  }).catch(function() { document.getElementById('svc-cards').innerHTML = 'Error loading services.'; });
}

function loadResources() {
  fetch('/_localrun/resources').then(function(r) { return r.json(); }).then(function(data) {
    allResources = data.resources || [];
    renderResources(allResources);
  });
}

function renderResources(list) {
  var html = '';
  list.forEach(function(r, i) {
    html += '<tr onclick="toggleDetail(' + i + ')">';
    html += '<td>' + (r.service||'') + '</td>';
    html += '<td>' + (r.type||'') + '</td>';
    html += '<td>' + (r.name||'') + '</td>';
    html += '<td><span style="color:#94a3b8;cursor:pointer">expand</span></td>';
    html += '</tr>';
    html += '<tr id="detail-' + i + '" style="display:none"><td colspan="4"><div class="detail">' + JSON.stringify(r, null, 2) + '</div></td></tr>';
  });
  document.getElementById('res-body').innerHTML = html;
}

function toggleDetail(i) {
  var row = document.getElementById('detail-' + i);
  row.style.display = row.style.display === 'none' ? '' : 'none';
}

function filterResources() {
  var q = document.getElementById('res-filter').value.toLowerCase();
  var filtered = allResources.filter(function(r) {
    return (r.name||'').toLowerCase().includes(q) || (r.service||'').toLowerCase().includes(q);
  });
  renderResources(filtered);
}

function loadRequests() {
  var svc = document.getElementById('req-svc-filter').value;
  var url = '/_localrun/requests?limit=50';
  if (svc) url += '&service=' + svc;
  fetch(url).then(function(r) { return r.json(); }).then(function(data) {
    var reqs = (data.requests || []).slice().reverse();
    var html = '';
    reqs.forEach(function(r) {
      var t = new Date(r.timestamp * 1000).toLocaleTimeString();
      var cls = r.status < 400 ? 'status-ok' : 'status-err';
      html += '<tr><td>' + t + '</td><td>' + (r.service||'') + '</td><td>' + (r.action||r.path||'') + '</td>';
      html += '<td class="' + cls + '">' + (r.status||'') + '</td><td>' + (r.duration_ms||0) + 'ms</td></tr>';
    });
    document.getElementById('req-body').innerHTML = html || '<tr><td colspan="5">No requests yet.</td></tr>';
  });
}

function loadFaults() {
  fetch('/_localrun/faults').then(function(r) { return r.json(); }).then(function(data) {
    var faults = data.faults || [];
    var html = '';
    faults.forEach(function(f) {
      html += '<tr><td>' + (f.id||'') + '</td><td>' + (f.service||'*') + '</td><td>' + (f.action||'*') + '</td>';
      html += '<td>' + (f.type||'') + '</td><td>' + (f.probability||1) + '</td>';
      html += '<td><button class="btn btn-danger" onclick="deleteFault(\'' + f.id + '\')">Delete</button></td></tr>';
    });
    document.getElementById('faults-body').innerHTML = html || '<tr><td colspan="6">No active faults.</td></tr>';
  });
}

function addFault() {
  var body = {
    type: document.getElementById('f-type').value,
    probability: parseFloat(document.getElementById('f-prob').value) || 1.0,
    status_code: parseInt(document.getElementById('f-status').value) || 500
  };
  var svc = document.getElementById('f-service').value;
  var act = document.getElementById('f-action').value;
  if (svc) body.service = svc;
  if (act) body.action = act;
  fetch('/_localrun/faults', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)})
    .then(function() { loadFaults(); });
}

function deleteFault(id) {
  fetch('/_localrun/faults?id=' + id, {method:'DELETE'}).then(function() { loadFaults(); });
}

function loadConfig() {
  fetch('/health').then(function(r) { return r.json(); }).then(function(data) {
    var html = '';
    html += '<div class="config-row"><span class="config-key">Version</span><span class="config-val">' + (data.version||'unknown') + '</span></div>';
    html += '<div class="config-row"><span class="config-key">Status</span><span class="config-val">' + (data.status||'unknown') + '</span></div>';
    var svcs = data.services || {};
    var svcNames = Object.keys(svcs).join(', ');
    html += '<div class="config-row"><span class="config-key">Services</span><span class="config-val">' + svcNames + '</span></div>';
    document.getElementById('config-body').innerHTML = html;
  });
}

loadOverview();
setInterval(function() {
  var activeTab = document.querySelector('.tab.active');
  if (activeTab && activeTab.id === 'requests') loadRequests();
}, 3000);
</script>
</body>
</html>"""
