// SECURITY, and the reason this file looks the way it does.
//
// Every result cell on this page comes out of the database, and documents in a
// database are attacker-controlled in any deployment that matters: whoever can
// INSERT can choose the bytes this page renders. So this file builds DOM nodes
// and sets .textContent. It never assigns innerHTML or outerHTML, never calls
// insertAdjacentHTML, document.write or eval, and there is no "but this value
// is safe" exception -- one exception is all an injection needs, and the
// sanitiser you would have to write to make the exception safe is longer than
// this file. If you came here to collapse the table builder into an HTML
// template string: please don't.
'use strict';

// The token arrives once, in the URL the CLI printed. We read it into this
// variable and then rewrite the address bar without it, so it is not sitting on
// screen during a screenshare and not written into the browser's history
// database (which some browsers sync between devices). Every later request
// sends it as a header instead, so it also stays out of access logs and
// Referer. From here on the token exists only in memory: navigating away loses
// it, which is why nothing on this page is a link or a form submit.
var TOKEN = new URLSearchParams(location.search).get('t') || '';
if (location.search) {
  // A console that failed to boot over a cosmetic URL rewrite would be a bad
  // trade, so a refusal here is survivable: the token is already captured.
  try {
    history.replaceState({}, '', location.pathname);
  } catch (e) { /* leave the token in the URL rather than lose the page */ }
}

var MAX_ROWS = 500; // DOM cap. Beyond this we truncate, and say so.
var MAX_HISTORY = 20;

var editor = document.getElementById('sql');
var statusLine = document.getElementById('status');
var results = document.getElementById('results');
var runBtn = document.getElementById('run');
var spinner = document.getElementById('spinner');
var collections = document.getElementById('collections');
var historyList = document.getElementById('history');

var busy = false;
// Session-scoped, in memory only -- see the note in the sidebar. Named `recent`
// rather than `history` so it cannot shadow window.history, which this file
// calls to strip the token out of the address bar.
var recent = [];

function el(tag, cls, text) {
  var node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function setStatus(text, kind) {
  statusLine.textContent = text;
  statusLine.className = kind ? 'status ' + kind : 'status';
}

function setBusy(on) {
  busy = on;
  runBtn.disabled = on;
  spinner.hidden = !on;
}

function api(path, init) {
  var opts = init || {};
  var headers = { 'X-Celastro-Token': TOKEN };
  for (var k in opts.headers) headers[k] = opts.headers[k];
  opts.headers = headers;
  return fetch(path, opts).then(function (res) {
    // A SQL error is HTTP 200 with ok:false. A real status code here means a
    // protocol problem (bad token, bad host, oversized body), and the body may
    // not be JSON at all, so do not try to parse it.
    if (!res.ok) throw new Error('HTTP ' + res.status + ' from ' + path);
    return res.json();
  });
}

// The metrics page is text, not JSON, and it is the only thing on this
// console that is.
function apiText(path) {
  return fetch(path, { headers: { 'X-Celastro-Token': TOKEN } }).then(function (res) {
    if (!res.ok) throw new Error('HTTP ' + res.status + ' from ' + path);
    return res.text();
  });
}

function runQuery() {
  var sql = editor.value.trim();
  if (busy) return;
  if (!sql) {
    setStatus('Nothing to run — the editor is empty.');
    editor.focus();
    return;
  }
  setBusy(true);
  setStatus('Running…');
  api('/api/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql: sql })
  }).then(function (res) {
    remember(sql);
    render(res);
    // DDL changes the catalog, and nothing in the response says whether it did.
    if (res && res.ok) loadCatalog();
  }).catch(function (err) {
    render({ ok: false, error: err && err.message ? err.message : String(err) });
  }).then(function () {
    setBusy(false);
  });
}

function render(res) {
  clear(results);
  if (!res || typeof res !== 'object') {
    renderError('The server sent a response this console could not read.');
    return;
  }
  if (!res.ok) {
    // The editor is untouched, so the statement can be corrected and re-run.
    renderError(res.error || 'unknown error');
    return;
  }
  if (res.kind === 'rows') renderRows(res);
  else if (res.kind === 'ack') renderAck(res);
  else if (res.kind === 'explain') renderText(res.text, 'Plan returned.');
  else if (res.kind === 'recall') renderText(res.text, 'Recall report returned.');
  else renderError('Unknown result kind: ' + String(res.kind));
}

function renderError(message) {
  var box = el('div', 'error');
  box.appendChild(el('strong', null, 'Error'));
  box.appendChild(el('pre', null, message));
  results.appendChild(box);
  setStatus('Error: ' + message, 'error');
}

// SHOW SEGMENTS, SHOW CATALOG, SHOW RESIDENCY and SHOW LIFECYCLE acknowledge
// with a column-aligned, multi-line report. Inside a <p> that collapses into
// one unreadable run of words and the alignment -- the whole point of the
// report -- is gone, so a message containing a newline is rendered
// preformatted. Still textContent, never innerHTML: this text came out of the
// database. The live region gets a one-line summary instead, because a screen
// reader announcing forty lines of padded columns is worse than silence.
function renderAck(res) {
  var message = String(res.message || 'ok');
  if (message.indexOf('\n') === -1) {
    results.appendChild(el('p', 'ack', message));
    setStatus(message);
    return;
  }
  var lines = message.split('\n');
  results.appendChild(el('pre', 'ack', message));
  setStatus(firstLine(lines) + ' — ' + lines.length + '-line report below.');
}

function firstLine(lines) {
  for (var i = 0; i < lines.length; i++) {
    var trimmed = lines[i].trim();
    if (trimmed) return trimmed;
  }
  return 'Report returned.';
}

function renderText(text, announcement) {
  results.appendChild(el('pre', 'text', text || ''));
  setStatus(announcement);
}

function renderRows(res) {
  var rows = Array.isArray(res.rows) ? res.rows : [];
  var count = typeof res.count === 'number' ? res.count : rows.length;
  var shown = Math.min(rows.length, MAX_ROWS);
  var summary = count + (count === 1 ? ' row' : ' rows');
  if (typeof res.elapsed_ms === 'number') summary += ' in ' + res.elapsed_ms + ' ms';
  if (shown < rows.length) {
    // "showing the first 500 of 2000 returned" can be read as the server having
    // capped the result at 500. It did not -- it sent all of them and this page
    // is the one truncating -- so name the side that dropped the rows.
    summary += ' — the server sent every one of these ' + rows.length + ' rows; this ' +
      'console renders the first ' + shown + ' and leaves the rest out of the table';
  }

  // A partial result rendered as a complete one is the worst failure this
  // console has, so it gets its own block and goes into the live region too.
  var missing = Array.isArray(res.missing) ? res.missing : [];
  var warnings = [];
  if (missing.length) {
    warnings.push('PARTIAL RESULT: ' + missing.join(', ') +
      ' did not answer. Rows held only there are missing from this table.');
  }
  // `truncated_prefixes` is `missing`'s sibling: both say the answer is
  // short. A wide `a*` comes back cut on every surface, and this console was
  // the one surface that showed the short table and said nothing.
  var cut = Array.isArray(res.truncated_prefixes) ? res.truncated_prefixes : [];
  cut.forEach(function (line) { warnings.push('TRUNCATED — ' + line); });
  // And the third sibling: a walk one of its caps bound.
  var cutWalks = Array.isArray(res.cut_walks) ? res.cut_walks : [];
  cutWalks.forEach(function (line) { warnings.push('CUT — ' + line); });
  if (warnings.length) {
    warnings.forEach(function (w) { results.appendChild(el('p', 'warn', w)); });
    setStatus(summary + '. ' + warnings.join(' '), 'warn');
  } else {
    setStatus(summary);
  }
  results.appendChild(el('p', 'summary', summary));
  if (res.next_cursor) {
    var more = el('p', 'muted', 'More rows remain. Next cursor: ');
    more.appendChild(el('code', null, res.next_cursor));
    results.appendChild(more);
  }

  // Only show score and distance when the query produced them; a column of
  // empty cells reads as "no score", which is a different claim.
  var hasScore = rows.some(function (r) { return r && r.score !== null && r.score !== undefined; });
  var hasDist = rows.some(function (r) { return r && r.distance !== null && r.distance !== undefined; });

  var head = el('tr');
  head.appendChild(el('th', null, 'key'));
  if (hasScore) head.appendChild(el('th', 'num', 'score'));
  if (hasDist) head.appendChild(el('th', 'num', 'distance'));
  head.appendChild(el('th', null, 'document'));
  var thead = el('thead');
  thead.appendChild(head);

  var tbody = el('tbody');
  for (var i = 0; i < shown; i++) {
    var r = rows[i] || {};
    var tr = el('tr');
    tr.appendChild(el('td', 'key', r.key));
    if (hasScore) tr.appendChild(el('td', 'num', num(r.score)));
    if (hasDist) tr.appendChild(el('td', 'num', num(r.distance)));
    var cell = el('td', 'doc');
    cell.appendChild(el('pre', null, json(r.doc)));
    tr.appendChild(cell);
    tbody.appendChild(tr);
  }
  var table = el('table');
  table.appendChild(thead);
  table.appendChild(tbody);

  var wrap = el('div', 'tablewrap');
  wrap.tabIndex = 0; // a scrollable region has to be reachable without a mouse
  wrap.setAttribute('role', 'region');
  wrap.setAttribute('aria-label', 'Result rows');
  wrap.appendChild(table);
  results.appendChild(wrap);
}

function num(v) {
  if (typeof v !== 'number') return '';
  return Number.isInteger(v) ? String(v) : v.toFixed(4);
}

// One space of indent: enough structure to read nested documents, tight enough
// that a 40-field document does not push the next row off the screen.
function json(doc) {
  if (doc === undefined) return '';
  var s = JSON.stringify(doc, null, 1);
  return s === undefined ? '' : s;
}

function loadCatalog() {
  api('/api/catalog').then(function (res) {
    var list = res && Array.isArray(res.collections) ? res.collections : [];
    clear(collections);
    if (!list.length) {
      collections.appendChild(el('li', 'muted', 'No collections yet.'));
      return;
    }
    list.forEach(function (entry) {
      // Accept a bare name or an object with one, rather than emptying the
      // sidebar over a field that got renamed.
      var name = typeof entry === 'string' ? entry : (entry && entry.name) || '';
      if (!name) return;
      var li = el('li');
      var button = el('button', 'link', name);
      button.type = 'button';
      button.addEventListener('click', function () {
        insert('SELECT * FROM ' + name + ' LIMIT 10;');
      });
      li.appendChild(button);
      if (entry && typeof entry.doc_count === 'number') {
        li.appendChild(el('span', 'count', entry.doc_count));
      }
      collections.appendChild(li);
    });
  }).catch(function (err) {
    clear(collections);
    collections.appendChild(el('li', 'muted', 'Catalog unavailable: ' + err.message));
  });
}

// Insert at the caret rather than replacing: clicking a collection while
// half-way through a statement should not throw the statement away.
function insert(text) {
  var before = editor.value.slice(0, editor.selectionStart);
  var after = editor.value.slice(editor.selectionEnd);
  var gap = before && before.slice(-1) !== '\n' ? '\n' : '';
  editor.value = before + gap + text + after;
  var caret = (before + gap + text).length;
  editor.setSelectionRange(caret, caret);
  editor.focus();
}

function remember(sql) {
  if (recent[0] === sql) return; // re-running the same statement is not new
  recent.unshift(sql);
  if (recent.length > MAX_HISTORY) recent.length = MAX_HISTORY;
  clear(historyList);
  recent.forEach(function (entry) {
    var li = el('li');
    var button = el('button', 'link hist', entry.replace(/\s+/g, ' '));
    button.type = 'button';
    button.title = entry;
    button.addEventListener('click', function () {
      editor.value = entry;
      editor.focus();
    });
    li.appendChild(button);
    historyList.appendChild(li);
  });
}

// ---------------------------------------------------------------- monitoring
//
// Two answers, side by side: what SHOW HEALTH says about this node and the
// ones it knows, and the handful of rates from /api/metrics that tell you
// whether it is working. Both go through paths this console already serves
// and nothing here reaches anywhere else.
//
// The health report marks what needs doing in capitals -- DOWN, UNREACHABLE,
// CLOCK OFF, EXPIRES SOON, GONE, NOT ADOPTED, AN OLDER PROCESS -- so this
// panel colours the lines carrying those rather than inventing a second
// opinion about what is wrong. If the server grows another marker, the line
// still shows; it is just not coloured, which is the safe direction.
var MON_DOWN = ['DOWN', 'UNREACHABLE', 'GONE', 'EXPIRED', 'NOT ADOPTED'];
var MON_ACT = ['EXPIRES SOON', 'CLOCK OFF', 'AN OLDER PROCESS', 'previous key(s) kept',
  'seal failure', 'restarted since last seen', 'none elected yet'];
var MON_EVERY_MS = 10000;
var MON_SAMPLES = 60; // ten minutes at ten seconds, and the sparkline's width

var monHealth = document.getElementById('mon-health');
var monNumbers = document.getElementById('mon-numbers');
var monAuto = document.getElementById('mon-auto');
var monTimer = null;
var monLast = null;   // the previous sample, for the deltas
var monSeries = {};   // name -> the last MON_SAMPLES rates, oldest first

// The rates this panel draws, in the order they are shown. `of` names the
// counter in the metrics page; a rate is its delta over the seconds between
// two samples, which is what the page can know without any history.
var MON_RATES = [
  { key: 'statements', of: 'celastro_statements_total', name: 'statements/s' },
  { key: 'refused', of: 'celastro_requests_refused_total', name: 'refused/s' },
  { key: 'failed', of: 'celastro_statements_failed_total', name: 'failed/s' },
  { key: 'connections', of: 'celastro_connections_total', name: 'connections/s' },
  { key: 'compactions', of: 'celastro_compactions_total', name: 'compactions/s' }
];

// The Prometheus text format, as much of it as this page needs: `name 1.5`
// and `name{labels} 1.5`, with the # lines skipped. Returns a map of the
// plain counters and, separately, the histogram's cumulative buckets.
function parseMetrics(text) {
  var values = {};
  var buckets = [];
  text.split('\n').forEach(function (raw) {
    var line = raw.trim();
    if (!line || line.charAt(0) === '#') return;
    var cut = line.lastIndexOf(' ');
    if (cut < 1) return;
    var name = line.slice(0, cut);
    var value = parseFloat(line.slice(cut + 1));
    if (!isFinite(value)) return;
    var le = /^celastro_statement_seconds_bucket\{le="([^"]+)"\}$/.exec(name);
    if (le) {
      buckets.push({ le: le[1] === '+Inf' ? Infinity : parseFloat(le[1]), count: value });
      return;
    }
    if (name.indexOf('{') === -1) values[name] = value;
  });
  buckets.sort(function (a, b) { return a.le - b.le; });
  return { values: values, buckets: buckets };
}

// The 95th percentile of the statements in THIS window: the difference
// between two cumulative bucket sets, interpolated inside the bucket the
// rank falls in, which is what histogram_quantile does. The widest finite
// bound is the last thing this can say -- beyond it the answer is "slower
// than that", and the panel says so rather than drawing a number.
function windowP95(before, after) {
  if (!before || before.length !== after.length || !after.length) return null;
  var deltas = after.map(function (b, i) { return Math.max(0, b.count - before[i].count); });
  var total = deltas[deltas.length - 1];
  if (!total) return null;
  var rank = 0.95 * total;
  for (var i = 0; i < deltas.length; i++) {
    if (deltas[i] < rank) continue;
    var lo = i === 0 ? 0 : after[i - 1].le;
    var below = i === 0 ? 0 : deltas[i - 1];
    if (!isFinite(after[i].le)) return { over: lo };
    var span = deltas[i] - below;
    var within = span ? (rank - below) / span : 0;
    return { at: lo + (after[i].le - lo) * within };
  }
  return null;
}

// A name, not an address: createElementNS identifies the SVG dialect with
// it and fetches nothing. It is the only absolute URL in this file, and the
// test in serve.rs holds it to that.
var SVG_NS = 'http://www.w3.org/2000/svg';

function sparkline(points) {
  var svg = document.createElementNS(SVG_NS, 'svg');
  svg.setAttribute('class', 'mon-spark');
  svg.setAttribute('viewBox', '0 0 ' + MON_SAMPLES + ' 10');
  svg.setAttribute('preserveAspectRatio', 'none');
  svg.setAttribute('aria-hidden', 'true'); // the number beside it is the content
  var top = Math.max.apply(null, points.concat([0])) || 1;
  var line = document.createElementNS(SVG_NS, 'polyline');
  // Right-aligned, so a fresh page draws the newest samples where the older
  // ones will be rather than stretching two points across the whole box.
  var start = MON_SAMPLES - points.length;
  line.setAttribute('points', points.map(function (v, i) {
    return (start + i) + ',' + (10 - (v / top) * 9.5).toFixed(2);
  }).join(' '));
  svg.appendChild(line);
  return svg;
}

function monStat(name, value, points) {
  var box = el('div', 'mon-stat');
  box.appendChild(el('span', 'mon-name', name));
  box.appendChild(el('span', 'mon-value', value));
  if (points && points.length > 1) box.appendChild(sparkline(points));
  return box;
}

function renderHealth(message) {
  clear(monHealth);
  var lines = String(message || '').split('\n').filter(function (l) { return l.trim(); });
  if (!lines.length) {
    monHealth.appendChild(el('p', 'muted', 'The node answered with nothing to report.'));
    return;
  }
  var flagged = 0;
  lines.forEach(function (text) {
    var kind = '';
    if (MON_DOWN.some(function (m) { return text.indexOf(m) !== -1; })) kind = ' down';
    else if (MON_ACT.some(function (m) { return text.indexOf(m) !== -1; })) kind = ' act';
    if (kind) flagged++;
    monHealth.appendChild(el('p', 'mon-line' + kind, text));
  });
  // The live region would otherwise read the whole report aloud on every
  // tick; a count of what is marked is the part worth hearing.
  monHealth.setAttribute('aria-label', flagged
    ? 'Health: ' + flagged + ' line(s) need attention'
    : 'Health: nothing marked');
}

function renderRates(sample) {
  clear(monNumbers);
  var seconds = monLast ? (sample.at - monLast.at) / 1000 : 0;
  MON_RATES.forEach(function (r) {
    var now = sample.values[r.of];
    var text = '—';
    if (typeof now === 'number') {
      if (monLast && seconds > 0 && typeof monLast.values[r.of] === 'number') {
        // A counter that went backwards is a process that restarted, not a
        // negative rate.
        var delta = now - monLast.values[r.of];
        var rate = delta < 0 ? 0 : delta / seconds;
        monSeries[r.key] = (monSeries[r.key] || []).concat([rate]).slice(-MON_SAMPLES);
        text = rate < 10 ? rate.toFixed(2) : Math.round(rate).toString();
      } else {
        text = '…';
      }
    }
    monNumbers.appendChild(monStat(r.name, text, monSeries[r.key]));
  });
  var p95 = monLast ? windowP95(monLast.buckets, sample.buckets) : null;
  var p95text = '—';
  if (p95 && p95.over !== undefined) p95text = '> ' + p95.over + ' s';
  else if (p95) {
    p95text = p95.at < 1 ? Math.round(p95.at * 1000) + ' ms' : p95.at.toFixed(2) + ' s';
    monSeries.p95 = (monSeries.p95 || []).concat([p95.at]).slice(-MON_SAMPLES);
  } else if (monLast) p95text = 'no statements';
  monNumbers.appendChild(monStat('p95 statement', p95text, monSeries.p95));
  monLast = sample;
}

function loadMonitoring() {
  // A statement is running: SHOW HEALTH would queue behind it and the timer
  // would pile up more. The next tick will do.
  if (busy) return;
  api('/api/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql: 'SHOW HEALTH' })
  }).then(function (res) {
    if (res && res.ok) renderHealth(res.message);
    else renderHealth('SHOW HEALTH: ' + ((res && res.error) || 'no answer'));
  }).catch(function (err) {
    clear(monHealth);
    monHealth.appendChild(el('p', 'mon-line down', 'Health unavailable: ' + err.message));
  });
  apiText('/api/metrics').then(function (text) {
    var parsed = parseMetrics(text);
    renderRates({ at: Date.now(), values: parsed.values, buckets: parsed.buckets });
  }).catch(function (err) {
    clear(monNumbers);
    monNumbers.appendChild(el('p', 'muted', 'Metrics unavailable: ' + err.message));
  });
}

function monitoringTimer(on) {
  if (monTimer) { clearInterval(monTimer); monTimer = null; }
  if (on) monTimer = setInterval(loadMonitoring, MON_EVERY_MS);
}

runBtn.addEventListener('click', runQuery);
document.getElementById('refresh').addEventListener('click', function () {
  loadCatalog();
  loadMonitoring();
});
document.getElementById('mon-refresh').addEventListener('click', loadMonitoring);
monAuto.addEventListener('change', function () { monitoringTimer(monAuto.checked); });
editor.addEventListener('keydown', function (e) {
  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
    e.preventDefault();
    runQuery();
  }
});

if (!TOKEN) {
  setStatus('No access token in this URL. Open the link the CLI printed.', 'error');
}
api('/api/health').then(function (res) {
  if (res && res.version) document.getElementById('version').textContent = 'v' + res.version;
}).catch(function () { /* the status line already reports anything that matters */ });
loadCatalog();
loadMonitoring();
monitoringTimer(monAuto.checked);
editor.focus();
