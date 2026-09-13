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

runBtn.addEventListener('click', runQuery);
document.getElementById('refresh').addEventListener('click', loadCatalog);
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
editor.focus();
