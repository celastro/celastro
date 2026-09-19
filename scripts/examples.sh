#!/usr/bin/env bash
#
# Every example in docs/commands.md and docs/sql.md, run against a built
# binary and checked: a single node, a served console, the tools, a
# three-node cluster on loopback, backups. Prints one PASS/FAIL line per
# example and exits non-zero if any failed. EXAMPLES_SHOW=1 prints every
# example's output as well, which is how the documents' samples were taken.
#
#   cargo build --release && scripts/examples.sh
#
# Needs: bash, curl, jq (for the console examples); docker for the image
# examples, which are skipped without it. Ports 18787-18790 and
# 23521-23523 on loopback.

set -u
CEL=${CELASTRO:-$(cd "$(dirname "$0")/.." && pwd)/target/release/celastro}
[[ -x $CEL ]] || { echo "no binary at $CEL (cargo build --release, or CELASTRO=path)"; exit 2; }
W=$(mktemp -d /tmp/celastro-examples.XXXXXX)
trap 'cleanup' EXIT
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do [[ -n $p ]] && kill "$p" 2>/dev/null; done; sleep 0.3; rm -rf "$W"; }
pass=0; fail=0
show() { [[ ${EXAMPLES_SHOW:-0} == 1 ]] && printf '%s\n' "$1" | sed 's/^/    | /'; return 0; }
# run <name> <expected regex> <command...>: PASS when the output matches.
run() {
  local name=$1 want=$2; shift 2
  local out; out=$("$@" 2>&1); local rc=$?
  if printf '%s' "$out" | grep -qE -- "$want"; then printf 'PASS %s\n' "$name"; pass=$((pass+1)); show "$out"
  else printf 'FAIL %s (exit %s): wanted /%s/\n' "$name" "$rc" "$want"; fail=$((fail+1)); printf '%s\n' "$out" | sed 's/^/    ! /'; fi
}
# sql <name> <expected regex> <dir> <statement>: `celastro --dir DIR exec`.
sql() { local name=$1 want=$2 dir=$3 stmt=$4; run "$name" "$want" "$CEL" --dir "$dir" exec "$stmt"; }
wait_console() { for _ in $(seq 1 100); do "$CEL" health --port "$1" >/dev/null 2>&1 && return 0; sleep 0.1; done; echo "no console on $1"; return 1; }

D=$W/data
echo "== one node, no server ($D)"
run "version" '^celastro [0-9]' "$CEL" version
run "help" 'USAGE' "$CEL" help
cat > "$W/quickstart.sql" <<'SQL'
CREATE COLLECTION notes (id TEXT PRIMARY KEY, topic TEXT, words INT);
CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english');
CREATE INDEX notes_emb ON notes USING vector (embedding) WITH (dims = 4, metric = 'cosine');
CREATE INDEX notes_topic ON notes USING secondary (topic);
INSERT INTO notes VALUES
  ('{"id":"n1","topic":"search","words":7,"body":"BM25 ranks documents by term frequency","embedding":[0.9,0.1,0.0,0.0]}'),
  ('{"id":"n2","topic":"storage","words":8,"body":"An LSM tree seals a memtable into segments","embedding":[0.0,0.0,0.9,0.1]}'),
  ('{"id":"n3","topic":"storage","words":6,"body":"Compaction merges segments into larger ones","embedding":[0.1,0.0,0.8,0.1]}'),
  ('{"id":"n4","topic":"graphs","words":5,"body":"A bounded walk follows edges between documents","embedding":[0.3,0.3,0.3,0.1]}');
SQL
run "run a script" '4 document\(s\) written' "$CEL" --dir "$D" run "$W/quickstart.sql"
run "catalog" 'notes' "$CEL" --dir "$D" catalog
sql "select by predicate" 'n3 .*storage' "$D" "SELECT id, topic FROM notes WHERE topic = 'storage' ORDER BY id"
sql "select IN, comparison, NOT" 'n1' "$D" "SELECT id FROM notes WHERE topic IN ('search', 'graphs') AND words > 6 AND NOT topic = 'graphs'"
sql "select LIKE and IS NULL" 'n1' "$D" "SELECT id FROM notes WHERE topic LIKE 'sea%' AND missing IS NULL"
sql "text match" 'n2' "$D" "SELECT id FROM notes WHERE text_match(body, 'segments memtable')"
sql "text match, prefix and exclusion" 'n3' "$D" "SELECT id FROM notes WHERE text_match(body, 'segment* -memtable')"
sql "text ranked" 'n2|n3' "$D" "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'segments')) LIMIT 2"
sql "vector nearest" 'n2' "$D" "SELECT id FROM notes ORDER BY embedding <=> [0.0,0.0,1.0,0.0] LIMIT 2"
sql "vector within a distance" 'n2' "$D" "SELECT id FROM notes WHERE embedding <=> [0.0,0.0,1.0,0.0] < 0.1"
sql "vector exact" 'n2' "$D" "SELECT id FROM notes ORDER BY embedding <=> [0.0,0.0,1.0,0.0] LIMIT 2 WITH (exact)"
sql "hybrid rrf" 'n2' "$D" "SELECT id, topic FROM notes ORDER BY hybrid(text_match(body, 'segments'), embedding <=> [0.0,0.0,1.0,0.0], method => 'rrf') LIMIT 3"
sql "hybrid linear, weighted" 'n[23]' "$D" "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'segments'), embedding <=> [0.0,0.0,1.0,0.0], method => 'linear', weights => [0.3, 0.7]) LIMIT 3"
sql "count" '4' "$D" "SELECT count(*) FROM notes"
sql "group by" 'storage.*2' "$D" "SELECT topic, count(*) AS n, sum(words) AS words, avg(words) AS avg_words FROM notes GROUP BY topic ORDER BY n DESC LIMIT 5"
sql "min and max" '5.*8|8.*5' "$D" "SELECT min(words), max(words) FROM notes WHERE words > 0"
sql "limit and offset" 'n3' "$D" "SELECT id FROM notes ORDER BY id LIMIT 2 OFFSET 2"
sql "cursor pagination" 'n3' "$D" "SELECT id FROM notes LIMIT 2 AFTER 'n2'"
sql "select with a deadline" 'n1' "$D" "SELECT id FROM notes LIMIT 1 WITH (deadline_ms = 5000)"
sql "explain" 'Query plan' "$D" "EXPLAIN SELECT id FROM notes WHERE topic = 'storage' ORDER BY embedding <=> [0.0,0.0,1.0,0.0] LIMIT 2"
sql "explain analyze" 'ms|µs' "$D" "EXPLAIN ANALYZE SELECT id FROM notes WHERE text_match(body, 'segments') LIMIT 2"
sql "show catalog" 'notes_body' "$D" "SHOW CATALOG notes"
sql "split a shard" 'shard 1 is \[n3, \)' "$D" "SPLIT SHARD 0 OF notes AT 'n3'"
sql "show catalog after the split" 'shard 1 on this node \[n3, \)' "$D" "SHOW CATALOG notes"
sql "count after the split" '4' "$D" "SELECT count(*) FROM notes"
sql "flush" 'flushed' "$D" "FLUSH notes"
sql "show segments" 'segment|seg' "$D" "SHOW SEGMENTS notes"
sql "compact" 'compact' "$D" "COMPACT notes"
sql "show residency" 'resident' "$D" "SHOW RESIDENCY notes"
sql "alter index tier" 'cached' "$D" "ALTER INDEX notes_topic ON notes SET TIER 'cached'"
sql "unload idle" 'unload|idle|0' "$D" "UNLOAD IDLE ON notes"
sql "lifecycle policy" 'policy' "$D" "CREATE LIFECYCLE POLICY cool ON notes FOR (notes_emb) MOVE TO cached AFTER 30 minutes OF INACTIVITY, MOVE TO archived AFTER 7 days SINCE CREATION"
sql "show lifecycle" 'cool' "$D" "SHOW LIFECYCLE"
sql "run lifecycle" 'due to move|moved' "$D" "RUN LIFECYCLE"
sql "drop lifecycle policy" 'dropped' "$D" "DROP LIFECYCLE POLICY cool"
sql "measure recall" 'recall|sample|no ' "$D" "MEASURE RECALL ON notes WITH (k = 2, samples = 2)"
sql "show health, one node" 'answer|node' "$D" "SHOW HEALTH"
sql "delete by key" '1 document\(s\) deleted' "$D" "DELETE FROM notes WHERE id = 'n4'"
sql "delete by predicate" '1 document\(s\) deleted' "$D" "DELETE FROM notes WHERE topic = 'search'"
sql "count after deletes" '2' "$D" "SELECT count(*) FROM notes"
sql "drop index" 'dropped' "$D" "DROP INDEX notes_topic ON notes"
sql "create an edge collection" 'created' "$D" "CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL)"
sql "alter collection" 'altered|set|nodes_of|cites' "$D" "ALTER COLLECTION cites SET (nodes_of = 'notes')"
sql "drop collection" 'dropped' "$D" "DROP COLLECTION cites"
run "json output" '"ok":true' "$CEL" --json --dir "$D" exec "SELECT id FROM notes ORDER BY id LIMIT 1"
run "a graph walk" 'p2' bash -c "$CEL --dir $W/graph run /dev/stdin" <<'SQL'
CREATE COLLECTION papers (id TEXT PRIMARY KEY);
CREATE INDEX papers_body ON papers USING fulltext (body);
CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL) WITH (nodes_of = 'papers');
CREATE INDEX cites_adj ON cites USING adjacency (src, dst);
INSERT INTO papers VALUES ('{"id":"p1","body":"retrieval"}'), ('{"id":"p2","body":"retrieval models"}'), ('{"id":"p3","body":"storage"}');
INSERT INTO cites VALUES ('{"id":"e1","src":"p1","dst":"p2"}'), ('{"id":"e2","src":"p2","dst":"p3"}');
SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites AND text_match(body, 'retrieval') LIMIT 10;
SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p3' VIA cites REVERSE LIMIT 10;
SELECT id FROM papers ORDER BY hybrid(text_match(body, 'retrieval'), hops(id WITHIN 2 HOPS OF 'p1' VIA cites)) LIMIT 10 WITH (max_fanout = 100, max_frontier = 1000);
SQL

echo "== export and import"
run "export" 'export|notes|wrote|copied' "$CEL" --dir "$D" export notes "$W/exp"
run "import" 'import|notes|adopted' "$CEL" --dir "$W/data2" import "$W/exp"
sql "the import answers" '2' "$W/data2" "SELECT count(*) FROM notes"

echo "== backups"
ack=$("$CEL" --dir "$D" exec "BACKUP TO '$W/backups'" 2>&1); echo "    backup: $ack"
run "backup" 'backed up|backup' "$CEL" --dir "$D" exec "BACKUP TO '$W/backups' KEEP 7"
run "verify backup" 'verified|ok|object' "$CEL" --dir "$D" exec "VERIFY BACKUP '$W/backups'"
run "restore" 'restored|restore' "$CEL" --dir "$W/restored" exec "RESTORE FROM '$W/backups'"
sql "the restore answers" '2' "$W/restored" "SELECT count(*) FROM notes"

echo "== encryption at rest, and the tools"
run "key master" 'master|written|key' "$CEL" key master "$W/master.key"
run "key init" 'key|written' env CELASTRO_MASTER_KEY_FILE="$W/master.key" "$CEL" key init "$W/data.key"
run "key master, a second" 'master|written|key' "$CEL" key master "$W/master2.key"
run "key rekey" 'rekey|rewrapped|key' env CELASTRO_MASTER_KEY_FILE="$W/master.key" "$CEL" key rekey "$W/data.key" "$W/master2.key"
run "an encrypted directory" 'created' env CELASTRO_MASTER_KEY_FILE="$W/master.key" "$CEL" --dir "$W/enc" exec "CREATE COLLECTION s (id TEXT PRIMARY KEY)"
run "a write into it" '1 document\(s\) written' env CELASTRO_MASTER_KEY_FILE="$W/master.key" "$CEL" --dir "$W/enc" exec "INSERT INTO s VALUES ('{\"id\":\"secret\"}')"
run "opened without the key, refused" 'key|encrypt' "$CEL" --dir "$W/enc" exec "SELECT id FROM s"
run "tls init" 'ca|cert|wrote|written' "$CEL" tls init "$W/tls" localhost 127.0.0.1 365
ls "$W/tls" | sed 's/^/    tls: /'

echo "== a served console"
export CELASTRO_TOKEN=examples-token-0123456789abcdef
"$CEL" --dir "$D" serve --bind 127.0.0.1 --port 18787 > "$W/serve.out" 2> "$W/serve.err" & PIDS+=($!)
wait_console 18787
run "serve prints its URL" 'http://127.0.0.1:18787' cat "$W/serve.out"
run "health" 'serving' "$CEL" health --port 18787
run "send" 'n2' "$CEL" send http://127.0.0.1:18787 "SELECT id FROM notes ORDER BY id LIMIT 1"
run "exec through --url" 'n2' "$CEL" --url http://127.0.0.1:18787 exec "SELECT id FROM notes ORDER BY id LIMIT 1"
run "catalog through --url" 'notes' "$CEL" --url http://127.0.0.1:18787 catalog
run "repl through --url" 'n2' bash -c "printf 'SELECT id FROM notes ORDER BY id LIMIT 1;\n' | $CEL --url http://127.0.0.1:18787 repl"
run "api query" '"ok":true' curl -s -H "X-Celastro-Token: $CELASTRO_TOKEN" -H "Content-Type: application/json" http://127.0.0.1:18787/api/query -d '{"sql": "SELECT count(*) FROM notes"}'
run "api health" '"ok":true' curl -s http://127.0.0.1:18787/api/health
run "api catalog" 'notes' curl -s -H "X-Celastro-Token: $CELASTRO_TOKEN" http://127.0.0.1:18787/api/catalog
run "api metrics" 'celastro_statements_total' curl -s -H "X-Celastro-Token: $CELASTRO_TOKEN" http://127.0.0.1:18787/api/metrics
run "api shutdown" 'ok' curl -s -X POST -H "X-Celastro-Token: $CELASTRO_TOKEN" http://127.0.0.1:18787/api/shutdown
sleep 1
run "a console over TLS" 'n2' bash -c "
  CELASTRO_TLS_CERT=$W/tls/tls.crt CELASTRO_TLS_KEY=$W/tls/tls.key CELASTRO_TLS_CA=$W/tls/ca.crt \
    $CEL --dir $D serve --bind 127.0.0.1 --port 18788 >/dev/null 2>$W/tls.err & echo \$! > $W/tls.pid
  for i in \$(seq 1 100); do curl -s --cacert $W/tls/ca.crt https://127.0.0.1:18788/api/health >/dev/null 2>&1 && break; sleep 0.1; done
  CELASTRO_TLS_CA=$W/tls/ca.crt $CEL send https://127.0.0.1:18788 \"SELECT id FROM notes ORDER BY id LIMIT 1\"
  kill \$(cat $W/tls.pid)"

echo "== three nodes on loopback"
export CELASTRO_WIRE_TOKEN=examples-wire-token-0123456789
NODE_PIDS=()
for i in 1 2 3; do
  CELASTRO_NODE=tcp://127.0.0.1:2352$i "$CEL" --dir "$W/node$i" serve --bind 127.0.0.1 --port $((18788 + i)) --shard-bind 127.0.0.1:2352$i >/dev/null 2>"$W/node$i.err" & PIDS+=($!); NODE_PIDS+=($!)
done
for i in 1 2 3; do wait_console $((18788 + i)) || cat "$W/node$i.err"; done
A=http://127.0.0.1:18789; B=http://127.0.0.1:18790; C=http://127.0.0.1:18791
run "attach node" 'attached' "$CEL" send $A "ATTACH NODE 'tcp://127.0.0.1:23522'"
run "attach node, the third" 'attached' "$CEL" send $A "ATTACH NODE 'tcp://127.0.0.1:23523'"
run "health --attached" 'serving' "$CEL" health --port 18789 --attached 2
run "create a collection over three shards" '3 shard' "$CEL" send $A "CREATE COLLECTION notes (id TEXT PRIMARY KEY, tenant TEXT NOT NULL) PARTITION BY (tenant) WITH (splits = ['m', 't'])"
run "an index on every holder" 'created' "$CEL" send $A "CREATE INDEX notes_body ON notes USING fulltext (body)"
run "a write forwarded to its holder" '3 document\(s\) written' "$CEL" send $B "INSERT INTO notes VALUES ('{\"id\":\"a1\",\"tenant\":\"acme\",\"body\":\"first\"}'), ('{\"id\":\"p1\",\"tenant\":\"pear\",\"body\":\"second\"}'), ('{\"id\":\"z1\",\"tenant\":\"zed\",\"body\":\"third\"}')"
run "a query from any node" '3' "$CEL" send $C "SELECT count(*) FROM notes"
run "group by tenant" 'acme' "$CEL" send $C "SELECT tenant, count(*) AS n FROM notes GROUP BY tenant ORDER BY tenant"
run "show health" '3 of 3 node' "$CEL" send $A "SHOW HEALTH"
run "show catalog with placement" 'shard 0' "$CEL" send $A "SHOW CATALOG notes"
run "move a shard" 'moved from' "$CEL" send $A "MOVE SHARD 2 OF notes TO 'tcp://127.0.0.1:23521'"
run "the map switched everywhere" '3' "$CEL" send $C "SELECT count(*) FROM notes"
run "rebalance" 'rebalance|moved|already' "$CEL" send $A "REBALANCE notes"
run "split a shard" 'shard 3 is \[p, t\)' "$CEL" send $A "SPLIT SHARD 1 OF notes AT 'p'"
run "the new shard moves" 'moved from' "$CEL" send $A "MOVE SHARD 3 OF notes TO 'tcp://127.0.0.1:23521'"
run "the count is whole after the split" '"count\(\*\)":3' "$CEL" send $C "SELECT count(*) FROM notes"
run "place shard (repair, here a no-op)" 'placed' "$CEL" send $A "LOCAL PLACE SHARD 0 OF notes ON 'tcp://127.0.0.1:23521'"
run "a cluster backup at one instant" 'backed up|instant|AS OF' "$CEL" send $A "BACKUP CLUSTER TO '$W/cluster-backups'"
run "detach refused while the node holds a shard" 'holds 1 shard' "$CEL" send $A "DETACH NODE 'tcp://127.0.0.1:23523'"
run "move its shard away first" 'moved from' "$CEL" send $A "MOVE SHARD 2 OF notes TO 'tcp://127.0.0.1:23521'"
run "detach node" 'detached' "$CEL" send $A "DETACH NODE 'tcp://127.0.0.1:23523'"
kill "${NODE_PIDS[1]}"; sleep 0.5
run "partial results when a node is gone" 'missing":\["shard 1"\]' "$CEL" send $A "SELECT count(*) FROM notes WITH (partial_results, deadline_ms = 3000)"
run "without partial results, refused naming the node" 'did not answer|deadline' "$CEL" send $A "SELECT count(*) FROM notes WITH (deadline_ms = 2000)"

if command -v docker >/dev/null 2>&1 && docker image inspect ghcr.io/celastro/celastro:0.55.0 >/dev/null 2>&1; then
  echo "== the image"
  run "docker version" '^celastro 0' docker run --rm ghcr.io/celastro/celastro:0.55.0 version
  run "docker demo" 'notes|hybrid|demo' docker run --rm ghcr.io/celastro/celastro:0.55.0 demo
else
  echo "== the image: skipped (no docker, or the image is not local)"
fi

if [[ $fail != 0 ]]; then
  echo "== what the servers said (stderr)"
  for f in "$W"/*.err; do [[ -f $f ]] && { echo "-- $(basename "$f")"; tail -5 "$f" | cut -c1-200; }; done
fi
printf '\n%d passed, %d failed\n' "$pass" "$fail"
[[ $fail == 0 ]]
