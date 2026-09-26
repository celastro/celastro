# The architecture, in two diagrams

What a node is made of, and what a cluster of them does together. Every
box names the module that implements it (`src/<module>.rs`, or
`src/<module>/`), so the diagram is an index into the code, not a
picture beside it. The prose that explains each decision is in
[design.md](design.md); the SQL each part answers to is in
[sql.md](sql.md). Both diagrams render on GitHub as Mermaid.

## One node

```mermaid
flowchart TB
    subgraph doors["The two doors"]
        console["console: HTTP, a token on every request, TLS when given certificates<br/><i>serve</i>"]
        wire["wire: length-prefixed frames between nodes, a shared token, the caller's identity, TLS<br/><i>wire, tls, crypto::tls13</i>"]
    end
    subgraph coordinator["The coordinator (any node, for any statement)"]
        sql["parse and plan<br/><i>sql, catalog, plan::exec</i>"]
        stats["global term statistics: cached per epoch, or gathered exactly<br/><i>engine (gather_stats)</i>"]
        scatter["scatter: every shard at once, a thread each, under the statement's deadline<br/><i>plan::service::scatter, deadline</i>"]
        fuse["merge, rank fusion, fetch the winning rows<br/><i>plan::fusion, plan::exec</i>"]
        walk["a graph walk, resolved before the scatter<br/><i>plan::walk</i>"]
        explain["EXPLAIN ANALYZE: every runtime decision reported<br/><i>plan::explain</i>"]
    end
    subgraph db["The control plane"]
        catalog["catalog: collections, indexes, policies; the tablet map with holders, followers and terms<br/><i>engine::Db, catalog</i>"]
        cache["statistics cache, refreshed on a write counter<br/><i>engine</i>"]
    end
    subgraph held["A held shard (one writer, at a term)"]
        memtable["memtable: the live rows, flat exact vectors, a write budget<br/><i>memtable</i>"]
        wal["write-ahead log, fsynced before the acknowledgement<br/><i>shard (LogSync)</i>"]
        segments["immutable segments: columns, postings, vector index, footer<br/><i>segment, column, variant, text::postings, vector::hnsw</i>"]
        deletes["commit timestamps and the delete log: visibility at an instant<br/><i>mvcc, time (HLC)</i>"]
        tiers["tiers and residency: active, cached, archived; what is loaded, what is on disk<br/><i>residency, lifecycle</i>"]
    end
    subgraph background["Background threads"]
        sealer["sealer: a memtable into a segment, published by manifest<br/><i>shard (seal)</i>"]
        compactor["compactor: size-tiered, dead-ratio and format-upgrade triggers, held back by the backup horizon<br/><i>compaction</i>"]
        shipper["shipper: the log to each follower, confirmed on two disks<br/><i>replication</i>"]
        stewardt["steward: the election, the lease, the sweep that promotes<br/><i>steward, serve (steward_sweep)</i>"]
    end
    subgraph followed["Followed copies (a follower per shard, applied from the holder's log)"]
        copy["a copy at a term, caught up or behind; promoted by hand or by the steward<br/><i>engine (Followed), replication</i>"]
    end
    subgraph rest["At rest"]
        cipher["every file in authenticated frames under a data key, wrapped under the master key; a ring across a rotation<br/><i>cipher, crypto</i>"]
        backup["backups and the log archive: segments by content, archived logs by timeline, PITR<br/><i>backup, objstore</i>"]
    end
    console --> sql
    wire --> sql
    wire --> held
    wire --> copy
    sql --> walk --> scatter
    sql --> stats --> scatter
    scatter --> held
    scatter -. "other nodes' shards" .-> wire
    scatter --> fuse --> explain
    sql --> catalog
    stats --> cache
    memtable --> wal
    memtable --> sealer --> segments
    segments --> compactor --> segments
    wal --> shipper --> copy
    held --> tiers
    segments --> deletes
    held --> cipher
    segments --> backup
    wal --> backup
    stewardt -. "hellos, leases" .-> wire
```

Not on it, on purpose: the SQL grammar's shape (`docs/sql.md` is that),
the segment's byte layout and the vector index's tiers (`segment` and
`vector` document their own formats), the statement deadline's threading
through every loop (`deadline` is consulted, not drawn), the simulator's
fault schedule on the shard boundary (`sim` stands where the scatter
meets a held shard and injects drops, restarts and reorder), the
console's metrics and logs (`serve`, `log`), and the command line
(`bin`), which is a client of the console like any other.

## A cluster

```mermaid
flowchart LR
    subgraph map["The tablet map, on every node"]
        placement["per collection, per shard: the key range, the holder, the followers, the term<br/><i>catalog (placement), engine (reconcile)</i>"]
    end
    subgraph nodes["Nodes"]
        A["node A: coordinator for this statement; holds shards; follows others<br/><i>engine::Db</i>"]
        B["node B: holds shard 1 at term 3; follows shard 0<br/><i>engine::Db</i>"]
        C["node C: holds shard 2; follows shard 1; the steward this term<br/><i>engine::Db, steward</i>"]
    end
    client["a client, through any node's console<br/><i>serve, bin</i>"] --> A
    A -- "1. a statement's scatter: candidates, scan, fetch, to every holder at once<br/><i>plan::service::scatter, wire (Remote)</i>" --> B
    A -- "1." --> C
    A -- "2. a write, carried to the holder of its key, in chunks; acknowledged after the holder and a follower have it on disk<br/><i>engine (carry_writes), replication (Confirm)</i>" --> B
    A -- "3. a DDL: applied here, then LOCAL on every holder at once; a node away adopts it when it reconnects<br/><i>engine (carry_statement, reconcile)</i>" --> C
    B -- "4. the holder's log, shipped record by record to its follower; a copy behind is caught up; a promotion fences the old holder at a higher term<br/><i>replication (Shipper, fence), engine (promote_here)</i>" --> C
    C -- "5. the steward's sweep: hellos, leases, a promotion for a holder that missed two sweeps, at the next term, told to everyone<br/><i>steward, serve (steward_sweep, lease_renewer)</i>" --> A
    C -- "5." --> B
    A -. "6. hellos and reconcile: the map and the terms, published as nodes meet; a node away learns what it missed<br/><i>wire (hello), engine (reconcile_from)</i>" .-> placement
    B -. "6." .-> placement
    C -. "6." .-> placement
    subgraph archive["The archive (an object store or a directory)"]
        pool["segments by content, once<br/><i>backup (SegmentPool)</i>"]
        logs["archived logs by shard and timeline; a restore forks a timeline<br/><i>backup (LogArchive)</i>"]
    end
    A -- "7. a cluster backup at one instant on every node; archived logs as they seal; PITR to an instant<br/><i>backup, objstore</i>" --> pool
    A --> logs
```

Not on it, on purpose: a shard's move between nodes (`engine` "moves":
the source pins the shard, the target pulls its files, the map
switches and every node agrees), a split and a merge (the holder does
both and the map learns the new range), regions and the placement of
followers across them (`engine` `region_of`, the steward's preference
for a same-region copy under `confirm = all`), the `minimal` tier's
decision of which replica holds an index (`residency::Placement`),
and the rebalance. Each of those is a paragraph in
[design.md](design.md), under the heading that names it.
