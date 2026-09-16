//! Recursive-descent parser.

use crate::catalog::Metric;
use crate::column::CmpOp;
use crate::error::{Error, Result};
use crate::lifecycle::{Every, Rule, Trigger, Unit};
use crate::residency::Tier;
use crate::sql::ast::*;
use crate::sql::lexer::{lex, Tok};
use crate::value::{Value, ValueType};

/// How deeply a predicate or literal may nest. `primary_expr` recurses into
/// `expr` once per `(`, so without a cap an input like `((((...))))` overflows
/// the stack long before it runs out of tokens. Real queries nest a handful of
/// levels.
const MAX_EXPR_DEPTH: usize = 128;

pub fn parse(sql: &str, params: &[Value]) -> Result<Statement> {
    let toks = lex(sql)?;
    let mut p = Parser { t: toks, i: 0, params, depth: 0 };
    let s = p.statement()?;
    p.eat_punct(";");
    p.expect_eof()?;
    Ok(s)
}

struct Parser<'a> {
    t: Vec<Tok>,
    i: usize,
    params: &'a [Value],
    /// Current recursive-descent depth; see `MAX_EXPR_DEPTH`.
    depth: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> &Tok {
        self.t.get(self.i).unwrap_or(&Tok::Eof)
    }

    fn next(&mut self) -> Tok {
        let t = self.t.get(self.i).cloned().unwrap_or(Tok::Eof);
        self.i += 1;
        t
    }

    /// Counts one level of recursive descent. Every caller must pair this with
    /// `leave` on the paths that return a value.
    fn enter(&mut self) -> Result<()> {
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            return Err(Error::Sql(format!("expression nests deeper than {MAX_EXPR_DEPTH}")));
        }
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }

    fn expect_eof(&self) -> Result<()> {
        if matches!(self.peek(), Tok::Eof) {
            Ok(())
        } else {
            Err(Error::Sql(format!("unexpected {} after statement", self.peek().describe())))
        }
    }

    /// Case-insensitive keyword match without consuming.
    fn is_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Tok::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(kw) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(Error::Sql(format!("expected `{kw}`, found {}", self.peek().describe())))
        }
    }

    fn eat_punct(&mut self, p: &str) -> bool {
        if matches!(self.peek(), Tok::Punct(x) if *x == p) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn expect_punct(&mut self, p: &str) -> Result<()> {
        if self.eat_punct(p) {
            Ok(())
        } else {
            Err(Error::Sql(format!("expected `{p}`, found {}", self.peek().describe())))
        }
    }

    fn ident(&mut self) -> Result<String> {
        match self.next() {
            Tok::Ident(s) => Ok(s),
            other => Err(Error::Sql(format!("expected an identifier, found {}", other.describe()))),
        }
    }

    /// A dotted path. A leading `doc.` is a convention for the document root
    /// and is stripped, so `doc.author.name` and `author.name` are the same
    /// path.
    fn path(&mut self) -> Result<String> {
        let mut p = self.path_segment()?;
        while self.eat_punct(".") {
            p.push('.');
            p.push_str(&self.path_segment()?);
        }
        Ok(p.strip_prefix("doc.").unwrap_or(&p).to_string())
    }

    /// One segment of a dotted path.
    ///
    /// A double-quoted identifier is the only way a `.` can reach here inside a
    /// single token, and every consumer of a path splits the string back apart
    /// on `.` (`Value::path`, `Value::set_path`, the catalog's path walk). So
    /// `"a.b"` was answered against the nested path `a` → `b` rather than
    /// against the field actually named `a.b`, with nothing to say which was
    /// meant. Refusing it rejects an input that was being silently resolved
    /// against the wrong field; the unquoted `a.b` means that nested path and
    /// is untouched, as is a quoted name with no dot in it, which is what
    /// quoting is for.
    fn path_segment(&mut self) -> Result<String> {
        let s = self.ident()?;
        if s.contains('.') {
            return Err(Error::Sql(format!(
                "a quoted identifier cannot contain `.`: `{s}` would be read as a nested path. \
                 A field whose NAME contains a dot cannot be reached by any path expression; \
                 store it under a name without one"
            )));
        }
        Ok(s)
    }

    // ---------------------------------------------------------------- stmts

    fn statement(&mut self) -> Result<Statement> {
        if self.eat_kw("LOCAL") {
            self.enter()?;
            let inner = self.statement();
            self.leave();
            return Ok(Statement::Local(Box::new(inner?)));
        }
        let attach = self.eat_kw("ATTACH");
        if attach || self.eat_kw("DETACH") {
            self.expect_kw("NODE")?;
            let url = match self.literal()? {
                Value::Str(s) => s,
                other => {
                    return Err(Error::Sql(format!(
                        "a node address is a string like 'tcp://host:port', not {}",
                        crate::json::to_string(&other)
                    )))
                }
            };
            return Ok(if attach {
                Statement::AttachNode { url }
            } else {
                Statement::DetachNode { url }
            });
        }
        if self.eat_kw("MOVE") || self.is_kw("PLACE") {
            let place = self.eat_kw("PLACE");
            self.expect_kw("SHARD")?;
            let shard = self.usize_literal()?;
            self.expect_kw("OF")?;
            let collection = self.ident()?;
            self.expect_kw(if place { "ON" } else { "TO" })?;
            let node = self.node_address()?;
            return Ok(if place {
                Statement::PlaceShard { collection, shard, node }
            } else {
                Statement::MoveShard { collection, shard, to: node }
            });
        }
        if self.eat_kw("REBALANCE") {
            return Ok(Statement::Rebalance { collection: self.ident()? });
        }
        if self.eat_kw("BACKUP") {
            self.expect_kw("TO")?;
            return Ok(Statement::Backup { to: self.destination()? });
        }
        if self.eat_kw("RESTORE") {
            self.expect_kw("FROM")?;
            let (from, node, as_of) = self.backup_source()?;
            return Ok(Statement::Restore { from, node, as_of });
        }
        if self.eat_kw("VERIFY") {
            self.expect_kw("BACKUP")?;
            let (from, node, as_of) = self.backup_source()?;
            return Ok(Statement::VerifyBackup { from, node, as_of });
        }
        if self.eat_kw("EXPLAIN") {
            let analyze = self.eat_kw("ANALYZE");
            // A prefix that recurses into `statement` is recursive descent like
            // any other and takes the same counter. Without it a repeated
            // keyword overflows the stack, and a stack overflow aborts the
            // process rather than returning the error the caller would get from
            // every other malformed statement.
            self.enter()?;
            let inner = self.statement();
            self.leave();
            return Ok(Statement::Explain { analyze, inner: Box::new(inner?) });
        }
        if self.eat_kw("CREATE") {
            if self.eat_kw("COLLECTION") {
                return self.create_collection();
            }
            if self.eat_kw("INDEX") {
                return self.create_index();
            }
            if self.eat_kw("LIFECYCLE") {
                self.expect_kw("POLICY")?;
                return self.create_lifecycle();
            }
            return Err(Error::Sql(
                "expected COLLECTION, INDEX or LIFECYCLE POLICY after CREATE".into(),
            ));
        }
        if self.eat_kw("ALTER") {
            if self.eat_kw("COLLECTION") {
                let collection = self.ident()?;
                self.expect_kw("SET")?;
                let mut prefix_expansion = None;
                let mut nodes_of = None;
                for (key, v) in self.option_list()? {
                    match key.as_str() {
                        "prefix_expansion" => prefix_expansion = Some(cap_option(&key, v)?),
                        "nodes_of" => nodes_of = Some(name_option(&key, v)?),
                        "splits" => {
                            return Err(Error::Sql(
                                "splits are fixed when the collection is created and cannot \
                                 be altered"
                                    .into(),
                            ))
                        }
                        other => {
                            return Err(Error::Sql(format!("unknown collection option `{other}`")))
                        }
                    }
                }
                if prefix_expansion.is_none() && nodes_of.is_none() {
                    return Err(Error::Sql(
                        "ALTER COLLECTION ... SET names no option; it takes prefix_expansion \
                         and nodes_of"
                            .into(),
                    ));
                }
                return Ok(Statement::AlterCollection { collection, prefix_expansion, nodes_of });
            }
            if !self.eat_kw("INDEX") {
                return Err(Error::Sql("expected COLLECTION or INDEX after ALTER".into()));
            }
            let index = self.ident()?;
            self.expect_kw("ON")?;
            let collection = self.ident()?;
            self.expect_kw("SET")?;
            self.expect_kw("TIER")?;
            let tier = self.tier_name()?;
            return Ok(Statement::AlterIndexTier { collection, index, tier });
        }
        if self.eat_kw("DROP") {
            if self.eat_kw("COLLECTION") {
                return Ok(Statement::DropCollection { name: self.ident()? });
            }
            if self.eat_kw("INDEX") {
                let index = self.ident()?;
                self.expect_kw("ON")?;
                let collection = self.ident()?;
                return Ok(Statement::DropIndex { collection, index });
            }
            if !self.eat_kw("LIFECYCLE") {
                return Err(Error::Sql(
                    "expected COLLECTION, INDEX or LIFECYCLE POLICY after DROP".into(),
                ));
            }
            self.expect_kw("POLICY")?;
            return Ok(Statement::DropLifecyclePolicy { name: self.ident()? });
        }
        if self.eat_kw("RUN") {
            self.expect_kw("LIFECYCLE")?;
            let c = if self.eat_kw("ON") { Some(self.ident()?) } else { None };
            return Ok(Statement::RunLifecycle { collection: c });
        }
        if self.eat_kw("UNLOAD") {
            self.expect_kw("IDLE")?;
            let c = if self.eat_kw("ON") { Some(self.ident()?) } else { None };
            return Ok(Statement::UnloadIdle { collection: c });
        }
        if self.eat_kw("INSERT") {
            return self.insert();
        }
        if self.eat_kw("DELETE") {
            self.expect_kw("FROM")?;
            let collection = self.ident()?;
            let predicate = if self.eat_kw("WHERE") { Some(self.expr()?) } else { None };
            return Ok(Statement::Delete(DeleteStmt { collection, predicate }));
        }
        if self.eat_kw("SELECT") {
            return Ok(Statement::Select(Box::new(self.select()?)));
        }
        if self.eat_kw("FLUSH") {
            return Ok(Statement::Flush { collection: self.ident()? });
        }
        if self.eat_kw("COMPACT") {
            return Ok(Statement::Compact { collection: self.ident()? });
        }
        if self.eat_kw("MEASURE") {
            self.expect_kw("RECALL")?;
            self.expect_kw("ON")?;
            let collection = self.ident()?;
            let mut k = 10usize;
            let mut samples = 32usize;
            if self.eat_kw("WITH") {
                self.expect_punct("(")?;
                loop {
                    let key = self.ident()?;
                    self.expect_punct("=")?;
                    let v = self.literal()?;
                    match key.to_ascii_lowercase().as_str() {
                        "k" => k = positive_usize(&key, &v)?,
                        "samples" => samples = positive_usize(&key, &v)?,
                        other => return Err(Error::Sql(format!("unknown option `{other}`"))),
                    }
                    if !self.eat_punct(",") {
                        break;
                    }
                }
                self.expect_punct(")")?;
            }
            return Ok(Statement::MeasureRecall { collection, k, samples });
        }
        if self.eat_kw("SHOW") {
            if self.eat_kw("SEGMENTS") {
                return Ok(Statement::ShowSegments { collection: self.ident()? });
            }
            if self.eat_kw("CATALOG") {
                let c =
                    if matches!(self.peek(), Tok::Ident(_)) { Some(self.ident()?) } else { None };
                return Ok(Statement::ShowCatalog { collection: c });
            }
            if self.eat_kw("RESIDENCY") {
                let c =
                    if matches!(self.peek(), Tok::Ident(_)) { Some(self.ident()?) } else { None };
                return Ok(Statement::ShowResidency { collection: c });
            }
            if self.eat_kw("LIFECYCLE") {
                return Ok(Statement::ShowLifecycle);
            }
            return Err(Error::Sql(
                "expected SEGMENTS, CATALOG, RESIDENCY or LIFECYCLE after SHOW".into(),
            ));
        }
        Err(Error::Sql(format!("unexpected {} at start of statement", self.peek().describe())))
    }

    fn create_collection(&mut self) -> Result<Statement> {
        let name = self.ident()?;
        let mut columns = Vec::new();
        if self.eat_punct("(") {
            loop {
                if self.is_kw("PRIMARY") {
                    self.expect_kw("PRIMARY")?;
                    self.expect_kw("KEY")?;
                    self.expect_punct("(")?;
                    let p = self.path()?;
                    self.expect_punct(")")?;
                    match columns.iter_mut().find(|c: &&mut DeclaredColumn| c.path == p) {
                        Some(c) => {
                            c.primary_key = true;
                            c.not_null = true;
                        }
                        // Dropping the constraint here would leave the engine to
                        // default the key to `id`, silently keying the collection
                        // on a field no document has.
                        None => {
                            return Err(Error::Sql(format!(
                                "PRIMARY KEY (`{p}`) names a column that was not declared"
                            )))
                        }
                    }
                } else {
                    let path = self.path()?;
                    let ty = self.type_name()?;
                    let mut not_null = false;
                    let mut primary_key = false;
                    loop {
                        if self.eat_kw("PRIMARY") {
                            self.expect_kw("KEY")?;
                            primary_key = true;
                            not_null = true;
                        } else if self.eat_kw("NOT") {
                            self.expect_kw("NULL")?;
                            not_null = true;
                        } else {
                            break;
                        }
                    }
                    columns.push(DeclaredColumn { path, ty, not_null, primary_key });
                }
                if !self.eat_punct(",") {
                    break;
                }
            }
            self.expect_punct(")")?;
        }
        let mut partition_by = None;
        if self.eat_kw("PARTITION") {
            self.expect_kw("BY")?;
            let paren = self.eat_punct("(");
            partition_by = Some(self.path()?);
            if paren {
                self.expect_punct(")")?;
            }
        }
        let mut splits = Vec::new();
        let mut prefix_expansion = None;
        let mut nodes = Vec::new();
        let mut nodes_of = None;
        let mut undirected = false;
        if self.eat_kw("WITH") {
            for (key, v) in self.option_list()? {
                match key.as_str() {
                    "nodes_of" => nodes_of = Some(name_option(&key, v)?),
                    "undirected" => undirected = bool_option(&key, Some(v))?,
                    "splits" => {
                        splits = v
                            .as_array()
                            .ok_or_else(|| Error::Sql("splits must be an array of keys".into()))?
                            .iter()
                            .map(|x| match x {
                                Value::Str(s) => s.clone(),
                                other => crate::json::to_string(other),
                            })
                            .collect()
                    }
                    "prefix_expansion" => prefix_expansion = Some(cap_option(&key, v)?),
                    "nodes" => {
                        nodes = v
                            .as_array()
                            .ok_or_else(|| {
                                Error::Sql("nodes must be an array of 'tcp://host:port'".into())
                            })?
                            .iter()
                            .map(|x| match x {
                                Value::Str(s) => Ok(s.clone()),
                                other => Err(Error::Sql(format!(
                                    "nodes must be an array of 'tcp://host:port', not {}",
                                    crate::json::to_string(other)
                                ))),
                            })
                            .collect::<Result<Vec<String>>>()?
                    }
                    other => {
                        return Err(Error::Sql(format!("unknown collection option `{other}`")))
                    }
                }
            }
        }
        Ok(Statement::CreateCollection(CreateCollection {
            name,
            columns,
            partition_by,
            splits,
            prefix_expansion,
            nodes,
            nodes_of,
            undirected,
        }))
    }

    /// `( key = literal, ... )`: the option list `CREATE COLLECTION ... WITH`
    /// and `ALTER COLLECTION ... SET` share. Keys come back lower-cased.
    fn option_list(&mut self) -> Result<Vec<(String, Value)>> {
        self.expect_punct("(")?;
        let mut out = Vec::new();
        loop {
            let key = self.ident()?.to_ascii_lowercase();
            self.expect_punct("=")?;
            out.push((key, self.literal()?));
            if !self.eat_punct(",") {
                break;
            }
        }
        self.expect_punct(")")?;
        Ok(out)
    }

    fn type_name(&mut self) -> Result<ValueType> {
        let t = self.ident()?;
        Ok(match t.to_ascii_uppercase().as_str() {
            "TEXT" | "STRING" | "VARCHAR" => ValueType::Str,
            "INT" | "INTEGER" | "BIGINT" | "DOUBLE" | "FLOAT" | "NUMERIC" | "NUMBER" => {
                ValueType::Number
            }
            "BOOL" | "BOOLEAN" => ValueType::Bool,
            "TIMESTAMP" => ValueType::Timestamp,
            "ARRAY" => ValueType::Array,
            "JSON" | "OBJECT" => ValueType::Object,
            other => return Err(Error::Sql(format!("unknown type `{other}`"))),
        })
    }

    fn create_index(&mut self) -> Result<Statement> {
        let name = self.ident()?;
        self.expect_kw("ON")?;
        let collection = self.ident()?;
        self.expect_kw("USING")?;
        let kind = self.ident()?;
        self.expect_punct("(")?;
        let path = self.path()?;
        // `adjacency (src, dst)` names two columns; every other kind one.
        let second = if self.eat_punct(",") { Some(self.path()?) } else { None };
        self.expect_punct(")")?;
        let mut analyzer = "standard".to_string();
        let mut dims: Option<usize> = None;
        let mut metric = Metric::Cosine;
        let mut tier = Tier::default();
        if self.eat_kw("WITH") {
            self.expect_punct("(")?;
            loop {
                let key = self.ident()?;
                self.expect_punct("=")?;
                let v = self.literal()?;
                match key.to_ascii_lowercase().as_str() {
                    "analyzer" => {
                        analyzer = v
                            .as_str()
                            .ok_or_else(|| Error::Sql("analyzer must be a string".into()))?
                            .to_string()
                    }
                    "dims" | "dimensions" => dims = Some(dims_option(&key, &v)?),
                    "metric" => {
                        metric = Metric::parse(
                            v.as_str()
                                .ok_or_else(|| Error::Sql("metric must be a string".into()))?,
                        )?
                    }
                    "tier" => {
                        tier = Tier::parse(
                            v.as_str().ok_or_else(|| Error::Sql("tier must be a string".into()))?,
                        )?
                    }
                    other => return Err(Error::Sql(format!("unknown index option `{other}`"))),
                }
                if !self.eat_punct(",") {
                    break;
                }
            }
            self.expect_punct(")")?;
        }
        let spec = match kind.to_ascii_lowercase().as_str() {
            "adjacency" => IndexSpec::Adjacency {
                to: second.clone().ok_or_else(|| {
                    Error::Sql(
                        "an adjacency index names the column a hop probes and the one it \
                         reads: USING adjacency (src, dst)"
                            .into(),
                    )
                })?,
            },
            _ if second.is_some() => {
                return Err(Error::Sql(format!(
                    "a {kind} index is over one column; only adjacency takes two"
                )))
            }
            "fulltext" | "text" => IndexSpec::FullText { analyzer },
            "vector" => IndexSpec::Vector {
                dims: dims.ok_or_else(|| {
                    Error::Sql("a vector index requires WITH (dims = ...)".into())
                })?,
                metric,
            },
            "btree" | "secondary" => IndexSpec::Secondary,
            other => return Err(Error::Sql(format!("unknown index type `{other}`"))),
        };
        Ok(Statement::CreateIndex(CreateIndex { name, collection, path, spec, tier }))
    }

    /// A node address literal, `'tcp://host:port'`.
    fn node_address(&mut self) -> Result<String> {
        match self.literal()? {
            Value::Str(s) => Ok(s),
            other => Err(Error::Sql(format!(
                "a node address is a string like 'tcp://host:port', not {}",
                crate::json::to_string(&other)
            ))),
        }
    }

    fn tier_name(&mut self) -> Result<Tier> {
        match self.next() {
            Tok::Ident(s) => Tier::parse(&s),
            Tok::Str(s) => Tier::parse(&s),
            other => Err(Error::Sql(format!(
                "expected a tier (active, minimal, cached, archived), found {}",
                other.describe()
            ))),
        }
    }

    /// ```text
    /// CREATE LIFECYCLE POLICY <name> ON <collection> [FOR (i1, i2, ...)]
    ///   MOVE TO <tier> AFTER <n> <minutes|hours|days> [OF INACTIVITY | SINCE CREATION]
    ///   [, ...]
    /// ```
    fn create_lifecycle(&mut self) -> Result<Statement> {
        let name = self.ident()?;
        self.expect_kw("ON")?;
        let collection = self.ident()?;
        let mut indexes = Vec::new();
        if self.eat_kw("FOR") {
            self.expect_punct("(")?;
            loop {
                indexes.push(self.ident()?);
                if !self.eat_punct(",") {
                    break;
                }
            }
            self.expect_punct(")")?;
        }
        let mut rules = Vec::new();
        loop {
            self.expect_kw("MOVE")?;
            self.expect_kw("TO")?;
            let to = self.tier_name()?;
            self.expect_kw("AFTER")?;
            let n = match self.literal()?.as_i64() {
                Some(v) if v > 0 => v as u64,
                _ => {
                    return Err(Error::Sql(
                        "a lifecycle duration must be a positive integer".into(),
                    ))
                }
            };
            let unit = Unit::parse(&self.ident()?)?;
            // `OF INACTIVITY` is the default; `SINCE CREATION` is the other
            // question you might be asking.
            let trigger = if self.eat_kw("SINCE") {
                // `SINCE CREATION` is retention; `SINCE ACCESS` is a synonym for
                // the default `OF INACTIVITY`. One `SINCE` decides between them,
                // so both spellings have to be handled here — a second
                // `eat_kw("SINCE")` further down can never match.
                if self.eat_kw("ACCESS") {
                    Trigger::Inactivity
                } else {
                    self.expect_kw("CREATION")?;
                    Trigger::SinceCreation
                }
            } else {
                if self.eat_kw("OF") {
                    self.expect_kw("INACTIVITY")?;
                }
                Trigger::Inactivity
            };
            rules.push(Rule { to, after: Every::new(n, unit)?, trigger });
            if !self.eat_punct(",") {
                break;
            }
        }
        if rules.is_empty() {
            return Err(Error::Sql("a lifecycle policy needs at least one MOVE TO rule".into()));
        }
        Ok(Statement::CreateLifecyclePolicy(LifecycleDecl { name, collection, indexes, rules }))
    }

    /// A backup destination: a quoted path or `s3://bucket/prefix`.
    /// `[NODE '<address>'] [AS OF <ts>]` after a backup's source, as RESTORE
    /// and VERIFY BACKUP take it.
    fn backup_source(&mut self) -> Result<(String, Option<String>, Option<u64>)> {
        let from = self.destination()?;
        let node = if self.eat_kw("NODE") {
            match self.literal()? {
                Value::Str(s) => Some(s),
                other => {
                    return Err(Error::Sql(format!(
                    "NODE wants the address a node backed up as, like 'tcp://host:port', not {}",
                    crate::json::to_string(&other)
                )))
                }
            }
        } else {
            None
        };
        let as_of = if self.eat_kw("AS") {
            self.expect_kw("OF")?;
            match self.literal()? {
                Value::Int(n) if n >= 0 => Some(n as u64),
                other => {
                    return Err(Error::Sql(format!(
                        "AS OF wants a backup's instant, the integer BACKUP reported, not {}",
                        crate::json::to_string(&other)
                    )))
                }
            }
        } else {
            None
        };
        Ok((from, node, as_of))
    }

    fn destination(&mut self) -> Result<String> {
        match self.literal()? {
            Value::Str(s) if !s.trim().is_empty() => Ok(s),
            other => Err(Error::Sql(format!(
                "a backup destination is a string like '/mnt/backups' or 's3://bucket/prefix', not {}",
                crate::json::to_string(&other)
            ))),
        }
    }

    fn insert(&mut self) -> Result<Statement> {
        self.expect_kw("INTO")?;
        let collection = self.ident()?;
        self.expect_kw("VALUES")?;
        let mut docs = Vec::new();
        loop {
            let wrapped = self.eat_punct("(");
            let v = self.literal()?;
            if wrapped {
                self.expect_punct(")")?;
            }
            // A document arrives either as a JSON string or as a bound
            // parameter that is already a value.
            docs.push(match v {
                Value::Str(s) => crate::json::parse(&s)?,
                other => other,
            });
            if !self.eat_punct(",") {
                break;
            }
        }
        Ok(Statement::Insert(Insert { collection, docs }))
    }

    fn select(&mut self) -> Result<Select> {
        let mut projections = Vec::new();
        loop {
            if self.eat_punct("*") {
                projections.push(Projection::All);
            } else if self.is_kw("score") {
                self.i += 1;
                projections.push(Projection::Score);
            } else if self.is_kw("distance") {
                self.i += 1;
                projections.push(Projection::Distance);
            } else if let Some(func) = self.aggregate_call() {
                self.i += 2;
                let path = if func == AggFunc::Count && self.eat_punct("*") {
                    None
                } else {
                    Some(self.path()?)
                };
                self.expect_punct(")")?;
                let alias = if self.eat_kw("AS") { Some(self.ident()?) } else { None };
                projections.push(Projection::Aggregate { func, path, alias });
            } else {
                let path = self.path()?;
                let alias = if self.eat_kw("AS") { Some(self.ident()?) } else { None };
                projections.push(Projection::Path { path, alias });
            }
            if !self.eat_punct(",") {
                break;
            }
        }
        self.expect_kw("FROM")?;
        let collection = self.ident()?;
        let predicate = if self.eat_kw("WHERE") { Some(self.expr()?) } else { None };
        let group_by = if self.eat_kw("GROUP") {
            self.expect_kw("BY")?;
            Some(self.path()?)
        } else {
            None
        };
        // An aggregate list is all aggregates, plus the grouped path if there
        // is one: a bare path beside a `count(*)` has no row to be read
        // from, and saying so here names the path.
        if projections.iter().any(|p| matches!(p, Projection::Aggregate { .. }))
            || group_by.is_some()
        {
            for p in &projections {
                match p {
                    Projection::Aggregate { .. } => {}
                    Projection::Path { path, .. } if group_by.as_deref() == Some(path.as_str()) => {
                    }
                    Projection::Path { path, .. } => {
                        return Err(Error::Sql(format!(
                            "`{path}` is neither aggregated nor the GROUP BY path"
                        )))
                    }
                    Projection::All => {
                        return Err(Error::Sql("`*` cannot be listed beside an aggregate".into()))
                    }
                    Projection::Score | Projection::Distance => {
                        return Err(Error::Sql(
                            "score and distance are per row; an aggregate has none".into(),
                        ))
                    }
                }
            }
        }

        let mut order = None;
        if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            order = Some(self.order_by()?);
        }
        // The tail clauses are accepted in any order. `COLLAPSE BY` reads
        // naturally on either side of `LIMIT`, and rejecting one of the two
        // orderings would be a rule with no purpose behind it.
        let mut collapse = None;
        let mut limit = None;
        let mut offset = 0usize;
        let mut cursor = None;
        let mut seen: Vec<&str> = Vec::new();
        let once = |seen: &mut Vec<&str>, name: &'static str| -> Result<()> {
            if seen.contains(&name) {
                // Taking the last silently is how `LIMIT 5 LIMIT 1` returns one
                // row and nobody notices the typo.
                return Err(Error::Sql(format!("`{name}` appears more than once")));
            }
            seen.push(name);
            Ok(())
        };
        loop {
            if self.eat_kw("COLLAPSE") {
                once(&mut seen, "COLLAPSE BY")?;
                self.expect_kw("BY")?;
                collapse = Some(self.path()?);
            } else if self.eat_kw("LIMIT") {
                once(&mut seen, "LIMIT")?;
                limit = Some(self.usize_literal()?);
            } else if self.eat_kw("OFFSET") {
                once(&mut seen, "OFFSET")?;
                offset = self.usize_literal()?;
            } else if self.eat_kw("AFTER") {
                once(&mut seen, "AFTER")?;
                cursor = Some(match self.literal()? {
                    Value::Str(s) => s,
                    other => crate::json::to_string(&other),
                });
            } else {
                break;
            }
        }
        let with = self.with_opts()?;

        // `LIMIT` is required with `hybrid()`: without it the query is a full
        // scan (§2.4). Rejecting it here rather than in the planner means the
        // error names the clause the user forgot.
        if matches!(order, Some(OrderBy::Hybrid(_))) && limit.is_none() {
            return Err(Error::Sql(
                "hybrid() requires LIMIT — without one the query is a full scan".into(),
            ));
        }
        Ok(Select {
            projections,
            collection,
            predicate,
            order,
            limit,
            offset,
            cursor,
            collapse,
            group_by,
            with,
        })
    }

    /// `count(`, `sum(`, ... at the cursor: the function, without consuming.
    fn aggregate_call(&self) -> Option<AggFunc> {
        let Tok::Ident(s) = self.peek() else { return None };
        let func = AggFunc::parse(s)?;
        matches!(self.t.get(self.i + 1), Some(Tok::Punct(p)) if *p == "(").then_some(func)
    }

    fn with_opts(&mut self) -> Result<WithOpts> {
        let mut w = WithOpts::default();
        if !self.eat_kw("WITH") {
            return Ok(w);
        }
        let paren = self.eat_punct("(");
        loop {
            let key = self.ident()?;
            let val = if self.eat_punct("=") { Some(self.literal()?) } else { None };
            match key.to_ascii_lowercase().as_str() {
                "exact" => w.exact = bool_option(&key, val)?,
                "exact_scoring" => w.exact_scoring = bool_option(&key, val)?,
                "partial_results" => w.partial_results = bool_option(&key, val)?,
                "ef_search" => {
                    let n = count_option(&key, val)?;
                    let n = usize::try_from(n)
                        .map_err(|_| Error::Sql("`ef_search` is too large".into()))?;
                    w.ef_search = Some(n);
                }
                "max_visits" => {
                    let n = count_option(&key, val)?;
                    let n = usize::try_from(n)
                        .map_err(|_| Error::Sql("`max_visits` is too large".into()))?;
                    w.max_visits = Some(n);
                }
                "max_frontier" => {
                    let n = count_option(&key, val)?;
                    let n = usize::try_from(n)
                        .map_err(|_| Error::Sql("`max_frontier` is too large".into()))?;
                    if n == 0 {
                        return Err(Error::Sql("`max_frontier` must be at least 1".into()));
                    }
                    w.max_frontier = Some(n);
                }
                "max_fanout" => {
                    let n = count_option(&key, val)?;
                    let n = usize::try_from(n)
                        .map_err(|_| Error::Sql("`max_fanout` is too large".into()))?;
                    if n == 0 {
                        return Err(Error::Sql("`max_fanout` must be at least 1".into()));
                    }
                    w.max_fanout = Some(n);
                }
                "deadline_ms" => w.deadline_ms = Some(count_option(&key, val)?),
                "no_deadline" => w.no_deadline = bool_option(&key, val)?,
                other => return Err(Error::Sql(format!("unknown WITH option `{other}`"))),
            }
            if !self.eat_punct(",") {
                break;
            }
        }
        if paren {
            self.expect_punct(")")?;
        }
        Ok(w)
    }

    fn usize_literal(&mut self) -> Result<usize> {
        match self.literal()?.as_i64() {
            Some(n) if n >= 0 => Ok(n as usize),
            _ => Err(Error::Sql("expected a non-negative integer".into())),
        }
    }

    fn order_by(&mut self) -> Result<OrderBy> {
        if self.is_kw("hybrid") {
            self.i += 1;
            self.expect_punct("(")?;
            return Ok(OrderBy::Hybrid(self.hybrid_args()?));
        }
        // `path <distance-op> literal` is an ANN access path, not a sort.
        let save = self.i;
        if let Ok(path) = self.path() {
            if let Some(op) = self.dist_op() {
                let q = self.vector_literal()?;
                // A trailing ASC is redundant but harmless; DESC on a distance
                // is a different query and is rejected rather than ignored.
                if self.eat_kw("DESC") {
                    return Err(Error::Sql(
                        "ORDER BY <distance> DESC is not an ANN access path; \
                         distances order ascending"
                            .into(),
                    ));
                }
                self.eat_kw("ASC");
                return Ok(OrderBy::Distance { path, op, query: q });
            }
        }
        self.i = save;
        let mut fields = Vec::new();
        loop {
            let p = self.path()?;
            let asc = if self.eat_kw("DESC") {
                false
            } else {
                self.eat_kw("ASC");
                true
            };
            fields.push((p, asc));
            if !self.eat_punct(",") {
                break;
            }
        }
        Ok(OrderBy::Fields(fields))
    }

    /// What follows `path WITHIN`: `k HOPS OF 'start' VIA edges [REVERSE]
    /// [WHERE term [THEN WHERE term]...]`. An edge filter is ONE term -- a
    /// comparison, a `NOT`, or a parenthesised predicate -- so that the
    /// `AND` after it belongs to the statement, where a reader expects it,
    /// and never to the walk; `THEN WHERE` gives the next hop its own.
    fn walk_clause(&mut self) -> Result<(usize, String, String, bool, Vec<Expr>)> {
        let k = self.usize_literal()?;
        if !self.eat_kw("HOPS") && !self.eat_kw("HOP") {
            return Err(Error::Sql(format!("expected `HOPS`, found {}", self.peek().describe())));
        }
        self.expect_kw("OF")?;
        let start = match self.literal()? {
            Value::Str(s) => s,
            other => crate::json::to_string(&other),
        };
        self.expect_kw("VIA")?;
        let via = self.ident()?;
        let reverse = self.eat_kw("REVERSE");
        let mut filters = Vec::new();
        if self.eat_kw("WHERE") {
            loop {
                self.enter()?;
                let f = self.not_expr();
                self.leave();
                filters.push(f?);
                if !self.eat_kw("THEN") {
                    break;
                }
                self.expect_kw("WHERE")?;
            }
        }
        Ok((k, start, via, reverse, filters))
    }

    fn dist_op(&mut self) -> Option<DistOp> {
        let op = match self.peek() {
            Tok::Punct("<->") => DistOp::L2,
            Tok::Punct("<=>") => DistOp::Cosine,
            Tok::Punct("<#>") => DistOp::InnerProduct,
            _ => return None,
        };
        self.i += 1;
        Some(op)
    }

    fn hybrid_args(&mut self) -> Result<HybridSpec> {
        let mut sources = Vec::new();
        let mut method = FusionMethod::Rrf;
        let mut weights: Vec<f32> = Vec::new();
        let mut k_prime = None;
        let mut rrf_c = 60.0f32;
        loop {
            if self.eat_punct(")") {
                break;
            }
            // Named argument?
            if let (Tok::Ident(name), Tok::Punct("=>")) =
                (self.peek().clone(), self.t.get(self.i + 1).cloned().unwrap_or(Tok::Eof))
            {
                self.i += 2;
                let v = self.literal()?;
                match name.to_ascii_lowercase().as_str() {
                    "method" => {
                        let name = v
                            .as_str()
                            .ok_or_else(|| Error::Sql("hybrid() method must be a string".into()))?;
                        method = match name.to_ascii_lowercase().as_str() {
                            "rrf" => FusionMethod::Rrf,
                            "linear" | "weighted" => FusionMethod::Linear,
                            other => {
                                return Err(Error::Sql(format!("unknown fusion method `{other}`")))
                            }
                        }
                    }
                    "weights" => {
                        weights = v
                            .as_array()
                            .ok_or_else(|| Error::Sql("weights must be an array".into()))?
                            .iter()
                            .map(|x| x.as_f64().unwrap_or(1.0) as f32)
                            .collect()
                    }
                    "k" | "k_prime" | "candidates" => {
                        // `as usize` on a negative would wrap to `usize::MAX`
                        // and become an allocation the size of the address
                        // space; zero would leave the scorer with an empty heap
                        // to index into.
                        let n = v.as_i64().unwrap_or(-1);
                        if n <= 0 {
                            return Err(Error::Sql(format!(
                                "hybrid() candidate depth must be positive, got {n}"
                            )));
                        }
                        k_prime = Some((n as usize).min(crate::plan::exec::MAX_K_PRIME))
                    }
                    "c" => rrf_c = v.as_f64().unwrap_or(60.0) as f32,
                    other => return Err(Error::Sql(format!("unknown hybrid() option `{other}`"))),
                }
            } else if self.is_kw("text_match") {
                self.i += 1;
                self.expect_punct("(")?;
                let path = self.path()?;
                self.expect_punct(",")?;
                let q = match self.literal()? {
                    Value::Str(s) => s,
                    other => crate::json::to_string(&other),
                };
                self.expect_punct(")")?;
                sources.push(HybridSource::Text { path, query: q });
            } else if self.is_kw("hops") {
                // `hops(id WITHIN 3 HOPS OF 'x' VIA cites [REVERSE] [WHERE ...])`
                self.i += 1;
                self.expect_punct("(")?;
                let path = self.path()?;
                self.expect_kw("WITHIN")?;
                let (k, start, via, reverse, filters) = self.walk_clause()?;
                self.expect_punct(")")?;
                sources.push(HybridSource::Hops { path, k, start, via, reverse, filters });
            } else {
                let path = self.path()?;
                let op = self
                    .dist_op()
                    .ok_or_else(|| Error::Sql("expected a distance operator in hybrid()".into()))?;
                let q = self.vector_literal()?;
                sources.push(HybridSource::Vector { path, op, query: q });
            }
            if !self.eat_punct(",") {
                self.expect_punct(")")?;
                break;
            }
        }
        if sources.is_empty() {
            return Err(Error::Sql("hybrid() needs at least one source".into()));
        }
        if weights.is_empty() {
            weights = vec![1.0; sources.len()];
        }
        if weights.len() != sources.len() {
            return Err(Error::Sql(format!(
                "hybrid() has {} sources but {} weights",
                sources.len(),
                weights.len()
            )));
        }
        Ok(HybridSpec { sources, method, weights, k_prime, rrf_c })
    }

    fn vector_literal(&mut self) -> Result<Vec<f32>> {
        let v = self.literal()?;
        let a = v
            .as_array()
            .ok_or_else(|| Error::Sql("expected a vector literal or parameter".into()))?;
        let mut out = Vec::with_capacity(a.len());
        for x in a {
            out.push(
                x.as_f64().ok_or_else(|| Error::Sql("vector elements must be numbers".into()))?
                    as f32,
            );
        }
        Ok(out)
    }

    // ----------------------------------------------------------- predicates

    fn expr(&mut self) -> Result<Expr> {
        let mut parts = vec![self.and_expr()?];
        while self.eat_kw("OR") {
            parts.push(self.and_expr()?);
        }
        Ok(if parts.len() == 1 { parts.pop().unwrap() } else { Expr::Or(parts) })
    }

    fn and_expr(&mut self) -> Result<Expr> {
        let mut parts = vec![self.not_expr()?];
        while self.eat_kw("AND") {
            parts.push(self.not_expr()?);
        }
        Ok(if parts.len() == 1 { parts.pop().unwrap() } else { Expr::And(parts) })
    }

    fn not_expr(&mut self) -> Result<Expr> {
        if self.eat_kw("NOT") {
            self.enter()?;
            let inner = self.not_expr();
            self.leave();
            return Ok(Expr::Not(Box::new(inner?)));
        }
        self.primary_expr()
    }

    fn primary_expr(&mut self) -> Result<Expr> {
        if self.eat_punct("(") {
            self.enter()?;
            let e = self.expr();
            self.leave();
            let e = e?;
            self.expect_punct(")")?;
            return Ok(e);
        }
        if self.is_kw("text_match") {
            self.i += 1;
            self.expect_punct("(")?;
            let path = self.path()?;
            self.expect_punct(",")?;
            let q = match self.literal()? {
                Value::Str(s) => s,
                other => crate::json::to_string(&other),
            };
            self.expect_punct(")")?;
            return Ok(Expr::TextMatch { path, query: q });
        }
        // `ANY(tags) = 'x'`
        if self.is_kw("ANY") {
            self.i += 1;
            self.expect_punct("(")?;
            let path = self.path()?;
            self.expect_punct(")")?;
            self.expect_punct("=")?;
            let lit = self.literal()?;
            return Ok(Expr::Compare { path, op: CmpOp::ArrayContains, lit });
        }
        let path = self.path()?;
        // `id WITHIN 2 HOPS OF 'p1' VIA cites [REVERSE] [WHERE kind = 'x']`:
        // a walk.
        if self.eat_kw("WITHIN") {
            let (k, start, via, reverse, filters) = self.walk_clause()?;
            return Ok(Expr::Hops { path, k, start, via, reverse, filters });
        }
        // `path <=> [..] < 0.2`: a distance threshold. The operator decides
        // the metric, exactly as it does under `ORDER BY`, and the
        // comparison that follows is an ordinary one against a number.
        if let Some(op) = self.dist_op() {
            let query = self.vector_literal()?;
            let cmp = self.comparison(&path)?;
            let threshold = match self.literal()? {
                Value::Int(i) => i as f64,
                Value::Float(f) => f,
                other => {
                    return Err(Error::Sql(format!(
                        "a distance threshold must be a number, found {}",
                        crate::json::to_string(&other)
                    )))
                }
            };
            if !threshold.is_finite() {
                return Err(Error::Sql("a distance threshold must be finite".into()));
            }
            return Ok(Expr::VectorDistance { path, op, query, cmp, threshold });
        }
        if self.eat_kw("IS") {
            let not = self.eat_kw("NOT");
            self.expect_kw("NULL")?;
            return Ok(Expr::Compare {
                path,
                op: if not { CmpOp::IsNotNull } else { CmpOp::IsNull },
                lit: Value::Null,
            });
        }
        if self.eat_kw("IN") {
            self.expect_punct("(")?;
            let mut items = Vec::new();
            loop {
                items.push(self.literal()?);
                if !self.eat_punct(",") {
                    break;
                }
            }
            self.expect_punct(")")?;
            return Ok(Expr::Compare { path, op: CmpOp::In, lit: Value::Array(items) });
        }
        if self.eat_kw("LIKE") {
            let lit = self.literal()?;
            let s = lit.as_str().unwrap_or("");
            if let Some(prefix) = s.strip_suffix('%') {
                if !prefix.contains('%') && !prefix.contains('_') {
                    return Ok(Expr::Compare {
                        path,
                        op: CmpOp::Prefix,
                        lit: Value::Str(prefix.to_string()),
                    });
                }
            }
            return Err(Error::Sql(
                "only prefix LIKE patterns ('abc%') are supported in v1".into(),
            ));
        }
        if self.eat_kw("CONTAINS") {
            let lit = self.literal()?;
            return Ok(Expr::Compare { path, op: CmpOp::ArrayContains, lit });
        }
        let op = self.comparison(&path)?;
        let lit = self.literal()?;
        Ok(Expr::Compare { path, op, lit })
    }

    /// One of the six orderings, or an error naming what was found instead.
    fn comparison(&mut self, path: &str) -> Result<CmpOp> {
        Ok(match self.next() {
            Tok::Punct("=") => CmpOp::Eq,
            Tok::Punct("<>") | Tok::Punct("!=") => CmpOp::Ne,
            Tok::Punct("<") => CmpOp::Lt,
            Tok::Punct("<=") => CmpOp::Le,
            Tok::Punct(">") => CmpOp::Gt,
            Tok::Punct(">=") => CmpOp::Ge,
            other => {
                return Err(Error::Sql(format!(
                    "expected a comparison operator after `{path}`, found {}",
                    other.describe()
                )))
            }
        })
    }

    // ------------------------------------------------------------- literals

    fn literal(&mut self) -> Result<Value> {
        // Array literals and unary minus recurse through here, so the cap that
        // protects predicates protects `[[[[...]]]]` too.
        self.enter()?;
        let v = self.literal_inner();
        self.leave();
        v
    }

    fn literal_inner(&mut self) -> Result<Value> {
        // `now()` and `now() ± interval '...'`.
        if self.is_kw("now") {
            self.i += 1;
            self.expect_punct("(")?;
            self.expect_punct(")")?;
            let mut micros = crate::time::now_micros();
            loop {
                let sign = if self.eat_punct("-") {
                    -1i64
                } else if self.eat_punct("+") {
                    1
                } else {
                    break;
                };
                let d = self.interval()?;
                micros = sign
                    .checked_mul(d)
                    .and_then(|x| micros.checked_add(x))
                    .ok_or_else(|| Error::Sql("timestamp arithmetic is out of range".into()))?;
            }
            return Ok(Value::Timestamp(micros));
        }
        if self.is_kw("timestamp") {
            self.i += 1;
            let s = match self.next() {
                Tok::Str(s) => s,
                other => {
                    return Err(Error::Sql(format!(
                        "expected a string after TIMESTAMP, found {}",
                        other.describe()
                    )))
                }
            };
            let m = crate::time::parse_iso8601(&s)
                .ok_or_else(|| Error::Sql(format!("`{s}` is not an ISO-8601 timestamp")))?;
            return Ok(Value::Timestamp(m));
        }
        if self.is_kw("interval") {
            return Ok(Value::Int(self.interval()?));
        }
        if self.eat_punct("[") {
            let mut items = Vec::new();
            if !self.eat_punct("]") {
                loop {
                    items.push(self.literal()?);
                    if !self.eat_punct(",") {
                        break;
                    }
                }
                self.expect_punct("]")?;
            }
            return Ok(Value::Array(items));
        }
        if self.eat_punct("-") {
            return Ok(match self.literal()? {
                // `-i64::MIN` has no answer: it panics where overflow checks
                // are on and wraps back to `i64::MIN` where they are not, so
                // the predicate compares against the value that was negated.
                Value::Int(i) => Value::Int(
                    i.checked_neg()
                        .ok_or_else(|| Error::Sql(format!("negating {i} is out of range")))?,
                ),
                Value::Float(f) => Value::Float(-f),
                other => return Err(Error::Sql(format!("cannot negate {}", other.ty().name()))),
            });
        }
        match self.next() {
            Tok::Str(s) => Ok(Value::Str(s)),
            Tok::Int(i) => Ok(Value::Int(i)),
            Tok::Float(f) => Ok(Value::Float(f)),
            Tok::Param(n) => {
                // Parameters are 1-based: `$0` would underflow the index and
                // wrap to `usize::MAX` in release.
                if n == 0 {
                    return Err(Error::Sql("parameter references start at $1".into()));
                }
                self.params
                    .get(n - 1)
                    .cloned()
                    .ok_or_else(|| Error::Sql(format!("parameter ${n} was not bound")))
            }
            Tok::Ident(s) if s.eq_ignore_ascii_case("true") => Ok(Value::Bool(true)),
            Tok::Ident(s) if s.eq_ignore_ascii_case("false") => Ok(Value::Bool(false)),
            Tok::Ident(s) if s.eq_ignore_ascii_case("null") => Ok(Value::Null),
            other => Err(Error::Sql(format!("expected a literal, found {}", other.describe()))),
        }
    }

    fn interval(&mut self) -> Result<i64> {
        self.expect_kw("interval")?;
        let s = match self.next() {
            Tok::Str(s) => s,
            other => {
                return Err(Error::Sql(format!(
                    "expected a string after INTERVAL, found {}",
                    other.describe()
                )))
            }
        };
        let mut it = s.split_whitespace();
        let n: i64 = it
            .next()
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| Error::Sql(format!("bad interval `{s}`")))?;
        let unit = it.next().unwrap_or("seconds").to_ascii_lowercase();
        let mult = match unit.trim_end_matches('s') {
            "microsecond" => 1i64,
            "millisecond" => 1_000,
            "second" => 1_000_000,
            "minute" => 60_000_000,
            "hour" => 3_600_000_000,
            "day" => 86_400_000_000,
            "week" => 604_800_000_000,
            other => return Err(Error::Sql(format!("unknown interval unit `{other}`"))),
        };
        // An overflow here wraps in release and silently reverses the sign of
        // the comparison, so an absurd interval quietly returns the wrong rows.
        n.checked_mul(mult).ok_or_else(|| Error::Sql(format!("interval `{s}` is out of range")))
    }
}

/// `WITH (partial_results)` is the bare-flag spelling, so an absent value means
/// true. A value that is present but not a boolean is a mistake: reading
/// `exact = 0` as true would set the flag to the opposite of what was written.
fn bool_option(key: &str, val: Option<Value>) -> Result<bool> {
    match val {
        None => Ok(true),
        Some(v) => v.as_bool().ok_or_else(|| Error::Sql(format!("`{key}` must be a boolean"))),
    }
}

/// A negative count would wrap to `usize::MAX` in the `as usize` cast, and the
/// recall harness then grows its query Vec until the process is killed; zero
/// asks for a measurement of nothing.
///
/// The upper bound is the other half of the same guard. The lexer turns an
/// integer too large for i64 into an `f64`, and `Value::as_i64` converts a
/// float with no fractional part back with a saturating cast, so `1e30` and
/// `i64::MAX` both arrive here as a positive count and are just as
/// unallocatable as the wrapped negative was.
const MAX_COUNT: i64 = 1 << 20;

fn positive_usize(key: &str, v: &Value) -> Result<usize> {
    match v.as_i64() {
        Some(n) if n > 0 && n <= MAX_COUNT => Ok(n as usize),
        _ => Err(Error::Sql(format!(
            "`{key}` must be a positive integer no larger than {MAX_COUNT}"
        ))),
    }
}

/// The largest `dims` a vector index will accept.
///
/// A dimension count is a per-vector allocation in the store, and the bare
/// `as usize` cast this replaces turned `-1` into `usize::MAX` and `1e30` into
/// `i64::MAX`; both reach `vec![0.0f32; n * dims]` and abort the process on
/// capacity overflow. Neither ever produced an index, so refusing them costs
/// nothing that worked. Real embeddings are orders of magnitude below the
/// bound.
const MAX_DIMS: i64 = 1 << 16;

fn dims_option(key: &str, v: &Value) -> Result<usize> {
    match v.as_i64() {
        Some(n) if n > 0 && n <= MAX_DIMS => Ok(n as usize),
        _ => {
            Err(Error::Sql(format!("`{key}` must be a positive integer no larger than {MAX_DIMS}")))
        }
    }
}

/// An integer `WITH` option.
///
/// Unlike `bool_option` there is no bare-flag spelling: `WITH (ef_search)` asks
/// for a value and supplies none, and dropping the bound on the floor is how a
/// tuning knob comes to be silently ignored. A negative value used to wrap
/// through `as usize` / `as u64` into the largest bound expressible, which is
/// the opposite of what was written.
fn count_option(key: &str, val: Option<Value>) -> Result<u64> {
    let v = val.ok_or_else(|| Error::Sql(format!("`{key}` requires a value")))?;
    match v.as_i64() {
        Some(n) if n >= 0 => Ok(n as u64),
        _ => Err(Error::Sql(format!("`{key}` must be a non-negative integer"))),
    }
}

/// `prefix_expansion`, for `CREATE COLLECTION ... WITH` and `ALTER COLLECTION
/// ... SET`: a count, as the other integer options are. Whether the engine can
/// honour it is the engine's decision, because the bound is its cache and not
/// the grammar's.
/// An option whose value names a collection.
fn name_option(key: &str, v: Value) -> Result<String> {
    match v {
        Value::Str(s) if !s.is_empty() => Ok(s),
        other => Err(Error::Sql(format!(
            "`{key}` names a collection, as a string, not {}",
            crate::json::to_string(&other)
        ))),
    }
}

fn cap_option(key: &str, v: Value) -> Result<usize> {
    let n = count_option(key, Some(v))?;
    usize::try_from(n).map_err(|_| Error::Sql(format!("`{key}` is out of range")))
}

#[cfg(test)]
mod tests {

    #[test]
    fn fuzz_sql_parsing_never_panics() {
        let samples = [
            "SELECT id, topic FROM notes WHERE topic = 'storage' AND n > 3 OR NOT (x IN ('a', 'b')) ORDER BY n DESC LIMIT 5 OFFSET 2",
            "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'a b*'), embedding <=> [0.5,0.5,0.0,0.0], method => 'rrf', weights => [1, 2]) LIMIT 3 WITH (partial_results, deadline_ms = 10)",
            "SELECT tenant, count(*) AS n, avg(w) FROM t WHERE id WITHIN 2 HOPS OF 'p1' VIA cites GROUP BY tenant ORDER BY n DESC",
            "CREATE COLLECTION notes (id TEXT PRIMARY KEY, topic TEXT NOT NULL) PARTITION BY (topic) WITH (splits = ['m', 't'])",
            "CREATE INDEX i ON notes USING vector (embedding) WITH (dims = 4, metric = 'cosine', tier = 'archived')",
            "INSERT INTO notes VALUES ('{\"id\":\"n1\",\"body\":\"x''y\"}'), ('{\"id\":\"n2\"}')",
            "DELETE FROM notes WHERE text_match(body, 'comp*') COLLAPSE BY parent",
            "CREATE LIFECYCLE POLICY p ON notes MOVE INDEX i TO 'archived' AFTER 30 days",
            "BACKUP TO 's3://b/p'; RESTORE FROM 'x' AS OF 12345 NODE 'tcp://a:2352'",
            "MOVE SHARD 1 OF notes TO 'tcp://h:2352'; ALTER INDEX i ON notes SET TIER 'active'",
        ];
        crate::fuzz::sweep_text(111, &samples, 6000, |t| {
            let _ = parse(t, &[Value::Int(1), Value::Str("p".into())]);
        });
    }
    use super::*;

    #[test]
    fn backup_and_restore_take_a_destination_and_restore_an_instant() {
        match parse("BACKUP TO '/mnt/backups'", &[]).unwrap() {
            Statement::Backup { to } => assert_eq!(to, "/mnt/backups"),
            other => panic!("{other:?}"),
        }
        match parse("restore from 's3://b/p' node 'tcp://a:1' as of 42", &[]).unwrap() {
            Statement::Restore { from, node, as_of } => {
                assert_eq!(
                    (from.as_str(), node.as_deref(), as_of),
                    ("s3://b/p", Some("tcp://a:1"), Some(42))
                )
            }
            other => panic!("{other:?}"),
        }
        match parse("RESTORE FROM 'nightly'", &[]).unwrap() {
            Statement::Restore { from, node, as_of } => {
                assert_eq!((from.as_str(), node, as_of), ("nightly", None, None))
            }
            other => panic!("{other:?}"),
        }
        assert!(parse("RESTORE FROM 'x' NODE 3", &[]).unwrap_err().to_string().contains("NODE"));
        assert!(parse("BACKUP TO 7", &[]).unwrap_err().to_string().contains("destination"));
        assert!(parse("RESTORE FROM '/x' AS OF 'now'", &[])
            .unwrap_err()
            .to_string()
            .contains("AS OF"));
        assert!(parse("BACKUP '/x'", &[]).is_err());
    }

    fn sel(sql: &str, params: &[Value]) -> Select {
        match parse(sql, params).unwrap() {
            Statement::Select(s) => *s,
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    #[test]
    fn aggregates_parse_with_their_names_and_the_group_by_path() {
        let s =
            sel("SELECT tenant, count(*), sum(doc.n) AS total, Avg(w) FROM t GROUP BY tenant", &[]);
        assert_eq!(s.group_by.as_deref(), Some("tenant"));
        assert!(s.aggregates());
        let names: Vec<String> = s.projections.iter().filter_map(|p| p.aggregate_name()).collect();
        assert_eq!(names, vec!["count(*)", "total", "avg(w)"]);
        assert!(matches!(
            &s.projections[1],
            Projection::Aggregate { func: AggFunc::Count, path: None, alias: None }
        ));
        // A path that happens to be named like a function is a path.
        let s = sel("SELECT count FROM t", &[]);
        assert!(matches!(&s.projections[0], Projection::Path { path, .. } if path == "count"));
        assert!(!s.aggregates());
        // Refused at parse time, naming the path.
        let e = parse("SELECT tenant, n, count(*) FROM t GROUP BY tenant", &[])
            .unwrap_err()
            .to_string();
        assert!(e.contains("`n` is neither aggregated"), "{e}");
        let e = parse("SELECT sum(*) FROM t", &[]).unwrap_err().to_string();
        assert!(e.contains("identifier"), "{e}");
    }

    #[test]
    fn a_repeated_explain_prefix_is_bounded_rather_than_overflowing_the_stack() {
        let sql = "EXPLAIN ".repeat(100_000) + "SELECT * FROM t";
        let e = parse(&sql, &[]).unwrap_err().to_string();
        assert!(e.contains("nests deeper"), "{e}");
        assert!(matches!(
            parse("EXPLAIN SELECT * FROM t", &[]).unwrap(),
            Statement::Explain { .. }
        ));
        assert!(matches!(
            parse("EXPLAIN ANALYZE SELECT * FROM t", &[]).unwrap(),
            Statement::Explain { analyze: true, .. }
        ));
    }

    #[test]
    fn out_of_range_counts_are_refused_rather_than_wrapping() {
        for bad in ["-1", "0", "1e30", "9223372036854775807"] {
            let sql = format!("CREATE INDEX i ON c USING vector (v) WITH (dims = {bad})");
            assert!(parse(&sql, &[]).is_err(), "accepted dims = {bad}");
            let sql = format!("MEASURE RECALL ON c WITH (k = {bad})");
            assert!(parse(&sql, &[]).is_err(), "accepted k = {bad}");
        }
        assert!(parse("CREATE INDEX i ON c USING vector (v) WITH (dims = 8)", &[]).is_ok());
        assert!(parse("MEASURE RECALL ON c WITH (k = 10, samples = 8)", &[]).is_ok());
    }

    #[test]
    fn a_with_option_that_takes_a_count_refuses_a_negative_or_missing_value() {
        for bad in ["ef_search = -1", "deadline_ms = -1", "ef_search", "deadline_ms = 'oops'"] {
            let sql = format!("SELECT * FROM c WITH ({bad})");
            assert!(parse(&sql, &[]).is_err(), "accepted {bad}");
        }
        // Zero is a meaningful `ef_search`: the search floors it at one.
        let s = sel("SELECT * FROM c WITH (max_visits = 64)", &[]);
        assert_eq!(s.with.max_visits, Some(64));
        let s = sel("SELECT * FROM c WITH (no_deadline)", &[]);
        assert!(s.with.no_deadline && s.with.deadline_ms.is_none());
        let s = sel("SELECT * FROM c WITH (ef_search = 0, deadline_ms = 50)", &[]);
        assert_eq!(s.with.ef_search, Some(0));
        assert_eq!(s.with.deadline_ms, Some(50));
    }

    #[test]
    fn negating_the_smallest_integer_is_refused_rather_than_wrapping() {
        let e = parse("SELECT * FROM c WHERE n = -$1", &[Value::Int(i64::MIN)]).unwrap_err();
        assert!(e.to_string().contains("out of range"), "{e}");
        let s = sel("SELECT * FROM c WHERE n = -$1", &[Value::Int(7)]);
        assert!(format!("{:?}", s.predicate).contains("Int(-7)"));
    }

    #[test]
    fn a_quoted_identifier_holding_a_dot_is_refused_rather_than_split() {
        let e = parse("SELECT * FROM c WHERE \"a.b\" = 1", &[]).unwrap_err();
        assert!(e.to_string().contains("quoted"), "{e}");
        // The unquoted dotted path and the quoted odd name both still parse.
        assert!(parse("SELECT * FROM c WHERE a.b = 1", &[]).is_ok());
        assert!(parse("SELECT * FROM c WHERE \"a b\" = 1", &[]).is_ok());
    }

    #[test]
    fn the_design_document_example_parses() {
        let params = vec![
            Value::Str("acme".into()),
            Value::Str("vector search".into()),
            Value::Array(vec![Value::Float(0.1), Value::Float(0.2), Value::Float(0.3)]),
        ];
        let s = sel(
            r#"SELECT id, title, doc.author.name
               FROM articles
               WHERE tenant_id = $1
                 AND status = 'published'
                 AND published_at > now() - interval '30 days'
               ORDER BY hybrid(
                   text_match(body, $2),
                   embedding <-> $3,
                   method => 'rrf'
                 )
               LIMIT 10"#,
            &params,
        );
        assert_eq!(s.collection, "articles");
        assert_eq!(s.limit, Some(10));
        // `doc.` is stripped: the path is the document path.
        assert!(
            matches!(&s.projections[2], Projection::Path { path, .. } if path == "author.name")
        );
        let Some(Expr::And(parts)) = &s.predicate else { panic!("{:?}", s.predicate) };
        assert_eq!(parts.len(), 3);
        let Some(OrderBy::Hybrid(h)) = &s.order else { panic!() };
        assert_eq!(h.sources.len(), 2);
        assert_eq!(h.method, FusionMethod::Rrf);
        assert_eq!(h.weights, vec![1.0, 1.0]);
    }

    #[test]
    fn hybrid_without_limit_is_rejected_by_name() {
        let e = parse("SELECT * FROM a ORDER BY hybrid(text_match(body, 'x'))", &[]).unwrap_err();
        assert!(format!("{e}").contains("LIMIT"), "{e}");
    }

    #[test]
    fn text_match_is_a_filter_in_where_and_a_source_in_hybrid() {
        let s = sel(
            "SELECT * FROM a WHERE text_match(body, 'must have') \
             ORDER BY hybrid(text_match(title, 'should have'), method => 'rrf') LIMIT 5",
            &[],
        );
        assert!(matches!(s.predicate, Some(Expr::TextMatch { .. })));
        let Some(OrderBy::Hybrid(h)) = &s.order else { panic!() };
        assert!(matches!(&h.sources[0], HybridSource::Text { path, .. } if path == "title"));
    }

    #[test]
    fn distance_order_is_an_access_path() {
        let s = sel("SELECT * FROM a ORDER BY emb <=> [1.0, 2.0] LIMIT 3", &[]);
        let Some(OrderBy::Distance { path, op, query }) = &s.order else { panic!() };
        assert_eq!(path, "emb");
        assert_eq!(*op, DistOp::Cosine);
        assert_eq!(query, &vec![1.0f32, 2.0]);
        // Descending distance is a different question, and saying so beats
        // silently sorting the wrong way.
        assert!(parse("SELECT * FROM a ORDER BY emb <=> [1.0] DESC LIMIT 3", &[]).is_err());
    }

    #[test]
    fn ddl_round_trips_the_documented_shape() {
        let Statement::CreateCollection(c) = parse(
            "CREATE COLLECTION articles (
               id TEXT PRIMARY KEY,
               tenant_id TEXT NOT NULL,
               published_at TIMESTAMP
             ) PARTITION BY (tenant_id)",
            &[],
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(c.name, "articles");
        assert_eq!(c.partition_by.as_deref(), Some("tenant_id"));
        assert!(c.columns[0].primary_key);
        assert!(c.columns[1].not_null);
        assert_eq!(c.columns[2].ty, ValueType::Timestamp);

        let Statement::CreateIndex(i) = parse(
            "CREATE INDEX articles_emb ON articles USING vector (embedding) \
             WITH (dims = 1536, metric = 'cosine')",
            &[],
        )
        .unwrap() else {
            panic!()
        };
        assert!(matches!(i.spec, IndexSpec::Vector { dims: 1536, metric: Metric::Cosine }));

        assert!(
            parse("CREATE INDEX e ON a USING vector (v) WITH (metric = 'cosine')", &[]).is_err()
        );
    }

    #[test]
    fn predicates_cover_arrays_null_in_and_prefix() {
        let s = sel(
            "SELECT * FROM a WHERE ANY(tags) = 'hot' AND status IN ('a','b') \
             AND deleted_at IS NULL AND slug LIKE 'intro-%'",
            &[],
        );
        let Some(Expr::And(p)) = &s.predicate else { panic!() };
        assert!(matches!(&p[0], Expr::Compare { op: CmpOp::ArrayContains, .. }));
        assert!(matches!(&p[1], Expr::Compare { op: CmpOp::In, .. }));
        assert!(matches!(&p[2], Expr::Compare { op: CmpOp::IsNull, .. }));
        assert!(
            matches!(&p[3], Expr::Compare { op: CmpOp::Prefix, lit: Value::Str(s), .. } if s == "intro-")
        );
        // A general LIKE is refused rather than half-supported.
        assert!(parse("SELECT * FROM a WHERE s LIKE '%mid%'", &[]).is_err());
    }

    #[test]
    fn with_options_and_pagination() {
        let s = sel(
            "SELECT * FROM a ORDER BY emb <-> [1.0] LIMIT 10 OFFSET 20 AFTER 'tenant/doc-9' \
             WITH (exact = true, ef_search = 400, partial_results)",
            &[],
        );
        assert!(s.with.exact);
        assert_eq!(s.with.ef_search, Some(400));
        assert!(s.with.partial_results);
        assert_eq!(s.offset, 20);
        assert_eq!(s.cursor.as_deref(), Some("tenant/doc-9"));
    }

    #[test]
    fn collapse_and_explain() {
        let s =
            sel("SELECT * FROM chunks ORDER BY emb <-> [1.0] LIMIT 10 COLLAPSE BY parent_id", &[]);
        assert_eq!(s.collapse.as_deref(), Some("parent_id"));
        let Statement::Explain { analyze, .. } =
            parse("EXPLAIN ANALYZE SELECT * FROM a LIMIT 1", &[]).unwrap()
        else {
            panic!()
        };
        assert!(analyze);
    }

    #[test]
    fn errors_name_what_was_wrong() {
        for (sql, needle) in [
            ("SELECT * FROM", "identifier"),
            ("SELECT * FROM a WHERE x", "comparison operator"),
            ("SELECT * FROM a ORDER BY hybrid(method => 'nope') LIMIT 1", "fusion method"),
            ("SELECT * FROM a LIMIT 1 WITH (nope = 1)", "unknown WITH option"),
            ("SELECT * FROM a LIMIT 5 LIMIT 1", "more than once"),
            // Sizes that would become allocations or empty heaps.
            ("SELECT * FROM a ORDER BY hybrid(text_match(b,'x'), k => 0) LIMIT 1", "positive"),
            ("SELECT * FROM a ORDER BY hybrid(text_match(b,'x'), k => -1) LIMIT 1", "positive"),
            // Arithmetic that would wrap and reverse a comparison.
            ("SELECT * FROM a WHERE t > now() - interval '9000000000000 days'", "out of range"),
            // Options that would silently fall back to a default.
            ("CREATE INDEX i ON a USING fulltext (b) WITH (analyzer = 5)", "must be a string"),
        ] {
            let e = parse(sql, &[]).unwrap_err().to_string();
            assert!(e.contains(needle), "`{sql}` gave `{e}`, expected to mention `{needle}`");
        }
    }

    #[test]
    fn param_zero_is_rejected_instead_of_underflowing_the_binding_index() {
        let e = parse("SELECT * FROM a WHERE x = $0", &[Value::Int(1)]).unwrap_err();
        assert!(e.to_string().contains("$1"), "{e}");
        let s = sel("SELECT * FROM a WHERE x = $1", &[Value::Int(7)]);
        assert!(matches!(s.predicate, Some(Expr::Compare { lit: Value::Int(7), .. })));
    }

    #[test]
    fn measure_recall_rejects_counts_that_would_wrap_to_a_huge_usize() {
        for sql in [
            "MEASURE RECALL ON t WITH (samples = -1)",
            "MEASURE RECALL ON t WITH (k = -1)",
            "MEASURE RECALL ON t WITH (k = 0)",
            "MEASURE RECALL ON t WITH (samples = 'lots')",
        ] {
            let e = parse(sql, &[]).unwrap_err().to_string();
            assert!(e.contains("positive"), "`{sql}` gave `{e}`");
        }
        let s = parse("MEASURE RECALL ON t WITH (k = 5, samples = 8)", &[]).unwrap();
        assert!(matches!(s, Statement::MeasureRecall { k: 5, samples: 8, .. }));
    }

    #[test]
    fn a_non_boolean_with_flag_errors_rather_than_turning_the_flag_on() {
        for sql in [
            "SELECT * FROM a WITH (exact = 0)",
            "SELECT * FROM a WITH (partial_results = 'false')",
            "SELECT * FROM a WITH (exact_scoring = 1)",
        ] {
            let e = parse(sql, &[]).unwrap_err().to_string();
            assert!(e.contains("must be a boolean"), "`{sql}` gave `{e}`");
        }
        // The bare-flag spelling has no value at all and still means true.
        assert!(sel("SELECT * FROM a WITH (exact)", &[]).with.exact);
        assert!(!sel("SELECT * FROM a WITH (exact = false)", &[]).with.exact);
    }

    #[test]
    fn a_primary_key_naming_an_undeclared_column_is_refused_not_dropped() {
        let e = parse("CREATE COLLECTION t (id TEXT, PRIMARY KEY (slug))", &[]).unwrap_err();
        let e = e.to_string();
        assert!(e.contains("slug") && e.contains("not declared"), "{e}");
        let c = match parse("CREATE COLLECTION t (id TEXT, PRIMARY KEY (id))", &[]).unwrap() {
            Statement::CreateCollection(c) => c,
            other => panic!("expected CREATE COLLECTION, got {other:?}"),
        };
        assert!(c.columns[0].primary_key && c.columns[0].not_null);
    }

    #[test]
    fn nesting_past_the_depth_cap_errors_instead_of_overflowing_the_stack() {
        let deep = format!("SELECT * FROM a WHERE {}x = 1{}", "(".repeat(300), ")".repeat(300));
        let e = parse(&deep, &[]).unwrap_err().to_string();
        assert!(e.contains("nests deeper"), "{e}");
        // Literals recurse through the same counter.
        let arr = format!("SELECT * FROM a WHERE x = {}1{}", "[".repeat(300), "]".repeat(300));
        let e = parse(&arr, &[]).unwrap_err().to_string();
        assert!(e.contains("nests deeper"), "{e}");
        // Nesting a human would write is unaffected.
        let ok = format!("SELECT * FROM a WHERE {}x = 1{}", "(".repeat(8), ")".repeat(8));
        assert!(parse(&ok, &[]).is_ok(), "{ok}");
    }
    /// `path <op> [..] <cmp> number` in WHERE is a predicate, not an order:
    /// the statement carries no `ORDER BY`, the leaf holds the operator, the
    /// vector, the comparison and the number, and it composes under AND like
    /// any other leaf. A threshold that is not a number is refused.
    #[test]
    fn a_distance_threshold_parses_as_a_predicate_and_not_as_an_order() {
        let sel = |sql: &str| match parse(sql, &[]).unwrap() {
            Statement::Select(s) => s,
            other => panic!("{other:?}"),
        };
        let s =
            sel("SELECT id FROM notes WHERE embedding <=> [1, 0] < 0.25 AND topic = 'a' LIMIT 5");
        assert!(s.order.is_none());
        let Some(Expr::And(parts)) = &s.predicate else { panic!("{:?}", s.predicate) };
        assert!(matches!(
            &parts[0],
            Expr::VectorDistance { path, op: DistOp::Cosine, query, cmp: CmpOp::Lt, threshold }
                if path == "embedding" && *query == vec![1.0, 0.0] && *threshold == 0.25
        ));
        let s = sel("SELECT id FROM notes WHERE embedding <-> [1, 0] = 0");
        assert!(matches!(
            &s.predicate,
            Some(Expr::VectorDistance { op: DistOp::L2, cmp: CmpOp::Eq, threshold, .. }) if *threshold == 0.0
        ));
        let s = sel("SELECT id FROM notes WHERE embedding <#> [1, 0] >= -0.5");
        assert!(matches!(
            &s.predicate,
            Some(Expr::VectorDistance { op: DistOp::InnerProduct, cmp: CmpOp::Ge, threshold, .. }) if *threshold == -0.5
        ));
        assert!(parse("SELECT id FROM notes WHERE embedding <=> [1, 0] < 'near'", &[]).is_err());
        assert!(parse("SELECT id FROM notes WHERE embedding <=> [1, 0] IN (0)", &[]).is_err());
    }

    /// The one dial a collection has after creation, spelled the way its
    /// creation options are: `ALTER COLLECTION ... SET (key = value)`. What is
    /// pinned is the grammar's half of the contract -- the number reaches the
    /// engine as a count, the splits cannot be re-set through this door, and
    /// an option that is not a count is refused here rather than as a zero.
    #[test]
    fn a_collection_s_prefix_expansion_is_set_at_creation_or_altered_later() {
        match parse("ALTER COLLECTION notes SET (prefix_expansion = 2048)", &[]).unwrap() {
            Statement::AlterCollection { collection, prefix_expansion, nodes_of } => {
                assert_eq!(collection, "notes");
                assert_eq!(prefix_expansion, Some(2048));
                assert_eq!(nodes_of, None);
            }
            other => panic!("{other:?}"),
        }
        match parse(
            "CREATE COLLECTION notes (id TEXT PRIMARY KEY) \
             WITH (splits = ['m'], PREFIX_EXPANSION = 1024)",
            &[],
        )
        .unwrap()
        {
            Statement::CreateCollection(c) => {
                assert_eq!(c.splits, vec!["m".to_string()], "the other option still parses");
                assert_eq!(c.prefix_expansion, Some(1024), "and the key is case-insensitive");
            }
            other => panic!("{other:?}"),
        }
        match parse("CREATE COLLECTION notes (id TEXT PRIMARY KEY)", &[]).unwrap() {
            Statement::CreateCollection(c) => assert_eq!(c.prefix_expansion, None),
            other => panic!("{other:?}"),
        }
        for (sql, why) in [
            ("ALTER COLLECTION notes SET (splits = ['m'])", "splits"),
            ("ALTER COLLECTION notes SET (prefix_expansion = 'wide')", "non-negative integer"),
            ("ALTER COLLECTION notes SET (prefix_expansion = 1.5)", "non-negative integer"),
            ("ALTER COLLECTION notes SET (colour = 1)", "unknown collection option"),
            ("ALTER COLLECTION notes SET ()", "expected"),
            ("ALTER TABLE notes SET (prefix_expansion = 1)", "COLLECTION or INDEX"),
            ("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (prefix_expansion = -1)", ""),
        ] {
            let e = parse(sql, &[]).unwrap_err().to_string();
            assert!(e.contains(why), "{sql}: {e}");
        }
    }

    /// The two DROP statements that used to be missing -- the DELETE refusal
    /// has said "or drop the collection" since before either existed.
    #[test]
    fn drop_collection_and_drop_index_parse_and_name_what_they_drop() {
        match parse("DROP COLLECTION notes", &[]).unwrap() {
            Statement::DropCollection { name } => assert_eq!(name, "notes"),
            other => panic!("{other:?}"),
        }
        match parse("drop index notes_body on notes", &[]).unwrap() {
            Statement::DropIndex { collection, index } => {
                assert_eq!((collection.as_str(), index.as_str()), ("notes", "notes_body"));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            parse("DROP LIFECYCLE POLICY p", &[]).unwrap(),
            Statement::DropLifecyclePolicy { .. }
        ));
        let e = parse("DROP TABLE notes", &[]).unwrap_err().to_string();
        assert!(e.contains("COLLECTION, INDEX or LIFECYCLE POLICY"), "{e}");
        let e = parse("DROP INDEX notes_body", &[]).unwrap_err().to_string();
        assert!(e.contains("ON"), "an index is named with its collection: {e}");
    }

    /// The three spellings the cluster adds: a `LOCAL` prefix on any
    /// statement, `ATTACH NODE` / `DETACH NODE` with a string address, and a
    /// `nodes` option on CREATE COLLECTION.
    #[test]
    fn cluster_statements_parse_and_name_their_nodes() {
        match parse("LOCAL CREATE INDEX i ON c USING fulltext (body)", &[]).unwrap() {
            Statement::Local(inner) => assert!(matches!(*inner, Statement::CreateIndex(_))),
            other => panic!("{other:?}"),
        }
        match parse("ATTACH NODE 'tcp://b:9000'", &[]).unwrap() {
            Statement::AttachNode { url } => assert_eq!(url, "tcp://b:9000"),
            other => panic!("{other:?}"),
        }
        match parse("detach node 'tcp://b:9000'", &[]).unwrap() {
            Statement::DetachNode { url } => assert_eq!(url, "tcp://b:9000"),
            other => panic!("{other:?}"),
        }
        match parse("MOVE SHARD 2 OF items TO 'tcp://c:9000'", &[]).unwrap() {
            Statement::MoveShard { collection, shard, to } => {
                assert_eq!((collection.as_str(), shard, to.as_str()), ("items", 2, "tcp://c:9000"));
            }
            other => panic!("{other:?}"),
        }
        match parse("PLACE SHARD 2 OF items ON 'tcp://c:9000'", &[]).unwrap() {
            Statement::PlaceShard { collection, shard, node } => {
                assert_eq!(
                    (collection.as_str(), shard, node.as_str()),
                    ("items", 2, "tcp://c:9000")
                );
            }
            other => panic!("{other:?}"),
        }
        match parse("LOCAL PLACE SHARD 0 OF items ON 'tcp://c:9000'", &[]).unwrap() {
            Statement::Local(inner) => {
                assert!(matches!(*inner, Statement::PlaceShard { shard: 0, .. }))
            }
            other => panic!("{other:?}"),
        }
        match parse("REBALANCE items", &[]).unwrap() {
            Statement::Rebalance { collection } => assert_eq!(collection, "items"),
            other => panic!("{other:?}"),
        }
        for (sql, why) in [
            ("MOVE SHARD 1 OF items TO 9000", "a node address is a string"),
            ("MOVE SHARD x OF items TO 'tcp://c:9000'", "expected a literal"),
            ("MOVE SHARD 1 items TO 'tcp://c:9000'", "expected `OF`"),
        ] {
            let e = parse(sql, &[]).unwrap_err().to_string();
            assert!(e.contains(why), "{sql}: {e}");
        }
        match parse("DETACH NODE 'tcp://b:9000'", &[]).unwrap() {
            Statement::DetachNode { url } => assert_eq!(url, "tcp://b:9000"),
            other => panic!("{other:?}"),
        }
        match parse(
            "CREATE COLLECTION c (id TEXT PRIMARY KEY) WITH (splits = ['m'], nodes = \
             ['tcp://a:1', 'tcp://b:1'])",
            &[],
        )
        .unwrap()
        {
            Statement::CreateCollection(c) => {
                assert_eq!(c.nodes, vec!["tcp://a:1".to_string(), "tcp://b:1".to_string()]);
                assert_eq!(c.splits, vec!["m".to_string()]);
            }
            other => panic!("{other:?}"),
        }
        for (sql, why) in [
            ("ATTACH NODE 9000", "string"),
            ("ATTACH SHARD 'x'", "NODE"),
            ("CREATE COLLECTION c (id TEXT PRIMARY KEY) WITH (nodes = 'tcp://a:1')", "array"),
        ] {
            let e = parse(sql, &[]).unwrap_err().to_string();
            assert!(e.contains(why), "{sql}: {e}");
        }
    }

    /// The walk is one predicate term beside the others, so it nests under
    /// `AND`, `OR` and `NOT` like any of them, and its edge filter is ONE
    /// term: the `AND` after it belongs to the statement. The caps are
    /// counts of at least one, the edge collection is declared with the
    /// node collection it points into, and the adjacency index names two
    /// columns where every other kind names one.
    #[test]
    fn a_walk_parses_as_a_filter_with_a_one_term_edge_filter() {
        let s = sel(
            "SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites WHERE kind = 'c' \
             AND text_match(body, 'graph') ORDER BY embedding <=> [1, 0] LIMIT 5 \
             WITH (max_frontier = 100, max_fanout = 8)",
            &[],
        );
        let Some(Expr::And(parts)) = &s.predicate else { panic!("{:?}", s.predicate) };
        assert_eq!(parts.len(), 2, "the AND after the edge filter is the statement's");
        match &parts[0] {
            Expr::Hops { path, k, start, via, reverse, filters } => {
                assert_eq!(
                    (path.as_str(), *k, start.as_str(), via.as_str(), *reverse),
                    ("id", 2, "p1", "cites", false)
                );
                assert!(
                    matches!(filters.as_slice(), [Expr::Compare { path, op: CmpOp::Eq, .. }] if path == "kind")
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(&parts[1], Expr::TextMatch { .. }));
        assert_eq!(s.with.max_frontier, Some(100));
        assert_eq!(s.with.max_fanout, Some(8));

        let s = sel("SELECT id FROM papers WHERE NOT (id WITHIN 1 HOP OF 'p1' VIA cites REVERSE WHERE (a = 1 OR b = 2))", &[]);
        let Some(Expr::Not(inner)) = &s.predicate else { panic!("{:?}", s.predicate) };
        match inner.as_ref() {
            Expr::Hops { k, reverse, filters, .. } => {
                assert_eq!((*k, *reverse), (1, true));
                assert!(matches!(filters.as_slice(), [Expr::Or(v)] if v.len() == 2));
            }
            other => panic!("{other:?}"),
        }

        // Per-hop filters, and the walk as a fusion source.
        let s = sel(
            "SELECT id FROM papers WHERE id WITHIN 3 HOPS OF 'p1' VIA cites WHERE kind = 'a' \
             THEN WHERE kind = 'b' THEN WHERE (kind = 'c' OR w > 1) AND n > 0 ORDER BY \
             hybrid(text_match(body, 'graph'), hops(id WITHIN 2 HOPS OF 'p2' VIA cites REVERSE \
             WHERE w > 1), method => 'linear') LIMIT 5",
            &[],
        );
        let Some(Expr::And(parts)) = &s.predicate else { panic!("{:?}", s.predicate) };
        match &parts[0] {
            Expr::Hops { k, filters, .. } => {
                assert_eq!(*k, 3);
                assert_eq!(filters.len(), 3, "one filter per hop");
                assert!(matches!(&filters[2], Expr::Or(_)));
            }
            other => panic!("{other:?}"),
        }
        assert!(
            matches!(&parts[1], Expr::Compare { path, .. } if path == "n"),
            "the AND is the statement's"
        );
        let Some(OrderBy::Hybrid(h)) = &s.order else { panic!("{:?}", s.order) };
        assert_eq!(h.sources.len(), 2);
        match &h.sources[1] {
            HybridSource::Hops { path, k, start, via, reverse, filters } => {
                assert_eq!(
                    (path.as_str(), *k, start.as_str(), via.as_str(), *reverse, filters.len()),
                    ("id", 2, "p2", "cites", true, 1)
                );
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(h.sources[1].name(), "hops(cites)");
        for (sql, why) in [
            ("SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1'", "expected `VIA`"),
            ("SELECT id FROM papers WHERE id WITHIN 2 OF 'p1' VIA cites", "expected `HOPS`"),
            ("SELECT id FROM papers WITH (max_frontier = 0)", "at least 1"),
            ("SELECT id FROM papers WITH (max_fanout = 0)", "at least 1"),
            ("CREATE INDEX x ON cites USING adjacency (src)", "USING adjacency (src, dst)"),
            ("CREATE INDEX x ON cites USING secondary (src, dst)", "over one column"),
            ("CREATE COLLECTION c (id TEXT PRIMARY KEY) WITH (nodes_of = 3)", "names a collection"),
        ] {
            let e = parse(sql, &[]).unwrap_err().to_string();
            assert!(e.contains(why), "{sql}: {e}");
        }
        match parse(
            "CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL) \
             WITH (nodes_of = 'papers', undirected = true)",
            &[],
        )
        .unwrap()
        {
            Statement::CreateCollection(c) => {
                assert_eq!(c.nodes_of.as_deref(), Some("papers"));
                assert!(c.undirected);
            }
            other => panic!("{other:?}"),
        }
        match parse("CREATE INDEX cites_adj ON cites USING adjacency (src, dst)", &[]).unwrap() {
            Statement::CreateIndex(c) => {
                assert_eq!(c.path, "src");
                assert!(matches!(c.spec, IndexSpec::Adjacency { ref to } if to == "dst"));
            }
            other => panic!("{other:?}"),
        }
        match parse("ALTER COLLECTION cites SET (nodes_of = 'papers')", &[]).unwrap() {
            Statement::AlterCollection { prefix_expansion, nodes_of, .. } => {
                assert_eq!((prefix_expansion, nodes_of.as_deref()), (None, Some("papers")));
            }
            other => panic!("{other:?}"),
        }
    }
}
