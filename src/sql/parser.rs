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
                "a quoted identifier cannot contain `.`: `{s}` would be read as a nested path"
            )));
        }
        Ok(s)
    }

    // ---------------------------------------------------------------- stmts

    fn statement(&mut self) -> Result<Statement> {
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
            self.expect_kw("INDEX")?;
            let index = self.ident()?;
            self.expect_kw("ON")?;
            let collection = self.ident()?;
            self.expect_kw("SET")?;
            self.expect_kw("TIER")?;
            let tier = self.tier_name()?;
            return Ok(Statement::AlterIndexTier { collection, index, tier });
        }
        if self.eat_kw("DROP") {
            self.expect_kw("LIFECYCLE")?;
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
        if self.eat_kw("WITH") {
            self.expect_punct("(")?;
            loop {
                let key = self.ident()?;
                self.expect_punct("=")?;
                let v = self.literal()?;
                match key.to_ascii_lowercase().as_str() {
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
                    other => {
                        return Err(Error::Sql(format!("unknown collection option `{other}`")))
                    }
                }
                if !self.eat_punct(",") {
                    break;
                }
            }
            self.expect_punct(")")?;
        }
        Ok(Statement::CreateCollection(CreateCollection { name, columns, partition_by, splits }))
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
            with,
        })
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
                "deadline_ms" => w.deadline_ms = Some(count_option(&key, val)?),
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
        let op = match self.next() {
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
        };
        let lit = self.literal()?;
        Ok(Expr::Compare { path, op, lit })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sel(sql: &str, params: &[Value]) -> Select {
        match parse(sql, params).unwrap() {
            Statement::Select(s) => *s,
            other => panic!("expected SELECT, got {other:?}"),
        }
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
}
