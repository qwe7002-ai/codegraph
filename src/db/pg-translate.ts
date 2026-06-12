/**
 * SQLite → PostgreSQL dialect translation (opt-in PG backend)
 *
 * The rest of the codebase speaks SQLite. This module rewrites those
 * statements to PostgreSQL on the fly so the PG adapter is a drop-in behind
 * the same `SqliteDatabase` interface — no query-site code changes.
 *
 * What it handles (the full surface used by `queries.ts` / `migrations.ts` /
 * `index.ts`):
 *   - `?` positional and `@named` placeholders  → `$1..$n`
 *   - `INSERT OR REPLACE` / `INSERT OR IGNORE`   → `ON CONFLICT … DO UPDATE/NOTHING`
 *   - `COLLATE NOCASE` comparisons               → `lower(x) = lower(y)`
 *   - `LIKE`                                      → `ILIKE` (SQLite LIKE is
 *                                                   case-insensitive; PG's is not)
 *   - `json_each(?)`                              → `json_array_elements_text(?::json)`
 *   - the one FTS5 `bm25 … MATCH` query           → `ts_rank … tsv @@ to_tsquery`
 *     (see {@link rewriteFts} — kept behaviorally consistent with FTS5)
 *   - `PRAGMA …` and the SQLite bootstrap schema  → no-op (PG schema is applied
 *     by the worker on connect)
 */

export interface TranslatedSql {
  /** PostgreSQL text with `$n` placeholders. Empty when {@link isNoop}. */
  text: string;
  /**
   * For `@named` statements: the distinct parameter names in `$1..$n` order, so
   * the adapter can turn the bound object into a positional array. `null` for
   * positional (`?`) statements.
   */
  named: string[] | null;
  /**
   * 0-based index into the final params array of the FTS MATCH argument, whose
   * value must be converted from FTS5 query syntax to `tsquery` syntax at bind
   * time. `null` when the statement has no FTS MATCH.
   */
  ftsParamIndex: number | null;
  /** Statement should be skipped entirely (PRAGMA / SQLite bootstrap schema). */
  isNoop: boolean;
}

/** Tables whose `INSERT OR REPLACE/IGNORE` maps to a known conflict target. */
const CONFLICT_KEYS: Record<string, string> = {
  nodes: 'id',
  files: 'path',
  project_metadata: 'key',
  schema_versions: 'version',
};

/**
 * The FTS5 bm25 column weights are name=20, qualified_name=5, signature=2,
 * docstring=1. PG `ts_rank` takes weights for labels {D, C, B, A}; the schema
 * maps name→A, qualified_name→B, signature→C, docstring→D, so the same 1:2:5:20
 * ratio (normalized to /20) is {D:0.05, C:0.1, B:0.25, A:1.0}. This keeps name
 * matches dominating exactly as bm25 does.
 */
const TS_RANK_WEIGHTS = `'{0.05,0.1,0.25,1.0}'`;

/**
 * Convert an FTS5 query string (as built by `searchNodesFTS`, e.g.
 * `"auth"* OR "user"*`) into a PG `tsquery` string (`auth:* | user:*`).
 *
 * The two share semantics — per-term prefix match, OR-combined — so the
 * conversion is mechanical: split on ` OR `, strip the wrapping quotes and the
 * trailing `*`, re-emit each term as a `:*` prefix lexeme joined by ` | `.
 * Non-word characters are dropped (they are not valid bare tsquery lexemes and
 * `searchNodesFTS` already stripped FTS5 operators). Returns a never-match
 * sentinel for an empty result so `to_tsquery` doesn't error.
 */
export function fts5ToTsquery(fts5: string): string {
  const terms = fts5
    .split(/\s+OR\s+/i)
    .map((t) => t.replace(/["*]/g, '').trim())
    .filter((t) => t.length > 0);

  const parts: string[] = [];
  for (const term of terms) {
    const pieces = term.split(/[^\w]+/).filter((p) => p.length > 0);
    if (pieces.length === 0) continue;
    if (pieces.length === 1) {
      parts.push(`${pieces[0]}:*`);
      continue;
    }
    // Dotted/hyphenated identifiers ('App.tsx', 'utils.parseConfig'): the
    // 'simple'-config tsvector may hold them either as ONE lexeme (the default
    // parser's host/file token types keep 'app.tsx' whole) or as SPLIT lexemes
    // ('utils', 'parseconfig'). FTS5's tokenizer always splits, so the quoted
    // FTS5 term matched. Emit BOTH forms OR-ed so either indexing outcome
    // matches; mirror FTS5's phrase-prefix by prefixing only the last piece.
    const whole = term.replace(/[^\w.\-/@]/g, '');
    const split = pieces.map((p, i) => (i === pieces.length - 1 ? `${p}:*` : p)).join(' & ');
    if (whole.length > 0 && /[^\w]/.test(whole)) {
      parts.push(`('${whole}':* | (${split}))`);
    } else {
      parts.push(`(${split})`);
    }
  }
  if (parts.length === 0) return 'codegraph_no_match_sentinel';
  return parts.join(' | ');
}

/** Rewrite the single FTS5 bm25/MATCH query to the consistent PG ts_rank form. */
function rewriteFts(sql: string): string {
  const head =
    /SELECT\s+nodes\.\*,\s*bm25\([^)]*\)\s+as\s+score\s+FROM\s+nodes_fts\s+JOIN\s+nodes\s+ON\s+nodes_fts\.id\s*=\s*nodes\.id\s+WHERE\s+nodes_fts\s+MATCH\s+\?/is;
  const replaced = sql.replace(
    head,
    `WITH _ftsq AS (SELECT to_tsquery('simple', ?) AS q) ` +
      `SELECT nodes.*, ts_rank(${TS_RANK_WEIGHTS}, nodes.tsv, _ftsq.q) AS score ` +
      `FROM nodes, _ftsq ` +
      `WHERE nodes.tsv @@ _ftsq.q`
  );
  // Fail LOUDLY if the queries.ts FTS query drifted away from the shape the
  // regex above matches. Without this, the raw SQLite SQL ("FROM nodes_fts …
  // MATCH ?") would reach PostgreSQL, error there, and be swallowed by
  // searchNodesFTS's catch — PG search would silently return [] forever.
  if (/nodes_fts/i.test(replaced)) {
    throw new Error(
      '[codegraph] FTS query shape not recognized by the PostgreSQL translator — ' +
        'update rewriteFts in src/db/pg-translate.ts to match the query in queries.ts'
    );
  }
  // bm25 ranks ascending (more-negative = better); ts_rank ranks descending.
  return replaced.replace(/ORDER\s+BY\s+score\b(?!\s+DESC)/i, 'ORDER BY score DESC');
}

/** Turn `INSERT OR REPLACE/IGNORE` into the appropriate `ON CONFLICT` form. */
function rewriteInsertOrAction(sql: string): string {
  const m = /INSERT\s+OR\s+(REPLACE|IGNORE)\s+INTO\s+(\w+)\s*\(([^)]*)\)/i.exec(sql);
  if (!m) return sql;
  const verb = m[1]!.toUpperCase();
  const table = m[2]!;
  const cols = m[3]!.split(',').map((c) => c.trim()).filter(Boolean);
  const key = CONFLICT_KEYS[table];

  // Drop the "OR REPLACE/IGNORE" so it becomes a plain INSERT.
  let out = sql.replace(/INSERT\s+OR\s+(REPLACE|IGNORE)\s+INTO/i, 'INSERT INTO');

  if (verb === 'REPLACE') {
    if (!key) return out; // no known conflict target → best-effort plain insert
    const setList = cols
      .filter((c) => c !== key)
      .map((c) => `${c} = EXCLUDED.${c}`)
      .join(', ');
    out += ` ON CONFLICT (${key}) DO UPDATE SET ${setList}`;
    // SQLite's REPLACE is delete-then-insert: with foreign_keys=ON the delete
    // CASCADEs, purging the replaced node's edges and unresolved_refs before
    // the fresh row lands. PG's DO UPDATE keeps the row, so those dependents
    // would survive and accumulate across re-upserts (e.g. framework route
    // nodes re-emitted every resolution pass). Replicate the cascade with
    // data-modifying CTEs on the dependent tables (NOT on nodes itself, so the
    // upsert still sees the existing row). Uses the same @id placeholder the
    // statement already binds, so it only applies to the @named form.
    if (table === 'nodes' && /@id\b/.test(sql)) {
      out =
        `WITH _cg_replaced_edges AS (DELETE FROM edges WHERE source = @id OR target = @id), ` +
        `_cg_replaced_refs AS (DELETE FROM unresolved_refs WHERE from_node_id = @id) ` +
        out;
    }
  } else {
    // IGNORE: SQLite's OR IGNORE swallows ANY uniqueness violation, so use the
    // targetless form — it matches every unique constraint, including ones a
    // table doesn't have yet (a plain INSERT would start throwing duplicate-key
    // the day a constraint is added, and only on the PG backend).
    out += ` ON CONFLICT DO NOTHING`;
  }
  return out;
}

/** Convert `?` / `@named` placeholders to `$n`, skipping single-quoted strings. */
function convertPlaceholders(sql: string): { text: string; named: string[] | null } {
  let out = '';
  let i = 0;
  let n = 0;
  let sawPositional = false;
  const nameToIndex = new Map<string, number>();
  const named: string[] = [];

  while (i < sql.length) {
    const ch = sql[i]!;

    // Skip single-quoted string literals verbatim ('' is an escaped quote).
    if (ch === "'") {
      out += ch;
      i++;
      while (i < sql.length) {
        out += sql[i];
        if (sql[i] === "'") {
          if (sql[i + 1] === "'") {
            out += sql[i + 1];
            i += 2;
            continue;
          }
          i++;
          break;
        }
        i++;
      }
      continue;
    }

    if (ch === '?') {
      sawPositional = true;
      out += `$${++n}`;
      i++;
      continue;
    }

    if (ch === '@') {
      const rest = sql.slice(i + 1);
      const nameMatch = /^[A-Za-z_]\w*/.exec(rest);
      if (nameMatch) {
        const name = nameMatch[0];
        let idx = nameToIndex.get(name);
        if (idx === undefined) {
          idx = ++n;
          nameToIndex.set(name, idx);
          named.push(name);
        }
        out += `$${idx}`;
        i += 1 + name.length;
        continue;
      }
    }

    out += ch;
    i++;
  }

  // Positional and named never mix in this codebase.
  return { text: out, named: sawPositional ? null : named.length > 0 ? named : null };
}

/**
 * Memo cache for {@link translateSql}. The statement corpus is small and
 * mostly static (QueryBuilder re-prepares dynamic IN-clause queries per call),
 * so caching makes repeat prepares O(1) instead of a multi-pass regex rewrite.
 * Bounded defensively; entries are immutable so sharing is safe.
 */
const translateCache = new Map<string, TranslatedSql>();
const TRANSLATE_CACHE_MAX = 500;

/**
 * Translate one SQLite statement to PostgreSQL.
 */
export function translateSql(sqlInput: string): TranslatedSql {
  const cached = translateCache.get(sqlInput);
  if (cached) return cached;
  const result = translateSqlUncached(sqlInput);
  if (translateCache.size >= TRANSLATE_CACHE_MAX) translateCache.clear();
  translateCache.set(sqlInput, result);
  return result;
}

function translateSqlUncached(sqlInput: string): TranslatedSql {
  const sql = sqlInput.trim();

  // PRAGMA and the SQLite bootstrap schema (FTS5 virtual table) are no-ops:
  // the PG schema is applied by the worker on connect.
  if (/^PRAGMA\b/i.test(sql) || /USING\s+fts5/i.test(sql) || /CREATE\s+VIRTUAL\s+TABLE/i.test(sql)) {
    return { text: '', named: null, ftsParamIndex: null, isNoop: true };
  }

  const isFts = /nodes_fts\s+MATCH/i.test(sql);

  let work = sql;
  if (isFts) work = rewriteFts(work);
  if (/INSERT\s+OR\s+(REPLACE|IGNORE)/i.test(work)) work = rewriteInsertOrAction(work);

  // COLLATE NOCASE comparisons → case-insensitive via lower().
  work = work
    .replace(/(\w+)\s+COLLATE\s+NOCASE\s*=\s*(\?|@\w+)/gi, 'lower($1) = lower($2)')
    .replace(/(\w+)\s*=\s*(\?|@\w+)\s+COLLATE\s+NOCASE/gi, 'lower($1) = lower($2)');

  // SQLite LIKE is case-insensitive; PG LIKE is not — use ILIKE for parity.
  work = work.replace(/\bNOT\s+LIKE\b/gi, 'NOT ILIKE').replace(/\bLIKE\b/gi, 'ILIKE');

  // json_each(?) → json_array_elements_text(?::json) (default output col `value`).
  work = work.replace(/json_each\(\s*\?\s*\)/gi, 'json_array_elements_text(?::json)');

  const { text, named } = convertPlaceholders(work);

  return {
    text,
    named,
    // The FTS MATCH `?` is the first placeholder in the rewritten query, so it
    // lands at params[0]. Its value needs FTS5→tsquery conversion at bind time.
    ftsParamIndex: isFts ? 0 : null,
    isNoop: false,
  };
}
