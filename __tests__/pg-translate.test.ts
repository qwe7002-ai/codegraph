/**
 * Tests for the SQLite → PostgreSQL dialect translation used by the opt-in
 * PostgreSQL backend. These exercise the pure translation functions; they do
 * NOT require a running PostgreSQL (the worker/adapter integration is gated
 * behind CODEGRAPH_DB_BACKEND=postgres at runtime).
 */

import { describe, it, expect } from 'vitest';
import { translateSql, fts5ToTsquery } from '../src/db/pg-translate';
import { createDatabase } from '../src/db/sqlite-adapter';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';

describe('pg-translate: placeholders', () => {
  it('converts positional ? to $n', () => {
    const t = translateSql('SELECT * FROM nodes WHERE id = ? AND kind = ?');
    expect(t.text).toBe('SELECT * FROM nodes WHERE id = $1 AND kind = $2');
    expect(t.named).toBeNull();
  });

  it('converts @named to $n and reports names in order, de-duplicating repeats', () => {
    const t = translateSql(
      'INSERT INTO files (path, language) VALUES (@path, @language) ON CONFLICT(path) DO UPDATE SET language = @language'
    );
    expect(t.named).toEqual(['path', 'language']);
    // @language appears twice but maps to the same $2.
    expect(t.text).toContain('VALUES ($1, $2)');
    expect(t.text).toContain('language = $2');
  });

  it('does not touch ? inside single-quoted string literals', () => {
    const t = translateSql("SELECT * FROM nodes WHERE name = 'a?b' AND kind = ?");
    expect(t.text).toBe("SELECT * FROM nodes WHERE name = 'a?b' AND kind = $1");
  });
});

describe('pg-translate: INSERT OR REPLACE / IGNORE', () => {
  it('maps INSERT OR REPLACE to ON CONFLICT DO UPDATE (excluding the key)', () => {
    const t = translateSql('INSERT OR REPLACE INTO nodes (id, name, kind) VALUES (@id, @name, @kind)');
    expect(t.text).toContain('INSERT INTO nodes');
    expect(t.text).toContain('ON CONFLICT (id) DO UPDATE SET');
    expect(t.text).toContain('name = EXCLUDED.name');
    expect(t.text).toContain('kind = EXCLUDED.kind');
    expect(t.text).not.toContain('id = EXCLUDED.id');
  });

  it('replicates SQLite REPLACE delete-cascade for nodes: dependent edges/refs are purged via CTEs', () => {
    // SQLite REPLACE = delete-then-insert; with foreign_keys=ON that CASCADEs
    // away the replaced node's edges and unresolved_refs. The PG upsert must
    // do the same or stale edges accumulate across re-upserts.
    const t = translateSql('INSERT OR REPLACE INTO nodes (id, name, kind) VALUES (@id, @name, @kind)');
    expect(t.text).toMatch(/^WITH _cg_replaced_edges AS \(DELETE FROM edges WHERE source = \$1 OR target = \$1\)/);
    expect(t.text).toContain('DELETE FROM unresolved_refs WHERE from_node_id = $1');
    // The CTE reuses the same @id binding, so `id` stays the first named param.
    expect(t.named?.[0]).toBe('id');
  });

  it('maps INSERT OR IGNORE to DO NOTHING', () => {
    const t = translateSql(
      'INSERT OR IGNORE INTO schema_versions (version, applied_at, description) VALUES (?, ?, ?)'
    );
    expect(t.text).toContain('ON CONFLICT DO NOTHING');
  });

  it('maps INSERT OR IGNORE on edges to targetless ON CONFLICT DO NOTHING', () => {
    // SQLite's OR IGNORE swallows ANY uniqueness violation; the targetless PG
    // form matches every unique constraint (including ones added later).
    const t = translateSql(
      'INSERT OR IGNORE INTO edges (source, target, kind) VALUES (@source, @target, @kind)'
    );
    expect(t.text).toContain('INSERT INTO edges');
    expect(t.text).toMatch(/ON CONFLICT DO NOTHING$/);
  });
});

describe('pg-translate: case-insensitivity', () => {
  it('rewrites COLLATE NOCASE (rhs) to lower()=lower()', () => {
    const t = translateSql('SELECT * FROM nodes WHERE name = ? COLLATE NOCASE');
    expect(t.text).toBe('SELECT * FROM nodes WHERE lower(name) = lower($1)');
  });

  it('rewrites COLLATE NOCASE (lhs) to lower()=lower()', () => {
    const t = translateSql('SELECT DISTINCT file_path FROM nodes WHERE name COLLATE NOCASE = ?');
    expect(t.text).toBe('SELECT DISTINCT file_path FROM nodes WHERE lower(name) = lower($1)');
  });

  it('rewrites LIKE / NOT LIKE to ILIKE / NOT ILIKE', () => {
    const t = translateSql('SELECT * FROM nodes WHERE name LIKE ? AND name NOT LIKE ?');
    expect(t.text).toBe('SELECT * FROM nodes WHERE name ILIKE $1 AND name NOT ILIKE $2');
  });
});

describe('pg-translate: json_each', () => {
  it('rewrites json_each(?) to json_array_elements_text(?::json)', () => {
    const t = translateSql(
      'SELECT * FROM edges WHERE source IN (SELECT value FROM json_each(?)) AND target IN (SELECT value FROM json_each(?))'
    );
    expect(t.text).toContain('json_array_elements_text($1::json)');
    expect(t.text).toContain('json_array_elements_text($2::json)');
  });
});

describe('pg-translate: no-ops', () => {
  it('marks PRAGMA statements as no-op', () => {
    expect(translateSql('PRAGMA optimize').isNoop).toBe(true);
    expect(translateSql('PRAGMA wal_checkpoint(PASSIVE)').isNoop).toBe(true);
  });

  it('marks the FTS5 virtual table bootstrap as no-op', () => {
    expect(
      translateSql('CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(id, name)').isNoop
    ).toBe(true);
  });
});

describe('pg-translate: FTS5 consistency', () => {
  it('rewrites the bm25/MATCH query to ts_rank over tsv with consistent weights', () => {
    const sql = `
      SELECT nodes.*, bm25(nodes_fts, 0, 20, 5, 1, 2) as score
      FROM nodes_fts
      JOIN nodes ON nodes_fts.id = nodes.id
      WHERE nodes_fts MATCH ?
       AND nodes.kind IN (?,?)
      ORDER BY score LIMIT ? OFFSET ?`;
    const t = translateSql(sql);
    // tsquery built from the FTS param, weights mirror name=A..docstring=D.
    expect(t.text).toContain("to_tsquery('simple', $1)");
    expect(t.text).toContain("ts_rank('{0.05,0.1,0.25,1.0}', nodes.tsv");
    expect(t.text).toContain('nodes.tsv @@ _ftsq.q');
    // bm25 ranks ascending; ts_rank ranks descending — must flip to DESC.
    expect(t.text).toContain('ORDER BY score DESC');
    expect(t.text).not.toContain('nodes_fts');
    // Param-position bookkeeping for the FTS MATCH arg.
    expect(t.ftsParamIndex).toBe(0);
    // Trailing filters keep their positions.
    expect(t.text).toContain('nodes.kind IN ($2,$3)');
    expect(t.text).toContain('LIMIT $4 OFFSET $5');
  });

  it('converts FTS5 prefix-OR query syntax to tsquery prefix-OR syntax', () => {
    expect(fts5ToTsquery('"auth"* OR "user"*')).toBe('auth:* | user:*');
    expect(fts5ToTsquery('"getUserById"*')).toBe('getUserById:*');
  });

  it('keeps dotted/hyphenated identifiers matchable (both whole-lexeme and split forms)', () => {
    // 'simple'-config tsvectors may hold 'App.tsx' as one lexeme (the default
    // parser's file/host token types) OR split; FTS5 always split and matched.
    // Gluing the pieces together ('Apptsx:*') would match NOTHING on PG.
    expect(fts5ToTsquery('"App.tsx"*')).toBe("('App.tsx':* | (App & tsx:*))");
    expect(fts5ToTsquery('"utils.parseConfig"* OR "auth"*')).toBe(
      "('utils.parseConfig':* | (utils & parseConfig:*)) | auth:*"
    );
  });

  it('returns a never-match sentinel for an empty FTS query', () => {
    expect(fts5ToTsquery('')).toBe('codegraph_no_match_sentinel');
  });

  it('throws loudly when the FTS query shape drifts from what the rewriter recognizes', () => {
    // Without this, drifted SQL would reach PG raw, error, get swallowed by
    // searchNodesFTS's catch, and PG search would silently return [] forever.
    expect(() =>
      translateSql('SELECT id, bm25(nodes_fts) AS rank FROM nodes_fts WHERE nodes_fts MATCH ?')
    ).toThrow(/FTS query shape not recognized/);
  });
});

describe('pg-schema invariants', () => {
  it('seeds the schema version from migrations.ts (cannot drift)', async () => {
    const { SEEDED_SCHEMA_VERSION, PG_SCHEMA } = await import('../src/db/pg-schema');
    const { CURRENT_SCHEMA_VERSION } = await import('../src/db/migrations');
    expect(SEEDED_SCHEMA_VERSION).toBe(CURRENT_SCHEMA_VERSION);
    expect(PG_SCHEMA).toContain(`VALUES (${CURRENT_SCHEMA_VERSION},`);
  });

  it('stores files.modified_at as DOUBLE PRECISION (mtimeMs is a fractional float)', async () => {
    // fs.Stats.mtimeMs is fractional on ns-precision filesystems; a BIGINT
    // column would reject it ('invalid input syntax for type bigint') and
    // break every file upsert.
    const { PG_SCHEMA } = await import('../src/db/pg-schema');
    expect(PG_SCHEMA).toMatch(/modified_at DOUBLE PRECISION NOT NULL/);
  });
});

describe('per-project PG schema naming', () => {
  it('derives a stable identifier from the db path and honors a recorded marker schema', async () => {
    const { derivePgSchemaName } = await import('../src/db/pg-adapter');
    const a = derivePgSchemaName('/proj/a/.codegraph/codegraph.db');
    const b = derivePgSchemaName('/proj/b/.codegraph/codegraph.db');
    expect(a).toMatch(/^codegraph_[0-9a-f]{16}$/);
    expect(a).not.toBe(b); // two projects must never share tables
    expect(derivePgSchemaName('/proj/a/.codegraph/codegraph.db')).toBe(a); // stable
    // A schema recorded in an existing marker wins over path derivation, so a
    // moved project keeps finding its data.
    expect(derivePgSchemaName('/proj/moved/.codegraph/codegraph.db', a)).toBe(a);
  });

  it('rejects a CODEGRAPH_PG_SCHEMA override that is not a plain identifier', async () => {
    const { derivePgSchemaName } = await import('../src/db/pg-adapter');
    const prev = process.env.CODEGRAPH_PG_SCHEMA;
    process.env.CODEGRAPH_PG_SCHEMA = 'bad-name; DROP SCHEMA public';
    try {
      expect(() => derivePgSchemaName('/x/.codegraph/codegraph.db')).toThrow(/identifier/);
    } finally {
      if (prev !== undefined) process.env.CODEGRAPH_PG_SCHEMA = prev;
      else delete process.env.CODEGRAPH_PG_SCHEMA;
    }
  });
});

describe('createDatabase backend dispatch', () => {
  it('defaults to node-sqlite when CODEGRAPH_DB_BACKEND is unset', () => {
    const prev = process.env.CODEGRAPH_DB_BACKEND;
    delete process.env.CODEGRAPH_DB_BACKEND;
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cg-pg-dispatch-'));
    try {
      const { db, backend } = createDatabase(path.join(dir, 'x.db'));
      expect(backend).toBe('node-sqlite');
      db.close();
    } finally {
      if (prev !== undefined) process.env.CODEGRAPH_DB_BACKEND = prev;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('explains the backend mismatch when SQLite is pointed at a PG presence marker', () => {
    // A project indexed with CODEGRAPH_DB_BACKEND=postgres leaves a JSON
    // marker at the SQLite db path. Opening it without the env var must say
    // what happened — not fail with an opaque SQLITE_NOTADB.
    const prev = process.env.CODEGRAPH_DB_BACKEND;
    delete process.env.CODEGRAPH_DB_BACKEND;
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cg-pg-marker-'));
    const dbPath = path.join(dir, 'codegraph.db');
    fs.writeFileSync(
      dbPath,
      JSON.stringify({ backend: 'postgres', schema: 'codegraph_abc', note: 'marker' }, null, 2)
    );
    try {
      expect(() => createDatabase(dbPath)).toThrow(/PostgreSQL backend.*CODEGRAPH_DB_BACKEND/s);
    } finally {
      if (prev !== undefined) process.env.CODEGRAPH_DB_BACKEND = prev;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
