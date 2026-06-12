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

  it('maps INSERT OR IGNORE with a known key to DO NOTHING', () => {
    const t = translateSql(
      'INSERT OR IGNORE INTO schema_versions (version, applied_at, description) VALUES (?, ?, ?)'
    );
    expect(t.text).toContain('ON CONFLICT (version) DO NOTHING');
  });

  it('maps INSERT OR IGNORE on edges to a plain INSERT (no unique key)', () => {
    const t = translateSql(
      'INSERT OR IGNORE INTO edges (source, target, kind) VALUES (@source, @target, @kind)'
    );
    expect(t.text).toContain('INSERT INTO edges');
    expect(t.text).not.toContain('ON CONFLICT');
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

  it('returns a never-match sentinel for an empty FTS query', () => {
    expect(fts5ToTsquery('')).toBe('codegraph_no_match_sentinel');
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
});
