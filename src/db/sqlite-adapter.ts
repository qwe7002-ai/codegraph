/**
 * SQLite Adapter
 *
 * Thin wrapper over Node's built-in `node:sqlite` (`DatabaseSync`), exposed
 * through a small better-sqlite3-shaped interface so the rest of the codebase
 * is storage-agnostic.
 *
 * CodeGraph ships with a bundled Node runtime, so `node:sqlite` (real SQLite,
 * with WAL + FTS5) is always available — there is no native build step and no
 * wasm fallback. When run from source instead, it requires Node >= 22.5.
 */

export interface SqliteStatement {
  run(...params: any[]): { changes: number; lastInsertRowid: number | bigint };
  get(...params: any[]): any;
  all(...params: any[]): any[];
  /**
   * Lazily yield result rows one at a time instead of materializing the whole
   * set with `all()`. Use for unbounded scans (e.g. every function/method node)
   * so memory stays O(1) in the row count rather than O(rows) — see #610, where
   * `all()`-ing every symbol on a dense project spiked the heap into an OOM.
   */
  iterate(...params: any[]): IterableIterator<any>;
}

export interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
  pragma(str: string, options?: { simple?: boolean }): any;
  transaction<T>(fn: (...args: any[]) => T): (...args: any[]) => T;
  close(): void;
  readonly open: boolean;
}

/**
 * The active storage backend. `node-sqlite` is the default; `postgres` is an
 * opt-in PostgreSQL backend (see `createDatabase`). Kept as a named type so
 * `codegraph status` and the per-instance reporting have a stable shape.
 */
export type SqliteBackend = 'node-sqlite' | 'postgres';

/**
 * Wraps Node's built-in `node:sqlite` (`DatabaseSync`) to match the
 * better-sqlite3 interface the rest of the code expects.
 *
 * node:sqlite is real SQLite compiled into Node, so it supports WAL, FTS5,
 * mmap, and `@named` params natively — the only shims needed are the
 * better-sqlite3 conveniences node:sqlite omits: a `.pragma()` helper, a
 * `.transaction()` helper, and `open` (node:sqlite exposes `isOpen`).
 */
class NodeSqliteAdapter implements SqliteDatabase {
  private _db: any;

  constructor(dbPath: string) {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { DatabaseSync } = require('node:sqlite');
    this._db = new DatabaseSync(dbPath);
  }

  get open(): boolean {
    return this._db.isOpen;
  }

  prepare(sql: string): SqliteStatement {
    // node:sqlite matches better-sqlite3's calling convention (variadic
    // positional args, or a single object for @named params), so params forward
    // through unchanged.
    const stmt = this._db.prepare(sql);
    return {
      run(...params: any[]) {
        const r = stmt.run(...params);
        return {
          changes: Number(r?.changes ?? 0),
          lastInsertRowid: r?.lastInsertRowid ?? 0,
        };
      },
      get(...params: any[]) {
        return stmt.get(...params);
      },
      all(...params: any[]) {
        return stmt.all(...params);
      },
      iterate(...params: any[]) {
        return stmt.iterate(...params);
      },
    };
  }

  exec(sql: string): void {
    this._db.exec(sql);
  }

  pragma(str: string, options?: { simple?: boolean }): any {
    const trimmed = str.trim();
    // Write pragma ("key = value"): node:sqlite is real SQLite, so every pragma
    // (WAL, mmap, synchronous, …) applies as-is.
    if (trimmed.includes('=')) {
      this._db.exec(`PRAGMA ${trimmed}`);
      return;
    }
    // Read pragma. Default: the row object (e.g. { journal_mode: 'wal' }).
    // `{ simple: true }` returns just the single column value, like better-sqlite3.
    const row = this._db.prepare(`PRAGMA ${trimmed}`).get();
    if (options?.simple) {
      return row && typeof row === 'object' ? Object.values(row)[0] : row;
    }
    return row;
  }

  transaction<T>(fn: (...args: any[]) => T): (...args: any[]) => T {
    return (...args: any[]) => {
      this._db.exec('BEGIN');
      try {
        const result = fn(...args);
        this._db.exec('COMMIT');
        return result;
      } catch (error) {
        this._db.exec('ROLLBACK');
        throw error;
      }
    };
  }

  close(): void {
    // node:sqlite's DatabaseSync.close() throws if already closed; make it
    // idempotent to match better-sqlite3 (callers may close more than once).
    if (this._db.isOpen) this._db.close();
  }
}

/**
 * Create a database connection.
 *
 * Default backend is `node:sqlite`. Set `CODEGRAPH_DB_BACKEND=postgres` (or
 * `pg`) to use the opt-in PostgreSQL backend instead — configure it with
 * `CODEGRAPH_PG_URL` / `DATABASE_URL` (or the standard `PG*` env vars). The PG
 * backend speaks the same `SqliteDatabase` interface via on-the-fly dialect
 * translation, so no query-site code changes are needed.
 *
 * Returns the active backend alongside the db so each `DatabaseConnection` can
 * report it per-instance — MCP can open multiple project DBs in one process, so
 * a process-global would race.
 */
export function isPostgresBackendSelected(): boolean {
  const requested = (process.env.CODEGRAPH_DB_BACKEND ?? '').trim().toLowerCase();
  return requested === 'postgres' || requested === 'pg' || requested === 'postgresql';
}

/**
 * Detect the JSON presence marker `createPgDatabase` leaves at the SQLite db
 * path. Opening it with node:sqlite would fail with an opaque SQLITE_NOTADB
 * ("file is not a database"); this lets us explain the backend mismatch
 * instead.
 */
function isPgMarkerFile(dbPath: string): boolean {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const fs = require('fs') as typeof import('fs');
    if (!fs.existsSync(dbPath)) return false;
    const fd = fs.openSync(dbPath, 'r');
    try {
      const buf = Buffer.alloc(512);
      const n = fs.readSync(fd, buf, 0, buf.length, 0);
      const head = buf.toString('utf8', 0, n);
      return head.trimStart().startsWith('{') && /"backend"\s*:\s*"postgres"/.test(head);
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    return false;
  }
}

export function createDatabase(dbPath: string): { db: SqliteDatabase; backend: SqliteBackend } {
  if (isPostgresBackendSelected()) {
    // Lazily required so the `pg` dependency and worker are only loaded when
    // the PostgreSQL backend is actually selected.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createPgDatabase } = require('./pg-adapter');
    return { db: createPgDatabase(dbPath), backend: 'postgres' };
  }

  if (isPgMarkerFile(dbPath)) {
    throw new Error(
      'This project was indexed with the PostgreSQL backend, but CODEGRAPH_DB_BACKEND is not set. ' +
        'Set CODEGRAPH_DB_BACKEND=postgres (plus CODEGRAPH_PG_URL / DATABASE_URL) to use that data, ' +
        'or delete the .codegraph/ directory and re-run `codegraph init` to switch back to SQLite.'
    );
  }

  try {
    return { db: new NodeSqliteAdapter(dbPath), backend: 'node-sqlite' };
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    throw new Error(
      'Failed to open SQLite via the built-in node:sqlite module.\n' +
      'CodeGraph requires node:sqlite (Node.js 22.5+). Install the self-contained\n' +
      'CodeGraph release (it bundles a compatible Node), or run on Node 22.5+.\n' +
      `Underlying error: ${msg}`
    );
  }
}
