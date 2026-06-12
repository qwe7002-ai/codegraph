/**
 * PostgreSQL adapter (opt-in backend)
 *
 * Presents the SAME synchronous `SqliteDatabase` interface the rest of the
 * codebase relies on, but is backed by PostgreSQL via the async `pg` driver
 * running on a `worker_threads` Worker.
 *
 * The synchronous-over-async bridge: for every DB call the main thread posts
 * the (already SQLite→PG translated) statement to the worker over a
 * MessagePort, then BLOCKS in `Atomics.wait` on a shared Int32Array. The worker
 * runs the async query, posts the result back, and wakes the main thread with
 * `Atomics.notify`. The main thread drains the result with
 * `receiveMessageOnPort` — which works even though the event loop is blocked,
 * because it reads the port queue directly. (Node permits `Atomics.wait` on the
 * main thread, unlike browsers — this is the basis of the bridge.)
 *
 * NOTE: blocking the main thread is the cost of preserving the synchronous
 * call contract without rewriting every query site. The MCP server and file
 * watcher share this thread, so their event loops are paused for the duration
 * of each query. This backend is therefore opt-in (see `createDatabase`).
 *
 * Caveats vs. SQLite:
 *   - `iterate()` materializes the full result set (no server-side cursor over
 *     the sync bridge), so it loses the O(1)-memory property of the SQLite path.
 *   - `pragma()` is a no-op (PG has no PRAGMAs); `journal_mode` reports
 *     `'postgres'` so `codegraph status` stays informative.
 */

import * as fs from 'fs';
import * as path from 'path';
import {
  Worker,
  MessageChannel,
  receiveMessageOnPort,
  type MessagePort,
} from 'worker_threads';
import { SqliteDatabase, SqliteStatement } from './sqlite-adapter';
import { translateSql, fts5ToTsquery, TranslatedSql } from './pg-translate';

/** Per-call timeout (ms). Generous headroom for schema apply / large scans. */
const CALL_TIMEOUT_MS = 120_000;

interface WorkerResponse {
  rows?: any[];
  rowCount?: number;
  ok?: boolean;
  error?: string;
  code?: string;
}

function normalizeParam(v: unknown): unknown {
  // pg rejects `undefined`; SQLite/better-sqlite3 treat it as NULL.
  return v === undefined ? null : v;
}

class PgDatabaseAdapter implements SqliteDatabase {
  private worker: Worker;
  private mainPort: MessagePort;
  private lock: Int32Array;
  private _closed = false;
  private txDepth = 0;
  private fatalError: Error | null = null;

  constructor(connectionString: string) {
    const { port1, port2 } = new MessageChannel();
    this.mainPort = port1;
    const signal = new SharedArrayBuffer(4);
    this.lock = new Int32Array(signal);

    this.worker = new Worker(path.join(__dirname, 'pg-worker.js'), {
      workerData: { port: port2, signal, connectionString },
      transferList: [port2],
    });
    // Surface worker crashes; the flag is observed after a blocking call's
    // timeout (the handler can't run while the main thread is parked).
    this.worker.on('error', (err) => {
      this.fatalError = err instanceof Error ? err : new Error(String(err));
    });
    // Keep the process from being held open by the worker once we're done.
    this.worker.unref();
  }

  get open(): boolean {
    return !this._closed && this.fatalError === null;
  }

  /** Block the main thread until the worker answers the posted request. */
  private call(req: { op: 'query' | 'exec' | 'close'; sql?: string; params?: unknown[] }): WorkerResponse {
    if (this._closed) throw new Error('PostgreSQL connection is closed');
    if (this.fatalError) throw this.fatalError;

    Atomics.store(this.lock, 0, 0);
    this.mainPort.postMessage(req);

    const waitResult = Atomics.wait(this.lock, 0, 0, CALL_TIMEOUT_MS);
    if (waitResult === 'timed-out') {
      throw this.fatalError ?? new Error('PostgreSQL worker timed out (or crashed)');
    }

    // Drain the response. Defensive spin: in rare cases the notify can win the
    // race against cross-thread message delivery.
    let msg = receiveMessageOnPort(this.mainPort);
    let spins = 0;
    while (!msg && spins < 5_000_000) {
      msg = receiveMessageOnPort(this.mainPort);
      spins++;
    }
    if (!msg) throw new Error('PostgreSQL worker sent no response');

    const result = msg.message as WorkerResponse;
    if (result.error) {
      const err = new Error(result.error);
      (err as { code?: string }).code = result.code;
      throw err;
    }
    return result;
  }

  /** Build the positional `$n` param array from a translated statement. */
  private bindParams(t: TranslatedSql, rawArgs: unknown[]): unknown[] {
    let params: unknown[];
    if (t.named) {
      const obj = (rawArgs[0] ?? {}) as Record<string, unknown>;
      params = t.named.map((name) => normalizeParam(obj[name]));
    } else {
      params = rawArgs.map(normalizeParam);
    }
    if (t.ftsParamIndex !== null && t.ftsParamIndex < params.length) {
      params[t.ftsParamIndex] = fts5ToTsquery(String(params[t.ftsParamIndex] ?? ''));
    }
    return params;
  }

  private runTranslated(t: TranslatedSql, rawArgs: unknown[]): WorkerResponse {
    if (t.isNoop) return { rows: [], rowCount: 0 };
    return this.call({ op: 'query', sql: t.text, params: this.bindParams(t, rawArgs) });
  }

  prepare(sql: string): SqliteStatement {
    const t = translateSql(sql);
    const self = this;
    return {
      run(...params: any[]) {
        const r = self.runTranslated(t, params);
        return { changes: r.rowCount ?? 0, lastInsertRowid: 0 };
      },
      get(...params: any[]) {
        const r = self.runTranslated(t, params);
        return r.rows?.[0];
      },
      all(...params: any[]) {
        const r = self.runTranslated(t, params);
        return r.rows ?? [];
      },
      iterate(...params: any[]) {
        const r = self.runTranslated(t, params);
        return (r.rows ?? [])[Symbol.iterator]();
      },
    };
  }

  exec(sql: string): void {
    const t = translateSql(sql);
    if (t.isNoop) return;
    // exec is used for DDL / DELETE / VACUUM with no bound params.
    this.call({ op: 'exec', sql: t.text });
  }

  pragma(str: string, options?: { simple?: boolean }): any {
    const trimmed = str.trim();
    if (trimmed.includes('=')) return; // write pragma → no-op on PG
    if (/^journal_mode/i.test(trimmed)) {
      return options?.simple ? 'postgres' : [{ journal_mode: 'postgres' }];
    }
    return options?.simple ? undefined : [];
  }

  transaction<T>(fn: (...args: any[]) => T): (...args: any[]) => T {
    return (...args: any[]): T => {
      // Nested transaction() calls reuse the open transaction (the codebase
      // never needs savepoints); only the outermost issues BEGIN/COMMIT.
      if (this.txDepth > 0) return fn(...args);
      this.txDepth++;
      this.call({ op: 'exec', sql: 'BEGIN' });
      try {
        const result = fn(...args);
        this.call({ op: 'exec', sql: 'COMMIT' });
        return result;
      } catch (error) {
        try {
          this.call({ op: 'exec', sql: 'ROLLBACK' });
        } catch {
          /* ignore rollback failure; surface the original error */
        }
        throw error;
      } finally {
        this.txDepth--;
      }
    };
  }

  close(): void {
    if (this._closed) return;
    try {
      this.call({ op: 'close' });
    } catch {
      /* ignore */
    }
    this._closed = true;
    try {
      void this.worker.terminate();
    } catch {
      /* ignore */
    }
  }
}

/**
 * Resolve the PostgreSQL connection string. Honors `CODEGRAPH_PG_URL` first,
 * then the conventional `DATABASE_URL`. When neither is set, an empty string is
 * passed and `pg` falls back to the standard `PG*` environment variables
 * (`PGHOST`, `PGUSER`, `PGDATABASE`, …).
 */
export function resolvePgConnectionString(): string {
  return process.env.CODEGRAPH_PG_URL ?? process.env.DATABASE_URL ?? '';
}

/**
 * Create a PostgreSQL-backed database that satisfies the `SqliteDatabase`
 * interface. `dbPath` is the path CodeGraph would have used for the SQLite
 * file; we write a small presence marker there so the lifecycle checks in
 * `DatabaseConnection` (`fs.existsSync` on open, `fs.statSync` for size) keep
 * working unchanged — the real data lives in PostgreSQL.
 */
export function createPgDatabase(dbPath: string): SqliteDatabase {
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  if (!fs.existsSync(dbPath)) {
    fs.writeFileSync(
      dbPath,
      JSON.stringify(
        {
          backend: 'postgres',
          note: 'CodeGraph data is stored in PostgreSQL. This file is only a presence marker.',
        },
        null,
        2
      )
    );
  }
  return new PgDatabaseAdapter(resolvePgConnectionString());
}
