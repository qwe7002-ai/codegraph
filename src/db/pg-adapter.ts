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
 * Every request carries a correlation `id` that the worker echoes back. The
 * drain loop discards any response whose id doesn't match the in-flight call,
 * so a response arriving after its call timed out can never be delivered as a
 * later call's answer (which would silently desync every subsequent result).
 *
 * NOTE: blocking the main thread is the cost of preserving the synchronous
 * call contract without rewriting every query site. The MCP server and file
 * watcher share this thread, so their event loops are paused for the duration
 * of each query. This backend is therefore opt-in (see `createDatabase`).
 *
 * Caveats vs. SQLite:
 *   - `iterate()` is chunked, not row-streamed: the worker materializes the
 *     full result set once and ships it to the main thread in fixed-size
 *     chunks, so main-thread memory stays O(chunk) but worker memory is
 *     O(rows) until the iterator is consumed or closed.
 *   - `pragma()` is a no-op (PG has no PRAGMAs); `journal_mode` reports
 *     `'postgres'` so `codegraph status` stays informative.
 */

import * as crypto from 'crypto';
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

/**
 * Upper bound for one Atomics.wait slice. Waiting in slices (instead of one
 * long wait) lets the drain loop re-poll the port, which covers the rare case
 * where the worker's notify lands while we're between drain and re-arm —
 * without the pathological busy-spin a pure polling loop would be.
 */
const WAIT_SLICE_MS = 100;

interface WorkerResponse {
  id?: number;
  rows?: any[];
  rowCount?: number;
  /** Cursor handle for chunked iterate(); present when more chunks remain. */
  cursor?: number;
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
  private seq = 0;

  constructor(connectionString: string, schemaName: string) {
    // Fail fast (and clearly) when the compiled worker isn't on disk — e.g.
    // running the TS sources via tsx/vitest, where __dirname holds only
    // pg-worker.ts. Without this check the Worker constructor fails async and
    // the first query would park the main thread for the full call timeout.
    const workerPath = path.join(__dirname, 'pg-worker.js');
    if (!fs.existsSync(workerPath)) {
      throw new Error(
        `PostgreSQL backend worker not found at ${workerPath} — the PG backend requires ` +
          'the compiled build (npm run build); it cannot run from TypeScript sources.'
      );
    }

    const { port1, port2 } = new MessageChannel();
    this.mainPort = port1;
    const signal = new SharedArrayBuffer(4);
    this.lock = new Int32Array(signal);

    this.worker = new Worker(workerPath, {
      workerData: { port: port2, signal, connectionString, schemaName },
      transferList: [port2],
    });
    // Surface worker crashes. These handlers can only run while the main
    // thread is NOT parked in Atomics.wait (event loop callbacks), so they
    // primarily make the NEXT call fail fast; a crash during a call is
    // reported by the worker's own uncaughtException responder.
    this.worker.on('error', (err) => {
      this.fatalError = err instanceof Error ? err : new Error(String(err));
    });
    this.worker.on('exit', (code) => {
      if (!this._closed && this.fatalError === null) {
        this.fatalError = new Error(`PostgreSQL worker exited unexpectedly (code ${code})`);
      }
    });
    // Keep the process from being held open by the worker once we're done.
    this.worker.unref();
  }

  get open(): boolean {
    return !this._closed && this.fatalError === null;
  }

  /** Block the main thread until the worker answers the posted request. */
  private call(req: {
    op: 'query' | 'exec' | 'fetch' | 'release' | 'close';
    sql?: string;
    params?: unknown[];
    chunk?: boolean;
    cursor?: number;
  }): WorkerResponse {
    if (this._closed) throw new Error('PostgreSQL connection is closed');
    if (this.fatalError) throw this.fatalError;

    const id = ++this.seq;
    Atomics.store(this.lock, 0, 0);
    this.mainPort.postMessage({ ...req, id });

    const deadline = Date.now() + CALL_TIMEOUT_MS;
    for (;;) {
      // Drain everything queued. Stale responses (a call that timed out
      // earlier finally answered) are discarded by the id check.
      let msg = receiveMessageOnPort(this.mainPort);
      while (msg !== undefined) {
        const resp = msg.message as WorkerResponse;
        if (resp.id === id) return this.unwrap(resp);
        msg = receiveMessageOnPort(this.mainPort);
      }

      if (this.fatalError) throw this.fatalError;
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw new Error(
          `PostgreSQL worker did not answer within ${CALL_TIMEOUT_MS}ms (slow query or crashed worker)`
        );
      }

      // Re-arm and wait. The wait is sliced so a notify that fired between the
      // drain above and this store is recovered on the next drain pass at most
      // WAIT_SLICE_MS later (the message itself is read via the port queue, so
      // nothing is lost — only the wakeup can race).
      Atomics.store(this.lock, 0, 0);
      Atomics.wait(this.lock, 0, 0, Math.min(remaining, WAIT_SLICE_MS));
    }
  }

  private unwrap(resp: WorkerResponse): WorkerResponse {
    if (resp.error) {
      const err = new Error(resp.error);
      (err as { code?: string }).code = resp.code;
      throw err;
    }
    return resp;
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
    if (t.ftsParamIndex !== null) {
      params[t.ftsParamIndex] = fts5ToTsquery(String(params[t.ftsParamIndex] ?? ''));
    }
    return params;
  }

  private runTranslated(t: TranslatedSql, rawArgs: unknown[], chunk = false): WorkerResponse {
    if (t.isNoop) return { rows: [], rowCount: 0 };
    return this.call({ op: 'query', sql: t.text, params: this.bindParams(t, rawArgs), chunk });
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
        // Chunked pull: the worker buffers the full result set once and ships
        // CURSOR_CHUNK_ROWS-sized slices on demand, so main-thread memory is
        // O(chunk) instead of O(rows) — preserving what callers like
        // iterateNodesByKind rely on (#610). `return()` releases the worker
        // buffer when a for..of exits early.
        const first = self.runTranslated(t, params, true);
        let buf = first.rows ?? [];
        let cursor = first.cursor;
        let idx = 0;
        const iterator: IterableIterator<any> = {
          [Symbol.iterator]() {
            return this;
          },
          next(): IteratorResult<any> {
            if (idx < buf.length) return { value: buf[idx++], done: false };
            while (cursor !== undefined) {
              const r = self.call({ op: 'fetch', cursor });
              buf = r.rows ?? [];
              idx = 0;
              cursor = r.cursor;
              if (idx < buf.length) return { value: buf[idx++], done: false };
            }
            return { value: undefined, done: true };
          },
          return(value?: any): IteratorResult<any> {
            if (cursor !== undefined) {
              try {
                self.call({ op: 'release', cursor });
              } catch {
                /* releasing is best-effort */
              }
              cursor = undefined;
            }
            buf = [];
            idx = 0;
            return { value, done: true };
          },
        };
        return iterator;
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
      this.mainPort.close();
    } catch {
      /* ignore */
    }
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

const PG_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Per-project PG schema name. One PostgreSQL database typically serves many
 * projects (DATABASE_URL is user/machine-global), so each project's tables
 * live in their own schema — otherwise two indexed repos would interleave
 * rows in shared tables and one project's clear()/sync would destroy the
 * other's graph. Precedence:
 *   1. `CODEGRAPH_PG_SCHEMA` (explicit override; must be a plain identifier),
 *   2. the schema recorded in an existing marker file (stable across moves),
 *   3. derived from the resolved dbPath (stable per project location).
 */
export function derivePgSchemaName(dbPath: string, markerSchema?: string): string {
  const explicit = (process.env.CODEGRAPH_PG_SCHEMA ?? '').trim();
  if (explicit) {
    if (!PG_IDENT.test(explicit) || explicit.length > 63) {
      throw new Error(
        `CODEGRAPH_PG_SCHEMA must be a plain PostgreSQL identifier (got '${explicit}')`
      );
    }
    return explicit;
  }
  if (markerSchema && PG_IDENT.test(markerSchema) && markerSchema.length <= 63) {
    return markerSchema;
  }
  const hash = crypto.createHash('sha256').update(path.resolve(dbPath)).digest('hex').slice(0, 16);
  return `codegraph_${hash}`;
}

/**
 * Create a PostgreSQL-backed database that satisfies the `SqliteDatabase`
 * interface. `dbPath` is the path CodeGraph would have used for the SQLite
 * file; we write a small presence marker there so the lifecycle checks in
 * `DatabaseConnection` (`fs.existsSync` on open) keep working unchanged — the
 * real data lives in PostgreSQL, in the per-project schema recorded in the
 * marker. An existing marker's schema name is reused so the project keeps
 * finding its data even if the directory is moved.
 */
export function createPgDatabase(dbPath: string): SqliteDatabase {
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  let markerSchema: string | undefined;
  if (fs.existsSync(dbPath)) {
    try {
      const marker = JSON.parse(fs.readFileSync(dbPath, 'utf8'));
      if (marker && marker.backend === 'postgres' && typeof marker.schema === 'string') {
        markerSchema = marker.schema;
      }
    } catch {
      /* unreadable marker → derive from path below */
    }
  }
  const schemaName = derivePgSchemaName(dbPath, markerSchema);

  if (markerSchema !== schemaName || !fs.existsSync(dbPath)) {
    fs.writeFileSync(
      dbPath,
      JSON.stringify(
        {
          backend: 'postgres',
          schema: schemaName,
          note: 'CodeGraph data is stored in PostgreSQL (in the schema above). This file is only a presence marker.',
        },
        null,
        2
      )
    );
  }
  return new PgDatabaseAdapter(resolvePgConnectionString(), schemaName);
}
