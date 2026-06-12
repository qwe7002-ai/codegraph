/**
 * PostgreSQL worker (opt-in PG backend)
 *
 * Runs on a `worker_threads` Worker. Holds a SINGLE `pg` Client (not a Pool —
 * a single session is required so BEGIN/COMMIT issued by the adapter's
 * `transaction()` share one connection) and serves one query at a time.
 *
 * The main thread drives this synchronously: it posts a request over the
 * MessagePort, then blocks in `Atomics.wait`. When the (async) query resolves,
 * this worker posts the result back over the port and wakes the main thread via
 * `Atomics.notify`. The main thread then drains the result with
 * `receiveMessageOnPort` — see `pg-adapter.ts`.
 *
 * Protocol notes:
 *   - Every request carries a monotonically increasing `id`, echoed on the
 *     response. The adapter discards responses whose id doesn't match the call
 *     it is blocked on, so a response that arrives after the adapter's timeout
 *     can never be mistaken for a later call's answer.
 *   - `iterate()` uses a chunked cursor protocol (`chunk` flag on the query,
 *     then `fetch`/`release` ops) so the main thread only ever holds one chunk
 *     of rows; the full result set lives once, in this worker, until consumed.
 *   - Because the main thread can't run its event loop while parked in
 *     `Atomics.wait`, a worker-side crash must be reported from THIS thread:
 *     the pg Client gets an `error` listener (a dropped connection otherwise
 *     kills the worker as an uncaught exception), and `uncaughtException`
 *     answers any in-flight request with an error instead of leaving the main
 *     thread to wait out its timeout.
 *
 * Connection + schema application are lazy: the first request triggers
 * `connect()`, which opens the client, creates/selects the per-project schema
 * (see `derivePgSchemaName` in pg-adapter.ts) and applies the PG schema. A
 * failed connect resets state so the next request retries instead of replaying
 * the same stale rejection for the rest of the session.
 */

import { workerData } from 'worker_threads';
import type { MessagePort } from 'worker_threads';
import { PG_SCHEMA, SEEDED_SCHEMA_VERSION } from './pg-schema';

interface WorkerData {
  port: MessagePort;
  signal: SharedArrayBuffer;
  connectionString: string;
  schemaName: string;
}

interface WorkerRequest {
  id: number;
  op: 'query' | 'exec' | 'fetch' | 'release' | 'close';
  sql?: string;
  params?: unknown[];
  /** query op: return rows in chunks via fetch/release (iterate() path). */
  chunk?: boolean;
  /** fetch/release ops: the cursor handle returned by a chunked query. */
  cursor?: number;
}

const { port, signal, connectionString, schemaName } = workerData as WorkerData;
const lock = new Int32Array(signal);

let client: any;
let ready: Promise<void> | null = null;

/** Requests currently being served — answered with an error on a fatal crash. */
const activeIds = new Set<number>();

/** node-postgres re-parses unnamed statements on every execution; naming them
 * makes PG cache the parse/plan per session. Bounded: past the cap, new texts
 * just run unnamed. */
const stmtNames = new Map<string, string>();
const STMT_NAME_MAX = 500;
function stmtNameFor(text: string): string | undefined {
  let name = stmtNames.get(text);
  if (!name && stmtNames.size < STMT_NAME_MAX) {
    name = `cg_stmt_${stmtNames.size}`;
    stmtNames.set(text, name);
  }
  return name;
}

/** Buffered result sets for chunked iterate(); keyed by cursor handle. */
const cursors = new Map<number, { rows: any[]; offset: number }>();
let nextCursorId = 1;
const CURSOR_CHUNK_ROWS = 1000;
const CURSOR_MAX = 16;

function resetConnection(): void {
  const c = client;
  client = undefined;
  ready = null;
  stmtNames.clear();
  if (c) {
    try {
      void c.end().catch(() => {});
    } catch {
      /* ignore */
    }
  }
}

function connect(): Promise<void> {
  if (ready) return ready;
  ready = (async () => {
    try {
      let pg: any;
      try {
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        pg = require('pg');
      } catch {
        throw new Error(
          "The 'pg' package is required for the PostgreSQL backend but is not installed.\n" +
            'Install it with: npm install pg'
        );
      }
      // PG returns BIGINT (int8, oid 20) as a string by default; CodeGraph
      // stores millisecond timestamps and counts there and expects JS numbers.
      // All such values are well under 2^53, so parsing to Number is safe.
      pg.types.setTypeParser(20, (v: string) => parseInt(v, 10));

      client = new pg.Client(connectionString ? { connectionString } : {});
      // Without a listener, a connection dropped BETWEEN queries (PG restart,
      // network blip) emits an unhandled 'error' and kills this worker — and
      // the parked main thread can only find out by timing out. Reset instead,
      // so the next request reconnects.
      client.on('error', () => {
        resetConnection();
      });
      await client.connect();

      // Per-project isolation: every project gets its own PG schema, derived
      // from its .codegraph db path (or CODEGRAPH_PG_SCHEMA). Identifier is
      // validated adapter-side; quote it defensively anyway.
      const ident = `"${schemaName.replace(/"/g, '')}"`;
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${ident}`);
      await client.query(`SET search_path TO ${ident}`);

      // Apply the schema only when it isn't already current: PG_SCHEMA's
      // CREATE OR REPLACE FUNCTION / DROP+CREATE TRIGGER take an ACCESS
      // EXCLUSIVE lock on nodes, so re-running ~30 DDL statements on every
      // connect would serialize against every other codegraph process sharing
      // the database.
      let current = -1;
      try {
        const res = await client.query(
          'SELECT version FROM schema_versions ORDER BY version DESC LIMIT 1'
        );
        current = res.rows?.[0]?.version ?? -1;
      } catch {
        // schema_versions missing → fresh schema; fall through and apply.
      }
      if (current < SEEDED_SCHEMA_VERSION) {
        await client.query(PG_SCHEMA);
      }
    } catch (err) {
      // Don't cache the rejection: a transient failure (PG briefly down) would
      // otherwise poison every later query for the life of the process.
      resetConnection();
      throw err;
    }
  })();
  return ready;
}

function respond(message: Record<string, unknown>): void {
  // Post the result FIRST, then wake the blocked main thread. The main thread
  // drains the message with receiveMessageOnPort once Atomics.wait returns.
  port.postMessage(message);
  Atomics.store(lock, 0, 1);
  Atomics.notify(lock, 0, 1);
}

function errorResponse(id: number, e: unknown): Record<string, unknown> {
  const err = e as { message?: string; code?: string };
  return { id, error: err?.message ?? String(e), code: err?.code };
}

/** First chunk of a buffered result set; registers a cursor when more rows
 * remain. The buffer is advanced by offset (no re-slicing of the remainder). */
function firstChunk(id: number, rows: any[], rowCount: number): Record<string, unknown> {
  if (rows.length <= CURSOR_CHUNK_ROWS) {
    return { id, rows, rowCount };
  }
  const cursorId = nextCursorId++;
  // Bound abandoned cursors (iterators that were never fully consumed nor
  // closed): evict the oldest. Its later fetch just reports done.
  if (cursors.size >= CURSOR_MAX) {
    const oldest = cursors.keys().next().value;
    if (oldest !== undefined) cursors.delete(oldest);
  }
  cursors.set(cursorId, { rows, offset: CURSOR_CHUNK_ROWS });
  return { id, rows: rows.slice(0, CURSOR_CHUNK_ROWS), rowCount, cursor: cursorId };
}

/** Next chunk of an open cursor; frees the buffer once exhausted. */
function nextChunk(id: number, cursorId: number): Record<string, unknown> {
  const cur = cursors.get(cursorId);
  if (!cur) return { id, rows: [], rowCount: 0 };
  const chunk = cur.rows.slice(cur.offset, cur.offset + CURSOR_CHUNK_ROWS);
  cur.offset += CURSOR_CHUNK_ROWS;
  if (cur.offset >= cur.rows.length) {
    cursors.delete(cursorId);
    return { id, rows: chunk, rowCount: chunk.length };
  }
  return { id, rows: chunk, rowCount: chunk.length, cursor: cursorId };
}

// Answer in-flight requests on a fatal worker error instead of dying silently:
// the main thread's event loop is parked, so this thread is the only one that
// can unblock it before the adapter's timeout.
process.on('uncaughtException', (err) => {
  for (const id of [...activeIds]) {
    activeIds.delete(id);
    try {
      respond(errorResponse(id, err));
    } catch {
      /* ignore */
    }
  }
  resetConnection();
});

port.on('message', async (req: WorkerRequest) => {
  const { id } = req;
  activeIds.add(id);
  try {
    if (req.op === 'close') {
      try {
        if (client) await client.end();
      } catch {
        /* ignore */
      }
      respond({ id, ok: true });
      port.close();
      return;
    }

    if (req.op === 'fetch') {
      respond(req.cursor !== undefined ? nextChunk(id, req.cursor) : { id, rows: [], rowCount: 0 });
      return;
    }

    if (req.op === 'release') {
      if (req.cursor !== undefined) cursors.delete(req.cursor);
      respond({ id, ok: true });
      return;
    }

    await connect();
    if (req.op === 'exec') {
      // DDL / multi-statement strings with no bound params.
      await client.query(req.sql);
      respond({ id, rows: [], rowCount: 0 });
    } else {
      const res = await client.query({
        text: req.sql,
        values: req.params ?? [],
        name: stmtNameFor(req.sql ?? ''),
      });
      const rows: any[] = res.rows ?? [];
      // Strip the tsvector column before shipping rows across the port: it's
      // an FTS implementation detail no caller reads, and on nodes scans it
      // roughly doubles the structured-clone payload.
      if (rows.length > 0 && Object.prototype.hasOwnProperty.call(rows[0], 'tsv')) {
        for (const row of rows) delete row.tsv;
      }
      if (req.chunk) {
        respond(firstChunk(id, rows, res.rowCount ?? rows.length));
      } else {
        respond({ id, rows, rowCount: res.rowCount ?? 0 });
      }
    }
  } catch (e: unknown) {
    respond(errorResponse(id, e));
  } finally {
    activeIds.delete(id);
  }
});
