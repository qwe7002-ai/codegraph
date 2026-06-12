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
 * Connection + schema application are lazy: the first request triggers
 * `connect()`, which opens the client and applies the full PG schema
 * (idempotent). The connection latency is simply absorbed by the main thread's
 * first `Atomics.wait`.
 */

import { workerData } from 'worker_threads';
import type { MessagePort } from 'worker_threads';
import { PG_SCHEMA } from './pg-schema';

interface WorkerData {
  port: MessagePort;
  signal: SharedArrayBuffer;
  connectionString: string;
}

const { port, signal, connectionString } = workerData as WorkerData;
const lock = new Int32Array(signal);

let client: any;
let ready: Promise<void> | null = null;

function connect(): Promise<void> {
  if (ready) return ready;
  ready = (async () => {
    let pg: any;
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      pg = require('pg');
    } catch {
      throw new Error(
        "The 'pg' package is required for the PostgreSQL backend but is not installed.\n" +
          "Install it with: npm install pg"
      );
    }
    // PG returns BIGINT (int8, oid 20) as a string by default; CodeGraph stores
    // millisecond timestamps and counts there and expects JS numbers. All such
    // values are well under 2^53, so parsing to Number is safe.
    pg.types.setTypeParser(20, (v: string) => parseInt(v, 10));

    client = new pg.Client(connectionString ? { connectionString } : {});
    await client.connect();
    await client.query(PG_SCHEMA);
  })();
  return ready;
}

function respond(message: unknown): void {
  // Post the result FIRST, then wake the blocked main thread. The main thread
  // drains the message with receiveMessageOnPort once Atomics.wait returns.
  port.postMessage(message);
  Atomics.store(lock, 0, 1);
  Atomics.notify(lock, 0, 1);
}

port.on('message', async (req: { op: 'query' | 'exec' | 'close'; sql?: string; params?: unknown[] }) => {
  if (req.op === 'close') {
    try {
      if (client) await client.end();
    } catch {
      /* ignore */
    }
    respond({ ok: true });
    port.close();
    return;
  }

  try {
    await connect();
    if (req.op === 'exec') {
      // DDL / multi-statement strings with no bound params.
      await client.query(req.sql);
      respond({ rows: [], rowCount: 0 });
    } else {
      const res = await client.query(req.sql, req.params ?? []);
      respond({ rows: res.rows ?? [], rowCount: res.rowCount ?? 0 });
    }
  } catch (e: unknown) {
    const err = e as { message?: string; code?: string };
    respond({ error: err?.message ?? String(e), code: err?.code });
  }
});
