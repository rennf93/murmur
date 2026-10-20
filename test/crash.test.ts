import Database from 'better-sqlite3';
import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, unlinkSync } from 'node:fs';
import { createServer } from 'node:net';
import type { Server, Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import { Broker } from '../src/broker.js';
import { MurmurClient } from '../src/client.js';
import { getPaths, readToken } from '../src/config.js';
import type { MurmurPaths } from '../src/config.js';
import { MurmurCore } from '../src/core.js';
import { LineReader } from '../src/protocol.js';
import type { ClientMessage, RegisterResult, SendResult } from '../src/protocol.js';
import { Store } from '../src/store.js';

// Crash recovery: a real broker process (node dist/cli.js serve) is killed
// with SIGKILL in the middle of a send burst whose pushes are slowed down by
// a scripted native peer, then a fresh broker opens the same home and the
// WAL recovery invariants are checked. dist must already be built; that is
// the Makefile's job, not this test's.

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const CLI_JS = join(REPO_ROOT, 'dist', 'cli.js');
// Opener plus burst stays under RATE_MAX_SENDS (20 per minute) so every send
// in the burst would succeed if the broker survived.
const BURST = 18;
const KILL_AFTER_REPLIES = 5;
const SLOW_PEER_DELAY_MS = 15;

interface Cleanup {
  children: ChildProcess[];
  servers: Server[];
  peerSockets: Socket[];
  clients: MurmurClient[];
  brokers: Broker[];
  stores: Store[];
  dirs: string[];
}

const cleanup: Cleanup = { children: [], servers: [], peerSockets: [], clients: [], brokers: [], stores: [], dirs: [] };

function req<T>(client: MurmurClient, msg: Record<string, unknown>): Promise<T> {
  return client.request<T>(msg as unknown as Omit<ClientMessage, 'v' | 'id'>);
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolveSleep) => {
    setTimeout(resolveSleep, ms);
  });
}

interface Spawned {
  child: ChildProcess;
  stdout: string[];
  stderr: string[];
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
}

function spawnCli(args: string[], home: string): Spawned {
  const env: NodeJS.ProcessEnv = { ...process.env, MURMUR_HOME: home };
  delete env.MURMUR_HANDLE;
  const child = spawn(process.execPath, [CLI_JS, ...args], { env, stdio: ['ignore', 'pipe', 'pipe'] });
  cleanup.children.push(child);
  const stdout: string[] = [];
  const stderr: string[] = [];
  child.stdout?.setEncoding('utf8');
  child.stderr?.setEncoding('utf8');
  child.stdout?.on('data', (chunk: string) => {
    stdout.push(chunk);
  });
  child.stderr?.on('data', (chunk: string) => {
    stderr.push(chunk);
  });
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolveExit) => {
    child.once('exit', (code, signal) => {
      resolveExit({ code, signal });
    });
  });
  return { child, stdout, stderr, exited };
}

// Polls until the broker child accepts an authenticated connection.
async function connectWithRetry(paths: MurmurPaths, timeoutMs: number): Promise<MurmurClient> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = null;
  while (Date.now() < deadline) {
    if (existsSync(paths.socketPath) && existsSync(paths.tokenPath)) {
      try {
        const client = await MurmurClient.connect({ socketPath: paths.socketPath, token: readToken(paths) });
        cleanup.clients.push(client);
        return client;
      } catch (err) {
        lastError = err;
      }
    }
    await sleep(25);
  }
  throw new Error('broker child never became reachable: ' + String(lastError));
}

// A native messaging peer that acknowledges each message only after a delay,
// so every push the broker runs is in flight for a while.
function slowNativePeer(socketPath: string): Promise<Server> {
  const server = createServer((socket: Socket) => {
    cleanup.peerSockets.push(socket);
    const reader = new LineReader();
    socket.setEncoding('utf8');
    socket.on('error', () => {});
    socket.on('data', (chunk: string) => {
      for (const line of reader.feed(chunk)) {
        const parsed = JSON.parse(line) as { type: string };
        if (parsed.type === 'message') {
          setTimeout(() => {
            if (!socket.destroyed) {
              socket.write('{"type":"ack"}\n');
            }
          }, SLOW_PEER_DELAY_MS);
        }
      }
    });
  });
  cleanup.servers.push(server);
  return new Promise((resolveServer, reject) => {
    server.once('error', reject);
    server.listen(socketPath, () => {
      resolveServer(server);
    });
  });
}

afterEach(async () => {
  for (const client of cleanup.clients.splice(0)) {
    client.close();
  }
  for (const child of cleanup.children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
    }
  }
  for (const broker of cleanup.brokers.splice(0)) {
    await broker.stop();
  }
  for (const store of cleanup.stores.splice(0)) {
    store.close();
  }
  for (const socket of cleanup.peerSockets.splice(0)) {
    socket.destroy();
  }
  for (const server of cleanup.servers.splice(0)) {
    await new Promise<void>((resolveClose) => {
      server.close(() => resolveClose());
    });
  }
  for (const dir of cleanup.dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

describe('broker crash mid-fanout and WAL recovery', () => {
  it('keeps every acknowledged send intact, no duplicate deliveries and contiguous seqs', async () => {
    expect(existsSync(CLI_JS), 'dist/cli.js must be built before the crash test runs (make build)').toBe(true);
    const home = mkdtempSync(join(tmpdir(), 'murmur-crash-'));
    cleanup.dirs.push(home);
    const paths = getPaths(home);

    // 1. A real broker process.
    const serve = spawnCli(['serve'], home);
    const alice = await connectWithRetry(paths, 15000);
    expect(serve.stdout.join('')).toContain(paths.socketPath);
    await req<RegisterResult>(alice, { type: 'register', handle: 'alice', clientKind: 'cli' });

    // 2. bob: a native peer that answers slowly. carol: registered, offline.
    const peerDir = mkdtempSync(join(tmpdir(), 'murmur-crash-peer-'));
    cleanup.dirs.push(peerDir);
    const peerSocket = join(peerDir, 'peer.sock');
    await slowNativePeer(peerSocket);
    const bob = await MurmurClient.connect({ socketPath: paths.socketPath, token: readToken(paths) });
    cleanup.clients.push(bob);
    await req<RegisterResult>(bob, { type: 'register', handle: 'bob', clientKind: 'hook' });
    await req<null>(bob, { type: 'native-register', handle: 'bob', socketPath: peerSocket, token: 'bob-token' });
    const carolSetup = await MurmurClient.connect({ socketPath: paths.socketPath, token: readToken(paths) });
    await req<RegisterResult>(carolSetup, { type: 'register', handle: 'carol', clientKind: 'cli' });
    carolSetup.close();

    // 3. The thread opener goes through the slow push too, proving the path.
    const opener = await req<SendResult>(alice, {
      type: 'send',
      threadId: null,
      to: ['bob', 'carol'],
      body: 'burst 0',
    });
    const threadId = opener.threadId;
    expect(opener.seq).toBe(0);

    // 4. Burst: fire everything at once; the broker serializes the
    // connection's requests and each one awaits the slow native push.
    const acknowledged: SendResult[] = [];
    const settledAll: Promise<void>[] = [];
    for (let i = 1; i <= BURST; i += 1) {
      const p = req<SendResult>(alice, {
        type: 'send',
        threadId,
        to: ['bob', 'carol'],
        body: 'burst ' + String(i),
      }).then(
        (result) => {
          acknowledged.push(result);
        },
        () => {
          // Rejected by the client once the broker died: expected.
        },
      );
      settledAll.push(p);
    }

    // 5. SIGKILL the broker while the fanout is still running.
    const killDeadline = Date.now() + 10000;
    while (acknowledged.length < KILL_AFTER_REPLIES) {
      if (Date.now() > killDeadline) {
        throw new Error('burst never produced ' + String(KILL_AFTER_REPLIES) + ' replies');
      }
      await sleep(1);
    }
    const repliesAtKill = acknowledged.length;
    serve.child.kill('SIGKILL');
    const exit = await serve.exited;
    expect(exit.signal).toBe('SIGKILL');
    await Promise.all(settledAll);
    expect(repliesAtKill).toBeGreaterThanOrEqual(KILL_AFTER_REPLIES);
    expect(acknowledged.length).toBeLessThan(BURST);
    expect(acknowledged.length).toBeGreaterThanOrEqual(repliesAtKill);

    // 6. The crashed process left its socket file behind: Broker.start does
    // not unlink a stale path, so a restart on the same home must clear it
    // first (otherwise listen fails with EADDRINUSE).
    expect(existsSync(paths.socketPath)).toBe(true);
    unlinkSync(paths.socketPath);
    const store = Store.open(paths.dbPath);
    cleanup.stores.push(store);
    const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} });
    const broker = await Broker.start({ paths, store, core });
    cleanup.brokers.push(broker);

    // 7. WAL recovery invariants, read straight from SQLite.
    const allAcked = [opener, ...acknowledged];
    const db = new Database(paths.dbPath, { readonly: true });
    try {
      for (const result of allAcked) {
        const row = db.prepare('SELECT id, thread_id, seq, body FROM messages WHERE id = ?').get(result.messageId) as
          | { id: number; thread_id: number; seq: number; body: string }
          | undefined;
        expect(row, 'acknowledged message ' + String(result.messageId) + ' must survive the crash').toBeDefined();
        expect(row?.thread_id).toBe(threadId);
        expect(row?.seq).toBe(result.seq);
        const deliveries = db
          .prepare('SELECT recipient, state FROM deliveries WHERE message_id = ? ORDER BY recipient')
          .all(result.messageId) as Array<{ recipient: string; state: string }>;
        expect(deliveries.map((d) => d.recipient)).toEqual(['bob', 'carol']);
        for (const delivery of deliveries) {
          expect(['queued', 'sent', 'delivered']).toContain(delivery.state);
        }
        expect(result.deliveries.map((d) => d.recipient).sort()).toEqual(['bob', 'carol']);
      }
      // No duplicate delivery rows anywhere.
      const dupes = db
        .prepare('SELECT message_id, recipient, COUNT(*) AS n FROM deliveries GROUP BY message_id, recipient HAVING n > 1')
        .all();
      expect(dupes).toEqual([]);
      // Every message either has its complete delivery set or none (the two
      // inserts are separate transactions); never a partial one.
      const counts = db
        .prepare(
          'SELECT m.id, COUNT(d.recipient) AS n FROM messages m LEFT JOIN deliveries d ON d.message_id = m.id GROUP BY m.id',
        )
        .all() as Array<{ id: number; n: number }>;
      for (const entry of counts) {
        expect([0, 2]).toContain(entry.n);
      }
      // Seq continuity in the thread: 0..n-1 with no gaps or duplicates.
      const seqs = (db.prepare('SELECT seq FROM messages WHERE thread_id = ? ORDER BY seq').all(threadId) as Array<{ seq: number }>).map(
        (r) => r.seq,
      );
      expect(seqs).toEqual(Array.from({ length: seqs.length }, (_, i) => i));
      expect(seqs.length).toBeGreaterThanOrEqual(allAcked.length);
      // At most one message beyond the acknowledged ones can exist: the one
      // whose push was in flight when the process died.
      expect(seqs.length).toBeLessThanOrEqual(allAcked.length + 1);
      const orphanDeliveries = db
        .prepare('SELECT COUNT(*) AS n FROM deliveries d LEFT JOIN messages m ON m.id = d.message_id WHERE m.id IS NULL')
        .get() as { n: number };
      expect(orphanDeliveries.n).toBe(0);
    } finally {
      db.close();
    }

    // 8. The restarted broker serves the recovered state and the CLI entry
    // point works end to end as a real process (entry guard true branch).
    const viewer = await MurmurClient.connect({ socketPath: paths.socketPath, token: readToken(paths) });
    cleanup.clients.push(viewer);
    const thread = await req<{ latestSeq: number; messages: unknown[] }>(viewer, {
      type: 'thread',
      threadId,
      sinceSeq: null,
      full: false,
    });
    expect(thread.messages.length).toBe(thread.latestSeq + 1);
    const status = spawnCli(['status'], home);
    const statusExit = await status.exited;
    expect(statusExit.code, 'stderr: ' + status.stderr.join('')).toBe(0);
    const lines = status.stdout.join('').split('\n').filter((l) => l.length > 0);
    expect(lines.find((l) => l.startsWith('alice '))).toMatch(/^alice (presumed-offline|offline) unread 0$/);
    expect(lines.find((l) => l.startsWith('bob '))).toMatch(/^bob (presumed-offline|offline) unread \d+$/);
    expect(lines.find((l) => l.startsWith('carol '))).toMatch(/^carol (presumed-offline|offline) unread \d+$/);
    const carolUnread = Number(/unread (\d+)/.exec(lines.find((l) => l.startsWith('carol ')) ?? '')?.[1]);
    expect(carolUnread).toBeGreaterThanOrEqual(allAcked.length);
  });

  it('exits non-zero through the real entry point on an unknown command', async () => {
    expect(existsSync(CLI_JS)).toBe(true);
    const home = mkdtempSync(join(tmpdir(), 'murmur-crash-cli-'));
    cleanup.dirs.push(home);
    const run = spawnCli(['definitely-not-a-command'], home);
    const exit = await run.exited;
    expect(exit.code).toBe(1);
    expect(run.stderr.join('')).toContain('murmur: unknown command: definitely-not-a-command');
    expect(run.stdout.join('')).toBe('');
  });
});
