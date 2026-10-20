#!/usr/bin/env node
// Latency and throughput bench. Usage: node scripts/bench.mjs <compiled dist dir>
// The same script measures any build, so before and after are comparable.
// It runs an isolated in-process broker on a short MURMUR_HOME (the Unix
// socket path limit) with a fake clock so the send rate limit never trips,
// and spawns real processes for the end to end rows.
import { spawn } from 'node:child_process';
import { mkdirSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const distArg = process.argv[2];
if (distArg === undefined) {
  console.error('usage: node scripts/bench.mjs <compiled dist dir>');
  process.exit(1);
}
const dist = resolve(distArg);
const load = (name) => import(pathToFileURL(join(dist, name)).href);
const { Store } = await load('store.js');
const { MurmurCore } = await load('core.js');
const { Broker } = await load('broker.js');
const { MurmurClient } = await load('client.js');
const { getPaths, ensureHome, readToken } = await load('config.js');

// Short on purpose: a Unix socket path must fit in ~104 bytes.
const HOME = process.env.BENCH_HOME ?? join(realpathSync('/tmp'), 'mm-bench');
const HISTORY = Number(process.env.BENCH_HISTORY ?? 100000);
const SESSIONS = 50;
const E2E_RUNS = Number(process.env.BENCH_E2E ?? 25);
const RT_RUNS = Number(process.env.BENCH_RT ?? 40);

rmSync(HOME, { recursive: true, force: true });
mkdirSync(HOME, { recursive: true });
process.env.MURMUR_HOME = HOME;
delete process.env.MURMUR_AUTOSTART;
const paths = getPaths(HOME);
ensureHome(paths);

// Seed: 100k historical deliveries (replied, spread over 50 handles, 50 per
// thread), 50 sessions with their peers, a 10 member group, sinks.
const store = Store.open(paths.dbPath);
const t0 = Date.now();
store.upsertPeer('renn', 'renn (owner)', null, true, t0);
for (let i = 0; i < SESSIONS; i += 1) {
  const handle = 'h' + String(i);
  store.upsertPeer(handle, null, null, false, t0);
  store.insertSession({
    client: 'claude', cliSessionId: 'seed-' + String(i), handle,
    hostPid: 900000 + i, hostStart: 'Mon Jan  1 00:00:00 2024', transcriptPath: null, cwd: null, now: t0,
  });
}
for (const handle of ['sink', 'cli-sink', 'burst-sink']) {
  store.upsertPeer(handle, null, null, false, t0);
}
store.createGroup('g10', t0);
for (let i = 0; i < 10; i += 1) {
  store.addMember('g10', 'h' + String(i), t0);
}
const { default: Database } = await import('better-sqlite3');
const raw = new Database(paths.dbPath);
raw.transaction(() => {
  const msg = raw.prepare(
    "INSERT INTO messages (thread_id, seq, sender, body, delivered_body, truncated, refs_json, mode, is_reply, in_reply_to, verdict, created_at) VALUES (?, ?, 'renn', 'old', 'old', 0, '[]', 'push', 0, NULL, NULL, ?)",
  );
  const del = raw.prepare(
    "INSERT INTO deliveries (message_id, recipient, state, sent_at, delivered_at, acked_at, replied_at, latency_ms) VALUES (?, ?, 'replied', ?, ?, ?, ?, 10)",
  );
  for (let i = 0; i < HISTORY; i += 1) {
    const at = t0 - (HISTORY - i) * 1000;
    const info = msg.run(1000 + Math.floor(i / 50), i % 50, at);
    del.run(Number(info.lastInsertRowid), 'h' + String(i % 50), at, at, at, at);
  }
})();
raw.close();

let fake = Date.now();
const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, () => (fake += 5000));
const broker = await Broker.start({ paths, store, core });
const token = readToken(paths);
const connect = () => MurmurClient.connect({ socketPath: paths.socketPath, token });

const wrapper = join(HOME, 'run.mjs');
writeFileSync(
  wrapper,
  `import { runCli } from ${JSON.stringify(pathToFileURL(join(dist, 'cli.js')).href)};\nprocess.exit(await runCli(process.argv.slice(2)));\n`,
);

function runProc(args, stdin) {
  return new Promise((done, fail) => {
    const started = process.hrtime.bigint();
    // sh in the middle gives the realistic hook -> sh -> host chain.
    const child = spawn('/bin/sh', ['-c', 'node "$0" "$@"; exit $?', wrapper, ...args], {
      env: { ...process.env, MURMUR_HOME: HOME },
      stdio: ['pipe', 'ignore', 'pipe'],
    });
    let errText = '';
    child.stderr.on('data', (c) => { errText += c; });
    child.on('error', fail);
    child.on('close', (code) => {
      if (code !== 0) {
        fail(new Error('exit ' + String(code) + ': ' + errText));
        return;
      }
      done(Number(process.hrtime.bigint() - started) / 1e6);
    });
    child.stdin.end(stdin ?? '');
  });
}

const pct = (xs, p) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)];
};
const rows = [];
async function measure(label, runs, fn) {
  await fn(-1); // warmup
  const times = [];
  for (let i = 0; i < runs; i += 1) {
    times.push(await fn(i));
  }
  rows.push([label, pct(times, 50), pct(times, 95)]);
}
const timed = async (fn) => {
  const t = process.hrtime.bigint();
  await fn();
  return Number(process.hrtime.bigint() - t) / 1e6;
};

const hookInput = JSON.stringify({ session_id: 'bench-session', transcript_path: join(HOME, 't.jsonl'), cwd: '/tmp/benchproj' });
await measure('hook prompt e2e (process)', E2E_RUNS, () => runProc(['hook', 'prompt', '--client', 'claude'], hookInput));
await measure('cli send e2e (process)', E2E_RUNS, (i) => runProc(['send', 'cli-sink', 'bench body ' + String(i) + ' ' + String(Math.random())]));

const rt = await connect();
await rt.request({ type: 'register', owner: true, clientKind: 'cli' });
let n = 0;
await measure('send fan-out to 10 (round trip)', RT_RUNS, () =>
  timed(() => rt.request({ type: 'send', threadId: null, to: ['@g10'], body: 'fan ' + String(n++) })));
await measure('status, 50 sessions (round trip)', RT_RUNS, () => timed(() => rt.request({ type: 'status' })));
await measure('fetch-push, 100k deliveries (round trip)', RT_RUNS, () =>
  timed(() => rt.request({ type: 'fetch-push', handle: 'h7', limit: 10 })));

const burst = 100;
const b0 = process.hrtime.bigint();
for (let i = 0; i < burst; i += 1) {
  await rt.request({ type: 'send', threadId: null, to: ['burst-sink'], body: 'burst ' + String(i) });
}
const perSec = burst / (Number(process.hrtime.bigint() - b0) / 1e9);

rt.close();
await broker.stop();
store.close();
rmSync(HOME, { recursive: true, force: true });

const width = Math.max(...rows.map((r) => r[0].length));
console.log('dist: ' + dist + '  history deliveries: ' + String(HISTORY));
console.log('scenario'.padEnd(width) + '   p50 ms   p95 ms');
for (const [label, p50, p95] of rows) {
  console.log(label.padEnd(width) + '  ' + p50.toFixed(2).padStart(7) + '  ' + p95.toFixed(2).padStart(7));
}
console.log('burst throughput'.padEnd(width) + '  ' + perSec.toFixed(0).padStart(7) + ' sends/s (single sender, 1 recipient)');
