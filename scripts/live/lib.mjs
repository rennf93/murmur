// Shared rig for the live checks. Each check runs a real CLI in tmux against
// an isolated broker and judges it only by the broker's own records: no real
// MURMUR_HOME and no real CLI config is read or written. A PATH shim makes
// `murmur` (what the installer writes into hooks and MCP entries) run this
// checkout's bin/murmur.js, and the installer itself runs into a scratch home
// so each check exercises exactly the files `murmur install` would write.
import { execFileSync, spawn } from 'node:child_process';
import { chmodSync, existsSync, mkdirSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';

export const REPO = fileURLToPath(new URL('../..', import.meta.url));
const BIN = join(REPO, 'bin', 'murmur.js');

export const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
export const log = (...args) => console.log(new Date().toISOString().slice(11, 19), ...args);

export async function until(label, fn, ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const value = await fn();
    if (value) return value;
    await wait(1000);
  }
  throw new Error('timed out waiting for ' + label);
}

export async function rig(name) {
  if (!existsSync(join(REPO, 'dist', 'cli.js'))) throw new Error('run npm run build first');
  // Short on purpose (a Unix socket path must fit in ~104 bytes) and resolved
  // (/tmp is a symlink on macOS; CLIs key folder trust by the real path).
  const root = join(realpathSync('/tmp'), 'mm-live-' + name);
  const home = join(root, 'h');
  const scratchHome = join(root, 'home');
  const bin = join(root, 'bin');
  rmSync(root, { recursive: true, force: true });
  for (const dir of [home, scratchHome, bin]) mkdirSync(dir, { recursive: true });
  writeFileSync(join(bin, 'murmur'), '#!/bin/sh\nexec node ' + JSON.stringify(BIN) + ' "$@"\n');
  chmodSync(join(bin, 'murmur'), 0o755);
  const env = { ...process.env, MURMUR_HOME: home, PATH: bin + ':' + process.env.PATH };
  const sh = (cmd, args) => execFileSync(cmd, args, { encoding: 'utf8', env }).trim();
  const murmur = (...args) => sh('node', [BIN, ...args]);
  murmur('init');
  const startBroker = async () => {
    const child = spawn('node', [BIN, 'serve'], { env, stdio: 'ignore' });
    await until('broker socket', () => existsSync(join(home, 'bus.sock')), 10000);
    return child;
  };
  let broker = await startBroker();
  const db = new Database(join(home, 'murmur.db'), { readonly: true });
  const { installClient } = await import(pathToFileURL(join(REPO, 'dist', 'install.js')).href);
  const panes = [];
  const pane = (session, lines = 14) =>
    sh('tmux', ['capture-pane', '-p', '-t', session]).split('\n').filter((l) => l.trim()).slice(-lines).join('\n');
  return {
    root,
    env,
    sh,
    murmur,
    db,
    pane,
    // Runs the real installer into the scratch home and returns that home.
    async install(client) {
      await installClient(client, { home: scratchHome });
      return scratchHome;
    },
    tmux(session, cwd, command, extraEnv = {}) {
      panes.push(session);
      // MURMUR_AUTOSTART=0: a SessionEnd hook that outlives cleanup must not respawn the broker.
      const vars = { MURMUR_HOME: home, PATH: env.PATH, MURMUR_AUTOSTART: '0', ...extraEnv };
      const flags = Object.entries(vars).flatMap(([k, v]) => ['-e', k + '=' + v]);
      sh('tmux', ['new-session', '-d', '-s', session, '-x', '200', '-y', '50', '-c', cwd, ...flags, command]);
    },
    async type(session, text) {
      sh('tmux', ['send-keys', '-t', session, '-l', text]);
      await wait(800);
      sh('tmux', ['send-keys', '-t', session, 'C-m']);
    },
    async keys(session, ...keys) {
      for (const key of keys) {
        sh('tmux', ['send-keys', '-t', session, key]);
        await wait(400);
      }
    },
    session: (client) => db.prepare('SELECT handle, client, cli_session_id, state, transcript_path FROM sessions WHERE client = ? ORDER BY id').all(client),
    deliveries: (messageId) =>
      db.prepare('SELECT recipient, state, sent_at IS NOT NULL AS sent, delivered_at IS NOT NULL AS delivered, latency_ms FROM deliveries WHERE message_id = ? ORDER BY recipient').all(messageId),
    // Stops the rig's own broker by its handle and starts a new one on the same home. The CLIs'
    // MCP servers have ~5 s to reconnect (MURMUR_AUTOSTART=0 keeps them from starting one).
    async restartBroker() {
      const exited = new Promise((resolve) => broker.once('exit', resolve));
      broker.kill('SIGTERM');
      await exited;
      rmSync(join(home, 'bus.sock'), { force: true });
      broker = await startBroker();
      log('broker restarted');
    },
    // Owner send; returns the ids the CLI prints.
    send(to, body) {
      const out = murmur('send', to, body);
      log('send\n' + out);
      return { messageId: Number(/message (\d+)/.exec(out)[1]), threadId: Number(/thread (\d+)/.exec(out)[1]) };
    },
    cleanup() {
      for (const session of panes) {
        try { sh('tmux', ['kill-session', '-t', session]); } catch {}
      }
      broker.kill('SIGTERM');
      db.close();
      rmSync(root, { recursive: true, force: true });
      log('cleaned up ' + root);
    },
  };
}

// The ask every check sends: one tool call, nothing else.
export const ASK =
  'Live murmur check. Call the murmur_reply tool once with messageId set to the number after "m" in the tag above, body "INFO received", verdict "info". Then stop.';

// Prints one PASS or FAIL line per check and sets the exit code.
export function verdict(checks) {
  let ok = true;
  for (const [label, pass, detail] of checks) {
    ok &&= Boolean(pass);
    log((pass ? 'PASS ' : 'FAIL ') + label + (detail === undefined ? '' : ': ' + detail));
  }
  log(ok ? 'RESULT: PASS' : 'RESULT: FAIL');
  process.exitCode = ok ? 0 : 1;
}
