#!/usr/bin/env node
// End-to-end replay of the live 3-peer murmur run.
//
// Plain Node ESM script. It never imports from src/: it spawns the broker and
// the claude CLI as child processes, speaks the murmur wire protocol directly
// over the unix socket with a minimal inline client, and reads the sqlite
// database with better-sqlite3 from node_modules.
//
// Modes:
//   real        three headless claude sessions (default): claude-peer-a,
//               claude-peer-b and claude-implementer
//   fake-peers  (--fake-peers) the two claude peer sessions are replaced by
//               script-driven protocol clients; the implementer stays a real
//               claude session. Exits 3 on success so verify can tell the
//               modes apart, 1 on failure. Real mode exits 0 on success.

import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createConnection } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';

const REPO_ROOT = fileURLToPath(new URL('..', import.meta.url));
const DIST_CLI = join(REPO_ROOT, 'dist', 'cli.js');

const GROUP = 'gate-review';
const PEER_A = 'claude-peer-a';
const PEER_B = 'claude-peer-b';
const IMPLEMENTER = 'claude-implementer';
const EXTRA = 'claude-extra';
const THREAD = 1;

const ASK_BODY = 'e2e ask: approve the gate review for build 42';
const PROBE_BODY = 'e2e probe: reply with verdict agree';
const REPLY_BODY = 'ACK';

// Tools the sessions may call, as specified, plus the mcp__murmur__ prefixed
// forms claude actually matches against MCP tool permission rules.
const TOOL_LIST = 'murmur_register,murmur_send,murmur_reply,murmur_wait,murmur_thread,murmur_read,murmur_collect';
const ALLOWED_TOOLS = TOOL_LIST + ',mcp__murmur__' + TOOL_LIST.split(',').join(',mcp__murmur__');

const SOCKET_WAIT_MS = 15000;
const IMPLEMENTER_TIMEOUT_MS = 240000;
// Real-mode choreography mirrors fake-peers mode: peer-a blocks in a
// 240000ms protocol wait that can only end in its timeout (any_replied has
// no first replier while every peer is waiting), then replies first;
// peer-b's any_replied wait is then resolved by peer-a's reply, so peer-b's
// thread read carries peer-a's reply deterministically, with no relay
// through the implementer. The 280000ms wait sits under the broker's 300000
// cap with 40s of slack for peer-a's reply to land after peer-a's own
// timeout.
const PEER_A_WAIT_MS = 240000;
const PEER_B_WAIT_MS = 280000;
// Process cap: the longest protocol wait (280000ms) plus headroom for
// startup, the thread read and the reply. A hard kill at 280s of process
// lifetime would land mid-wait, before the peer can act on the wake.
const REAL_PEER_TIMEOUT_MS = 360000;
const FAKE_A_WAIT_MS = 30000;
const FAKE_B_WAIT_MS = 2000;
const FAKE_STAGGER_MS = 300;
const FAKE_SETTLE_MS = 45000;
const REAL_SETTLE_MS = 90000;
// Each implementer attempt gets this long to produce the ask. Model turns on
// a busy machine can take minutes, so this sits just under the session's own
// 240s cap; a session that dies early (these get killed by outside watchers)
// still fails fast instead of burning the whole budget.
const IMPLEMENTER_ASK_DEADLINE_MS = 210000;
const IMPLEMENTER_ATTEMPTS = 2;
const IMPLEMENTER_TAIL_GRACE_MS = 60000;

// Everything that needs cleaning up, reachable from every exit path
// (main's finally, the unhandledRejection guard, signals).
const runState = {
  broker: null,
  brokerInfo: { exited: false, code: null, stderr: '' },
  claudeChildren: new Set(),
  clients: [],
  db: null,
  runRoot: null,
};

function peerPrompt(handle, waitMs) {
  return (
    'Call murmur_register with handle ' + handle +
    '. Then call murmur_wait with threadId ' + String(THREAD) +
    ', until any_replied, timeoutMs ' + String(waitMs) +
    '. When it returns or times out, call murmur_thread for thread ' +
    String(THREAD) + ', then murmur_reply to the ask message with verdict agree and body ' + REPLY_BODY +
    '. Then say DONE.'
  );
}

function implementerPrompt() {
  return (
    'Call murmur_register with handle ' + IMPLEMENTER +
    '. Then call murmur_send with to ["@' + GROUP + '"] and body "' + ASK_BODY + '"' +
    '. Then call murmur_send with to ["' + EXTRA + '"] and body "' + PROBE_BODY + '"' +
    '. Then say DONE.'
  );
}

// Minimal newline-delimited JSON client for the murmur unix socket. Auth uses
// wire id 1, requests start at 2, server events go to onEvent. Mirrors the
// shapes in src/protocol.ts without importing it.
class BusClient {
  constructor(socketPath, token, onEvent) {
    this.socketPath = socketPath;
    this.token = token;
    this.onEvent = onEvent;
    this.socket = null;
    this.buffer = '';
    this.pending = new Map();
    this.nextId = 2;
  }

  connect(timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
      const socket = createConnection(this.socketPath);
      let settled = false;
      const finish = (err) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        if (err) {
          socket.destroy();
          reject(new Error(err));
        } else {
          resolve();
        }
      };
      const timer = setTimeout(() => {
        finish('connect to ' + this.socketPath + ' timed out after ' + String(timeoutMs) + 'ms');
      }, timeoutMs);
      socket.once('error', (err) => {
        finish('connect to ' + this.socketPath + ' failed: ' + err.message);
      });
      socket.once('connect', () => {
        this.socket = socket;
        socket.on('data', (chunk) => this.onData(chunk));
        socket.once('close', () => {
          this.failAll('connection to the broker closed');
        });
        this.request({ type: 'auth', token: this.token }, 1)
          .then(() => finish(null))
          .catch((err) => finish(err.message));
      });
    });
  }

  onData(chunk) {
    this.buffer += chunk.toString('utf8');
    for (;;) {
      const idx = this.buffer.indexOf('\n');
      if (idx === -1) {
        break;
      }
      const line = this.buffer.slice(0, idx).trim();
      this.buffer = this.buffer.slice(idx + 1);
      if (line.length === 0) {
        continue;
      }
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch {
        this.failAll('broker sent a line that is not valid JSON');
        return;
      }
      if (parsed !== null && typeof parsed === 'object' && parsed.type === 'event') {
        if (this.onEvent !== undefined) {
          try {
            this.onEvent(parsed);
          } catch {
            // A throwing event handler must not kill the bus connection.
          }
        }
        continue;
      }
      const waiter = this.pending.get(parsed.id);
      if (waiter !== undefined) {
        this.pending.delete(parsed.id);
        if (parsed.ok) {
          waiter.resolve(parsed.result);
        } else {
          waiter.reject(new Error('murmur ' + parsed.error.code + ': ' + parsed.error.message));
        }
      }
    }
  }

  request(msg, forcedId) {
    return new Promise((resolve, reject) => {
      if (this.socket === null) {
        reject(new Error('bus client is not connected'));
        return;
      }
      const id = forcedId === undefined ? this.nextId : forcedId;
      if (forcedId === undefined) {
        this.nextId += 1;
      }
      this.pending.set(id, { resolve, reject });
      try {
        this.socket.write(JSON.stringify({ v: 1, id, ...msg }) + '\n');
      } catch (err) {
        this.pending.delete(id);
        reject(new Error('write to the broker failed: ' + err.message));
      }
    });
  }

  failAll(message) {
    for (const waiter of this.pending.values()) {
      waiter.reject(new Error(message));
    }
    this.pending.clear();
  }

  close() {
    if (this.socket !== null) {
      this.socket.destroy();
      this.socket = null;
    }
    this.failAll('bus client closed');
  }
}

function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function parseMode(argv) {
  if (argv.length === 0) {
    return 'real';
  }
  if (argv.length === 1 && argv[0] === '--fake-peers') {
    return 'fake-peers';
  }
  throw new Error('unknown arguments: ' + argv.join(' ') + ' (supported: --fake-peers)');
}

function readTokenSafe(home) {
  return readFileSync(join(home, 'token'), 'utf8').trim();
}

// Awaits the session end, killing it if it is still running after graceMs.
async function awaitSession(session, graceMs) {
  const finished = await Promise.race([session.done.then(() => true), delay(graceMs).then(() => false)]);
  if (!finished) {
    session.kill();
  }
  return session.done;
}

// Resolves { askId, session } as soon as the implementer's ask lands. If the
// session dies first, gives the bus a short grace (a send may already be in
// flight) and hands back the finished run so the caller can retry.
async function waitForAskOrExit(db, session) {
  const deadline = Date.now() + IMPLEMENTER_ASK_DEADLINE_MS;
  let run = null;
  let settled = false;
  session.done.then((r) => {
    run = r;
    settled = true;
  });
  const askQuery = db.prepare(
    'SELECT id FROM messages WHERE thread_id = ? AND is_reply = 0 AND sender = ? ORDER BY seq ASC LIMIT 1',
  );
  for (;;) {
    const row = askQuery.get(THREAD, IMPLEMENTER);
    if (row !== undefined) {
      return { askId: row.id, run: null };
    }
    if (settled) {
      for (let i = 0; i < 10; i += 1) {
        const again = askQuery.get(THREAD, IMPLEMENTER);
        if (again !== undefined) {
          return { askId: again.id, run: null };
        }
        await delay(200);
      }
      return { askId: null, run };
    }
    if (Date.now() > deadline) {
      return { askId: null, run: null };
    }
    await delay(200);
  }
}

// Runs the implementer session, retrying with a fresh one when an attempt
// dies or stalls before the ask lands. The session is returned still running
// (its second send may not have happened yet); the caller awaits it.
async function runImplementerWithRetry(db, dirImp, home) {
  let lastRun = null;
  for (let attempt = 1; attempt <= IMPLEMENTER_ATTEMPTS; attempt += 1) {
    const session = spawnClaude('implementer', dirImp, implementerPrompt(), IMPLEMENTER_TIMEOUT_MS, home);
    const outcome = await waitForAskOrExit(db, session);
    if (outcome.askId !== null) {
      console.log('setup: implementer attempt ' + String(attempt) + ' produced the ask');
      return { askId: outcome.askId, session };
    }
    let run = outcome.run;
    if (run === null) {
      // Still running past the ask deadline: it is not coming back.
      session.kill();
      run = await session.done;
    }
    lastRun = run;
    console.log(
      'setup: implementer attempt ' + String(attempt) + ' produced no ask (exit ' + String(run.code) +
        ' signal ' + String(run.signal) + (run.timedOut ? ', timed out' : '') + '), stdout lines ' +
        String(run.stdout.split('\n').length) + ', stderr tail: ' + run.stderr.slice(-200).replace(/\n/g, ' '),
    );
  }
  throw new Error(
    'implementer produced no ask in ' + String(IMPLEMENTER_ATTEMPTS) + ' attempts (last exit=' +
      String(lastRun.code) + ' signal=' + String(lastRun.signal) + '), stderr tail: ' +
      lastRun.stderr.slice(-400).replace(/\n/g, ' '),
  );
}

async function main() {
  let mode;
  try {
    mode = parseMode(process.argv.slice(2));
  } catch (err) {
    console.log('E2E FAIL: ' + err.message);
    return 1;
  }
  console.log('MODE ' + mode);

  if (!existsSync(DIST_CLI)) {
    console.log('E2E FAIL: dist/cli.js is missing, build the project first (npm run build)');
    return 1;
  }

  const results = [];

  const check = (num, desc, pass, detail) => {
    results.push({ num, pass });
    const line = (pass ? 'PASS ' : 'FAIL ') + String(num) + ': ' + desc;
    console.log(detail !== undefined && detail.length > 0 ? line + ' :: ' + detail : line);
  };

  try {
    // Temporary murmur home: the broker, every CLI call, every MCP server and
    // every fake peer live inside it for the whole run.
    const runRoot = mkdtempSync(join(tmpdir(), 'murmur-e2e-'));
    runState.runRoot = runRoot;
    const home = join(runRoot, 'home');
    const dirA = join(runRoot, 'peer-a');
    const dirB = join(runRoot, 'peer-b');
    const dirImp = join(runRoot, 'implementer');
    mkdirSync(home, { recursive: true });
    mkdirSync(dirA, { recursive: true });
    mkdirSync(dirB, { recursive: true });
    mkdirSync(dirImp, { recursive: true });
    const socketPath = join(home, 'bus.sock');
    const dbPath = join(home, 'murmur.db');

    // 1. Spawn the broker and wait for the socket.
    const broker = spawn(process.execPath, [DIST_CLI, 'serve'], {
      cwd: REPO_ROOT,
      env: { ...process.env, MURMUR_HOME: home },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    runState.broker = broker;
    const brokerInfo = runState.brokerInfo;
    let brokerStdout = '';
    broker.stdout.on('data', (chunk) => {
      brokerStdout += chunk.toString('utf8');
    });
    broker.stderr.on('data', (chunk) => {
      brokerInfo.stderr += chunk.toString('utf8');
      if (brokerInfo.stderr.length > 8000) {
        brokerInfo.stderr = brokerInfo.stderr.slice(-8000);
      }
    });
    broker.once('exit', (code) => {
      brokerInfo.exited = true;
      brokerInfo.code = code;
    });
    const socketDeadline = Date.now() + SOCKET_WAIT_MS;
    while (!existsSync(socketPath)) {
      if (brokerInfo.exited) {
        throw new Error(
          'broker exited with code ' + String(brokerInfo.code) + ' before opening the socket: ' +
            (brokerInfo.stderr.trim() || brokerStdout.trim() || 'no output'),
        );
      }
      if (Date.now() > socketDeadline) {
        throw new Error('socket ' + socketPath + ' did not appear within ' + String(SOCKET_WAIT_MS) + 'ms');
      }
      await delay(100);
    }

    const runCli = (args, extraEnv) => {
      const env = { ...process.env, MURMUR_HOME: home };
      if (extraEnv !== undefined) {
        Object.assign(env, extraEnv);
      }
      const res = spawnSync(process.execPath, [DIST_CLI, ...args], { cwd: REPO_ROOT, env, encoding: 'utf8' });
      if (res.status !== 0) {
        throw new Error(
          'murmur ' + args.join(' ') + ' failed with exit ' + String(res.status) + ': ' +
            String(res.stderr !== '' ? res.stderr : res.stdout).trim(),
        );
      }
      return res;
    };

    // 2. Setup via the CLI. The hook path is the CLI's named peer
    // registration (MURMUR_HANDLE wins, session-start creates the peer row),
    // and the rows must exist before groups add validates its members
    // against registered handles.
    runCli(['init']);
    console.log('setup: murmur init at ' + home);
    runCli(['hook', 'session-start'], { MURMUR_HANDLE: PEER_A });
    runCli(['hook', 'session-start'], { MURMUR_HANDLE: PEER_B });
    console.log('setup: registered named peers ' + PEER_A + ', ' + PEER_B + ' via hook session-start');
    runCli(['groups', 'create', GROUP]);
    console.log('setup: groups create ' + GROUP);
    runCli(['groups', 'add', GROUP, PEER_A, PEER_B]);
    console.log('setup: groups add ' + GROUP + ' ' + PEER_A + ' ' + PEER_B);
    runCli(['hook', 'session-start'], { MURMUR_HANDLE: IMPLEMENTER });
    console.log('setup: registered named peer ' + IMPLEMENTER + ' via hook session-start');

    const token = readTokenSafe(home);

    // mcp.json per claude cwd: the murmur MCP server pinned to the temp home
    // with the session handle in its environment.
    const writeMcpConfig = (dir, handle) => {
      const config = {
        mcpServers: {
          murmur: {
            command: process.execPath,
            args: [DIST_CLI, 'mcp'],
            env: { MURMUR_HOME: home, MURMUR_HANDLE: handle },
          },
        },
      };
      writeFileSync(join(dir, 'mcp.json'), JSON.stringify(config, null, 2) + '\n');
    };

    // The claude-extra fake peer: registered over a raw client connection
    // (clientKind cli), stays connected, answers any push with a verdict
    // agree reply. Used in both modes.
    const extra = { pushes: [], replies: [] };
    const extraClient = new BusClient(socketPath, token, (ev) => {
      if (ev.event !== 'push' || ev.payload === undefined || ev.payload.message === undefined) {
        return;
      }
      const message = ev.payload.message;
      extra.pushes.push({ messageId: message.id, threadId: message.threadId, body: message.body });
      extraClient
        .request({ type: 'reply', messageId: message.id, body: REPLY_BODY, verdict: 'agree' })
        .then((r) => extra.replies.push({ messageId: message.id, result: r }))
        .catch((err) => extra.replies.push({ messageId: message.id, error: err.message }));
    });
    runState.clients.push(extraClient);
    await extraClient.connect();
    await extraClient.request({ type: 'register', handle: EXTRA, clientKind: 'cli' });
    console.log('setup: registered fake peer ' + EXTRA + ' over a raw client connection');

    // The implementer is a real headless claude session in both modes.
    writeMcpConfig(dirImp, IMPLEMENTER);

    let fakeLogs = [];
    let claudeRuns = [];
    let implementerSession = null;
    if (mode === 'fake-peers') {
      // The implementer runs first; the fake peers start once the ask exists,
      // so their post-wait thread reads never hit an unknown thread.
      runState.db = new Database(dbPath);
      const impl = await runImplementerWithRetry(runState.db, dirImp, home);
      implementerSession = impl.session;
      console.log('setup: ask message ' + String(impl.askId) + ' present in thread ' + String(THREAD));

      // Script-driven peers. B is launched after a short stagger, times out
      // of its any_replied wait and replies first; A's wait then resolves on
      // B's reply, so A reads B's reply off the thread before replying, with
      // no relay through the implementer.
      const runFakePeer = async (handle, waitTimeoutMs) => {
        const log = { handle, steps: [] };
        const client = new BusClient(socketPath, token);
        runState.clients.push(client);
        await client.connect();
        await client.request({ type: 'register', handle, clientKind: 'cli' });
        log.steps.push({ step: 'register', handle });
        let wait;
        try {
          wait = await client.request({
            type: 'wait',
            threadId: THREAD,
            until: 'any_replied',
            timeoutMs: waitTimeoutMs,
          });
        } catch (err) {
          wait = { error: err.message };
        }
        log.steps.push({ step: 'wait', result: wait });
        const thread = await client.request({ type: 'thread', threadId: THREAD, sinceSeq: null, full: true });
        log.steps.push({ step: 'thread', messages: thread.messages });
        const ask = thread.messages.find((m) => m.isReply === false);
        if (ask === undefined) {
          throw new Error(handle + ': no ask message found in thread ' + String(THREAD));
        }
        await client.request({ type: 'reply', messageId: ask.id, body: REPLY_BODY, verdict: 'agree' });
        log.steps.push({ step: 'reply', messageId: ask.id, body: REPLY_BODY, verdict: 'agree' });
        client.close();
        return log;
      };
      const wrap = (promise, handle) =>
        promise.catch((err) => {
          throw new Error(err instanceof Error ? handle + ': ' + err.message : handle + ': ' + String(err));
        });
      const peerAPromise = wrap(runFakePeer(PEER_A, FAKE_A_WAIT_MS), PEER_A);
      await delay(FAKE_STAGGER_MS);
      const peerBPromise = wrap(runFakePeer(PEER_B, FAKE_B_WAIT_MS), PEER_B);
      const all = await Promise.all([impl.session.done, peerAPromise, peerBPromise]);
      claudeRuns = [all[0]];
      fakeLogs = [all[1], all[2]];
    } else {
      writeMcpConfig(dirA, PEER_A);
      writeMcpConfig(dirB, PEER_B);
      const peerASession = spawnClaude('peer-a', dirA, peerPrompt(PEER_A, PEER_A_WAIT_MS), REAL_PEER_TIMEOUT_MS, home);
      const peerBSession = spawnClaude('peer-b', dirB, peerPrompt(PEER_B, PEER_B_WAIT_MS), REAL_PEER_TIMEOUT_MS, home);
      runState.db = new Database(dbPath);
      const impl = await runImplementerWithRetry(runState.db, dirImp, home);
      implementerSession = impl.session;
      const all = await Promise.all([impl.session.done, peerASession.done, peerBSession.done]);
      claudeRuns = [all[0], all[1], all[2]];
    }
    for (const run of claudeRuns) {
      console.log(
        'claude ' + run.label + ': exit ' + String(run.code) + ' signal ' + String(run.signal) + ' in ' +
          String(run.ms) + 'ms' + (run.timedOut ? ' (timed out)' : ''),
      );
    }

    // 3. Wait until every reply is in, then assert against the database.
    const db = runState.db;
    const settled = await waitSettled(db, mode === 'fake-peers' ? FAKE_SETTLE_MS : REAL_SETTLE_MS);
    if (!settled.ok) {
      throw new Error('replies never settled: ' + settled.detail + ' | state: ' + dumpState(db));
    }
    // The implementer's work is done once the asks have landed; do not let a
    // chatty tail block the run.
    if (implementerSession !== null) {
      await awaitSession(implementerSession, IMPLEMENTER_TAIL_GRACE_MS);
    }

    const ask = db
      .prepare('SELECT id, thread_id, seq, sender FROM messages WHERE thread_id = ? AND is_reply = 0 AND sender = ? ORDER BY seq ASC LIMIT 1')
      .get(THREAD, IMPLEMENTER);
    if (ask === undefined) {
      throw new Error('no ask message from ' + IMPLEMENTER + ' in thread ' + String(THREAD) + ': ' + dumpState(db));
    }
    const probe = db
      .prepare(
        'SELECT m.id, m.thread_id FROM messages m JOIN deliveries d ON d.message_id = m.id ' +
          'WHERE m.is_reply = 0 AND m.sender = ? AND d.recipient = ? ORDER BY m.id ASC LIMIT 1',
      )
      .get(IMPLEMENTER, EXTRA);
    if (probe === undefined) {
      throw new Error('no direct probe message to ' + EXTRA + ': ' + dumpState(db));
    }

    // Assertion 1: every recipient of the ask has exactly one delivery row
    // and its final state is replied.
    const askDeliveries = db
      .prepare('SELECT recipient, state, latency_ms FROM deliveries WHERE message_id = ? ORDER BY recipient ASC')
      .all(ask.id);
    const expectedRecipients = [PEER_A, PEER_B];
    const gotRecipients = askDeliveries.map((r) => r.recipient);
    const oneEachAndReplied =
      gotRecipients.length === expectedRecipients.length &&
      expectedRecipients.every((handle) => gotRecipients.includes(handle)) &&
      new Set(gotRecipients).size === gotRecipients.length &&
      askDeliveries.every((r) => r.state === 'replied');
    check(
      1,
      'ask deliveries: one row per recipient, final state replied',
      oneEachAndReplied,
      'message_id=' + String(ask.id) + ' rows=' + JSON.stringify(askDeliveries),
    );

    // Assertion 2: replies carry in_reply_to equal to the ask id and the
    // thread seq is contiguous. Seqs are 0-based in this implementation (the
    // store numbers the first message of a thread 0), so contiguity is
    // checked as the gapless run 0..N-1.
    const replies = db
      .prepare('SELECT id, seq, sender, in_reply_to, verdict, body FROM messages WHERE thread_id = ? AND is_reply = 1 ORDER BY seq ASC')
      .all(THREAD);
    const seqs = db.prepare('SELECT seq FROM messages WHERE thread_id = ? ORDER BY seq ASC').all(THREAD).map((r) => r.seq);
    const seqContiguous = seqs.length > 0 && seqs.every((seq, i) => seq === i);
    const inReplyToOk = replies.length > 0 && replies.every((r) => r.in_reply_to === ask.id);
    check(
      2,
      'replies in_reply_to the ask and thread seq contiguous',
      inReplyToOk && seqContiguous,
      'replies=' + JSON.stringify(replies) + ' seqs=' + JSON.stringify(seqs),
    );

    // Assertion 3: a peer read the other peer's reply straight off the
    // thread, with no second send from the implementer (thread 1 holds the
    // one ask and nothing else non-reply).
    const askCount = db
      .prepare('SELECT COUNT(*) AS n FROM messages WHERE thread_id = ? AND is_reply = 0')
      .get(THREAD).n;
    let evidence = null;
    if (mode === 'fake-peers') {
      const logA = fakeLogs.find((log) => log.handle === PEER_A);
      const threadStep = logA !== undefined ? logA.steps.find((s) => s.step === 'thread') : undefined;
      const sawB =
        threadStep !== undefined &&
        threadStep.messages.some(
          (m) => m.sender === PEER_B && m.isReply === true && m.inReplyTo === ask.id && m.body === REPLY_BODY,
        );
      if (sawB) {
        evidence = PEER_A + ' thread view contains ' + PEER_B + ' reply (in_reply_to ' + String(ask.id) + ', body ' + REPLY_BODY + ')';
      }
    } else {
      const runA = claudeRuns.find((run) => run.label === 'peer-a');
      const runB = claudeRuns.find((run) => run.label === 'peer-b');
      if (runA !== undefined && peerReadEvidence(runA.stdout, PEER_B)) {
        evidence = 'peer-a stdout murmur_thread result contains ' + PEER_B + ' reply';
      } else if (runB !== undefined && peerReadEvidence(runB.stdout, PEER_A)) {
        evidence = 'peer-b stdout murmur_thread result contains ' + PEER_A + ' reply';
      }
    }
    check(
      3,
      'peer read the other peer reply with no sender relay (no second implementer send)',
      evidence !== null && askCount === 1,
      (evidence ?? 'no peer thread-read evidence found') + ' | thread 1 non-reply messages: ' + String(askCount),
    );

    // Assertion 4: collect verdict tally shows agree for every recipient,
    // with reply latencies present. Collect runs over a raw client
    // connection.
    const collectClient = new BusClient(socketPath, token);
    runState.clients.push(collectClient);
    await collectClient.connect();
    const collect1 = await collectClient.request({ type: 'collect', threadId: THREAD });
    collectClient.close();
    const recipientsOk =
      collect1.recipients.length === expectedRecipients.length &&
      collect1.recipients.every(
        (r) => r.state === 'replied' && r.verdict === 'agree' && typeof r.repliedLatencyMs === 'number',
      );
    const tallyOk = collect1.verdictTally.agree >= expectedRecipients.length;
    check(
      4,
      'collect verdict tally agree for every recipient with latencies',
      recipientsOk && tallyOk,
      'matrix=' + JSON.stringify(collect1.recipients) + ' tally=' + JSON.stringify(collect1.verdictTally),
    );

    // Assertion 5: the fake claude-extra peer received exactly one delivery,
    // so the group ask plus the direct probe never double-fanned-out.
    const extraRows = db
      .prepare('SELECT message_id, state, latency_ms FROM deliveries WHERE recipient = ? ORDER BY message_id ASC')
      .all(EXTRA);
    const firstExtra = extraRows[0];
    const extraExactlyOne =
      extraRows.length === 1 &&
      firstExtra !== undefined &&
      firstExtra.message_id === probe.id &&
      firstExtra.state === 'replied' &&
      extra.pushes.length === 1;
    check(
      5,
      'claude-extra received exactly one delivery (group plus direct addressing deduped)',
      extraExactlyOne,
      'db rows=' + JSON.stringify(extraRows) + ' push events=' + String(extra.pushes.length) +
        ' probe message_id=' + String(probe.id),
    );

    // Assertion 6: the implementer stream carries both body strings and every
    // peer acted (replied).
    const impRun = claudeRuns.find((run) => run.label === 'implementer');
    const impStdout = impRun !== undefined ? impRun.stdout : '';
    const bodiesInStdout = impStdout.includes(ASK_BODY) && impStdout.includes(PROBE_BODY);
    let allPeersActed = extra.replies.length === 1;
    const actedDetail = ['claude-extra replied: ' + String(extra.replies.length === 1)];
    if (mode === 'fake-peers') {
      for (const handle of [PEER_A, PEER_B]) {
        const log = fakeLogs.find((entry) => entry.handle === handle);
        const acted = log !== undefined && log.steps.some((s) => s.step === 'reply');
        allPeersActed = allPeersActed && acted;
        actedDetail.push(handle + ' replied: ' + String(acted));
      }
    } else {
      for (const handle of [PEER_A, PEER_B]) {
        const run = claudeRuns.find((entry) => entry.label === (handle === PEER_A ? 'peer-a' : 'peer-b'));
        const acted = run !== undefined && replyToolUsed(run.stdout);
        allPeersActed = allPeersActed && acted;
        actedDetail.push(handle + ' murmur_reply in stream: ' + String(acted));
      }
    }
    check(
      6,
      'implementer stream carries the ask and probe bodies and every peer acted',
      bodiesInStdout && allPeersActed,
      'bodies in stdout: ' + String(bodiesInStdout) + '; ' + actedDetail.join('; '),
    );

    // Assertion 7 (probe thread): the direct probe thread collects
    // claude-extra as agree with a latency.
    const collectClient2 = new BusClient(socketPath, token);
    runState.clients.push(collectClient2);
    await collectClient2.connect();
    const collect2 = await collectClient2.request({ type: 'collect', threadId: probe.thread_id });
    collectClient2.close();
    const probeMatrix = collect2.recipients.find((r) => r.recipient === EXTRA);
    const probeOk =
      probeMatrix !== undefined &&
      probeMatrix.verdict === 'agree' &&
      probeMatrix.state === 'replied' &&
      typeof probeMatrix.repliedLatencyMs === 'number';
    check(
      7,
      'probe thread collect shows claude-extra agree with latency',
      probeOk,
      'matrix=' + JSON.stringify(collect2.recipients) + ' tally=' + JSON.stringify(collect2.verdictTally),
    );

    const failures = results.filter((r) => !r.pass);
    if (failures.length > 0) {
      const names = failures.map((r) => 'check ' + String(r.num)).join(', ');
      console.log('E2E FAIL: ' + String(failures.length) + ' of ' + String(results.length) + ' checks failed: ' + names);
      return 1;
    }
    console.log('E2E PASS (mode ' + mode + ')');
    return mode === 'fake-peers' ? 3 : 0;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.log('E2E FAIL: ' + message);
    if (runState.brokerInfo.stderr.trim().length > 0) {
      console.log('broker stderr tail: ' + runState.brokerInfo.stderr.trim().slice(-1000));
    }
    return 1;
  } finally {
    await cleanup();
  }
}

// Spawns one headless claude session. The claude process keeps the real user
// config for auth but runs in its own temp project dir, and
// --strict-mcp-config pins the murmur server to the temp-home mcp.json.
// Returns a handle: done resolves with the captured streams, kill hard-kills.
function spawnClaude(label, dir, prompt, timeoutMs, home) {
  const startedAt = Date.now();
  const env = { ...process.env, MURMUR_HOME: home };
  // Never let a stray murmur hook inside the session register a native
  // transport, and drop any inherited session handle so hook traffic cannot
  // masquerade as one of the run's peers.
  delete env.CLAUDE_CODE_MESSAGING_SOCKET;
  delete env.CLAUDE_CODE_MESSAGING_TOKEN;
  delete env.MURMUR_HANDLE;
  const child = spawn(
    'claude',
    [
      '-p',
      prompt,
      '--mcp-config',
      join(dir, 'mcp.json'),
      '--allowedTools',
      ALLOWED_TOOLS,
      '--strict-mcp-config',
      '--output-format',
      'stream-json',
      '--verbose',
    ],
    { cwd: dir, env, stdio: ['ignore', 'pipe', 'pipe'] },
  );
  runState.claudeChildren.add(child);
  let stdout = '';
  let stderr = '';
  let timedOut = false;
  child.stdout.on('data', (chunk) => {
    stdout += chunk.toString('utf8');
    if (stdout.length > 2000000) {
      stdout = stdout.slice(-1000000);
    }
  });
  child.stderr.on('data', (chunk) => {
    stderr += chunk.toString('utf8');
    if (stderr.length > 20000) {
      stderr = stderr.slice(-10000);
    }
  });
  let resolveDone;
  const done = new Promise((resolve) => {
    resolveDone = resolve;
  });
  const finish = (code, signal) => {
    clearTimeout(timer);
    if (hardKill !== null) {
      clearTimeout(hardKill);
    }
    runState.claudeChildren.delete(child);
    resolveDone({ label, code, signal, stdout, stderr, timedOut, ms: Date.now() - startedAt });
  };
  // The hard kill follows the timeout SIGTERM by 5s; nothing kills the
  // session before the timeout.
  let hardKill = null;
  const timer = setTimeout(() => {
    timedOut = true;
    try {
      child.kill('SIGTERM');
    } catch {
      // already gone
    }
    hardKill = setTimeout(() => {
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
    }, 5000);
  }, timeoutMs);
  child.once('exit', (code, signal) => finish(code, signal));
  child.once('error', (err) => {
    stderr += '\nspawn error: ' + err.message;
    finish(null, null);
  });
  return {
    label,
    done,
    kill: () => {
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
    },
  };
}

// True when the session's stream shows a murmur_reply tool call.
function replyToolUsed(stdout) {
  return /"name"\s*:\s*"(mcp__murmur__)?murmur_reply"/.test(stdout);
}

// True when the session's stream shows a murmur_thread result carrying the
// other peer's reply (sender, reply flag, ACK body). The claude CLI renders
// an MCP tool result as a JSON string inside the stream-json user event, so
// the result's quotes appear escaped there (\\"sender\\":\\"handle\\"); a
// rendering that embeds the parsed object would carry the plain form, so
// both count. The tool name itself is a stream field and never escaped,
// which is why the murmur_thread call is matched with a regex.
function peerReadEvidence(stdout, otherHandle) {
  const senderForms = ['\\"sender\\":\\"' + otherHandle + '\\"', '"sender":"' + otherHandle + '"'];
  const replyFlagForms = ['\\"isReply\\":true', '"isReply":true'];
  return (
    /"name"\s*:\s*"(mcp__murmur__)?murmur_thread"/.test(stdout) &&
    senderForms.some((form) => stdout.includes(form)) &&
    replyFlagForms.some((form) => stdout.includes(form)) &&
    stdout.includes(REPLY_BODY)
  );
}

// Polls the database until the ask is fully replied and the probe is replied
// by claude-extra.
function waitSettled(db, deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  return new Promise((resolve) => {
    const askQuery = db.prepare(
      'SELECT id FROM messages WHERE thread_id = ? AND is_reply = 0 AND sender = ? ORDER BY seq ASC LIMIT 1',
    );
    const probeQuery = db.prepare(
      'SELECT m.id FROM messages m JOIN deliveries d ON d.message_id = m.id ' +
        'WHERE m.is_reply = 0 AND m.sender = ? AND d.recipient = ? ORDER BY m.id ASC LIMIT 1',
    );
    const poll = () => {
      try {
        const ask = askQuery.get(THREAD, IMPLEMENTER);
        if (ask === undefined) {
          return retry('no ask yet');
        }
        const replied = db
          .prepare("SELECT COUNT(*) AS n FROM deliveries WHERE message_id = ? AND state = 'replied'")
          .get(ask.id).n;
        if (replied !== 2) {
          return retry('ask replies: ' + String(replied) + '/2');
        }
        const probe = probeQuery.get(IMPLEMENTER, EXTRA);
        if (probe === undefined) {
          return retry('no probe yet');
        }
        const probeReplied = db
          .prepare("SELECT COUNT(*) AS n FROM deliveries WHERE message_id = ? AND recipient = ? AND state = 'replied'")
          .get(probe.id, EXTRA).n;
        if (probeReplied !== 1) {
          return retry('probe reply: ' + String(probeReplied) + '/1');
        }
        resolve({ ok: true, detail: 'settled' });
      } catch (err) {
        resolve({ ok: false, detail: 'settle poll failed: ' + err.message });
      }
    };
    const retry = (why) => {
      if (Date.now() > deadline) {
        resolve({ ok: false, detail: why });
        return;
      }
      setTimeout(poll, 200);
    };
    poll();
  });
}

function dumpState(db) {
  try {
    const messages = db
      .prepare(
        'SELECT id, thread_id, seq, sender, is_reply, in_reply_to, verdict, substr(body, 1, 50) AS body FROM messages ORDER BY id ASC',
      )
      .all();
    const deliveries = db
      .prepare('SELECT message_id, recipient, state, latency_ms FROM deliveries ORDER BY message_id ASC, recipient ASC')
      .all();
    return 'messages=' + JSON.stringify(messages) + '; deliveries=' + JSON.stringify(deliveries);
  } catch (err) {
    return 'state dump failed: ' + err.message;
  }
}

function stopBroker(broker) {
  return new Promise((resolve) => {
    const done = () => resolve();
    if (broker.exitCode !== null || broker.signalCode !== null) {
      done();
      return;
    }
    const hard = setTimeout(() => {
      try {
        broker.kill('SIGKILL');
      } catch {
        // already gone
      }
      done();
    }, 5000);
    broker.once('exit', () => {
      clearTimeout(hard);
      done();
    });
    try {
      broker.kill('SIGTERM');
    } catch {
      clearTimeout(hard);
      done();
    }
  });
}

// Kills any process whose cwd sits inside the run tree, so a claude child
// killed mid-session cannot leave its murmur MCP server behind.
function killProcessesUnder(dir) {
  let out = '';
  try {
    const res = spawnSync('lsof', ['-n', '-w', '-d', 'cwd'], { encoding: 'utf8' });
    out = typeof res.stdout === 'string' ? res.stdout : '';
  } catch {
    return;
  }
  const killed = new Set();
  for (const line of out.split('\n')) {
    if (!line.includes(dir)) {
      continue;
    }
    const cols = line.trim().split(/\s+/);
    const pid = Number(cols[1]);
    if (!Number.isInteger(pid) || pid === process.pid || killed.has(pid)) {
      continue;
    }
    killed.add(pid);
    try {
      process.kill(pid, 'SIGKILL');
    } catch {
      // already gone
    }
  }
}

// Best effort synchronous part of cleanup, safe from signal handlers.
function hardCleanupSync() {
  for (const child of runState.claudeChildren) {
    try {
      child.kill('SIGKILL');
    } catch {
      // already gone
    }
  }
  for (const client of runState.clients) {
    try {
      client.close();
    } catch {
      // already closed
    }
  }
  if (runState.broker !== null) {
    try {
      runState.broker.kill('SIGKILL');
    } catch {
      // already gone
    }
  }
  if (runState.db !== null) {
    try {
      runState.db.close();
    } catch {
      // already closed
    }
  }
  if (runState.runRoot !== null) {
    killProcessesUnder(runState.runRoot);
    try {
      rmSync(runState.runRoot, { recursive: true, force: true });
    } catch {
      // best effort
    }
  }
}

async function cleanup() {
  for (const child of runState.claudeChildren) {
    try {
      child.kill('SIGTERM');
    } catch {
      // already gone
    }
  }
  for (const client of runState.clients) {
    try {
      client.close();
    } catch {
      // already closed
    }
  }
  if (runState.broker !== null) {
    await stopBroker(runState.broker);
  }
  if (runState.db !== null) {
    try {
      runState.db.close();
    } catch {
      // already closed
    }
  }
  if (runState.runRoot !== null) {
    killProcessesUnder(runState.runRoot);
    try {
      rmSync(runState.runRoot, { recursive: true, force: true });
    } catch {
      // best effort cleanup
    }
  }
}

process.on('unhandledRejection', (err) => {
  const message = err instanceof Error ? err.message : String(err);
  console.log('E2E FAIL: unhandled rejection: ' + message);
  hardCleanupSync();
  process.exit(1);
});
process.on('SIGINT', () => {
  console.log('E2E FAIL: interrupted');
  hardCleanupSync();
  process.exit(1);
});
process.on('SIGTERM', () => {
  console.log('E2E FAIL: terminated');
  hardCleanupSync();
  process.exit(1);
});

main().then((code) => process.exit(code));
