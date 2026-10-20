import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { createServer } from 'node:net';
import type { Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { Broker } from '../src/broker.js';
import { MurmurClient } from '../src/client.js';
import { ensureHome, getPaths, readToken } from '../src/config.js';
import type { MurmurPaths } from '../src/config.js';
import { MurmurCore } from '../src/core.js';
import { runHook } from '../src/hooks.js';
import { LineReader, MAX_BODY_CHARS } from '../src/protocol.js';
import type { ClientMessage, RegisterResult, SendResult, StatusResult } from '../src/protocol.js';
import { Store } from '../src/store.js';

// runHook against a live broker. Every scenario passes an explicit env
// object so nothing here depends on, or leaks into, the real environment.

interface Stack {
  home: string;
  paths: MurmurPaths;
  token: string;
  store: Store;
  core: MurmurCore;
  broker: Broker;
}

interface HookOutput {
  hookSpecificOutput: { hookEventName: string; additionalContext: string };
}

const stacks: Stack[] = [];
const homes: string[] = [];
const clients: MurmurClient[] = [];
const savedHome = process.env.MURMUR_HOME;

function freshHome(): string {
  const home = mkdtempSync(join(tmpdir(), 'murmur-hooks-'));
  homes.push(home);
  return home;
}

async function buildStack(): Promise<Stack> {
  const home = freshHome();
  const paths = getPaths(home);
  ensureHome(paths);
  const store = Store.open(paths.dbPath);
  const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} });
  const broker = await Broker.start({ paths, store, core });
  const stack: Stack = { home, paths, token: readToken(paths), store, core, broker };
  stacks.push(stack);
  return stack;
}

function req<T>(client: MurmurClient, msg: Record<string, unknown>): Promise<T> {
  return client.request<T>(msg as unknown as Omit<ClientMessage, 'v' | 'id'>);
}

async function connectPeer(stack: Stack, handle: string): Promise<MurmurClient> {
  const client = await MurmurClient.connect({ socketPath: stack.paths.socketPath, token: stack.token });
  clients.push(client);
  await req<RegisterResult>(client, { type: 'register', handle, clientKind: 'mcp' });
  return client;
}

async function presenceOf(viewer: MurmurClient, handle: string): Promise<string | undefined> {
  const status = await req<StatusResult>(viewer, { type: 'status' });
  return status.peers.find((p) => p.handle === handle)?.presence;
}

async function settle(pred: () => Promise<boolean> | boolean, label: string): Promise<void> {
  const deadline = Date.now() + 10000;
  while (!(await pred())) {
    if (Date.now() > deadline) {
      throw new Error('timed out waiting for ' + label);
    }
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
  }
}

function envFor(stack: Stack, extra: Record<string, string> = {}): NodeJS.ProcessEnv {
  return { MURMUR_HOME: stack.home, ...extra };
}

afterEach(async () => {
  for (const client of clients.splice(0)) {
    client.close();
  }
  for (const stack of stacks.splice(0)) {
    await stack.broker.stop();
    stack.store.close();
  }
  for (const home of homes.splice(0)) {
    rmSync(home, { recursive: true, force: true });
  }
  if (savedHome === undefined) {
    delete process.env.MURMUR_HOME;
  } else {
    process.env.MURMUR_HOME = savedHome;
  }
});

describe('session-start', () => {
  it('registers the handle as idle and skips native registration without the CLAUDE vars', async () => {
    const stack = await buildStack();
    const viewer = await connectPeer(stack, 'viewer');
    const outcome = await runHook('session-start', '{}', envFor(stack));
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    expect(stack.store.getPeer('claude-unknown')).not.toBeNull();
    expect(stack.store.getNativeTransport('claude-unknown')).toBeNull();
    await settle(async () => (await presenceOf(viewer, 'claude-unknown')) === 'idle', 'claude-unknown idle');
  });

  it('native-registers the session when both CLAUDE messaging vars are present', async () => {
    const stack = await buildStack();
    const outcome = await runHook(
      'session-start',
      '{"session_id":"sess-12345678-rest"}',
      envFor(stack, {
        CLAUDE_CODE_MESSAGING_SOCKET: '/tmp/cc-socks/4242.sock',
        CLAUDE_CODE_MESSAGING_TOKEN: 'cc-secret',
      }),
    );
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    expect(stack.store.getPeer('claude-sess-123')).not.toBeNull();
    expect(stack.store.getNativeTransport('claude-sess-123')).toEqual({
      owner: 'claude-sess-123',
      socketPath: '/tmp/cc-socks/4242.sock',
      token: 'cc-secret',
    });
  });

  it('skips native registration when either var is missing or empty', async () => {
    const stack = await buildStack();
    await runHook('session-start', '{}', envFor(stack, { MURMUR_HANDLE: 'only-socket', CLAUDE_CODE_MESSAGING_SOCKET: '/tmp/x.sock' }));
    expect(stack.store.getPeer('only-socket')).not.toBeNull();
    expect(stack.store.getNativeTransport('only-socket')).toBeNull();
    await runHook('session-start', '{}', envFor(stack, { MURMUR_HANDLE: 'only-token', CLAUDE_CODE_MESSAGING_TOKEN: 'tok' }));
    expect(stack.store.getNativeTransport('only-token')).toBeNull();
    await runHook(
      'session-start',
      '{}',
      envFor(stack, { MURMUR_HANDLE: 'empty-socket', CLAUDE_CODE_MESSAGING_SOCKET: '', CLAUDE_CODE_MESSAGING_TOKEN: 'tok' }),
    );
    expect(stack.store.getNativeTransport('empty-socket')).toBeNull();
    await runHook(
      'session-start',
      '{}',
      envFor(stack, { MURMUR_HANDLE: 'empty-token', CLAUDE_CODE_MESSAGING_SOCKET: '/tmp/x.sock', CLAUDE_CODE_MESSAGING_TOKEN: '' }),
    );
    expect(stack.store.getNativeTransport('empty-token')).toBeNull();
  });
});

describe('prompt', () => {
  it('returns the additionalContext JSON with the injected push bodies and marks them delivered', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, 'alice');
    // The first prompt creates the peer row (nothing to inject yet).
    const first = await runHook('prompt', '{}', envFor(stack, { MURMUR_HANDLE: 'agent-x' }));
    expect(first).toEqual({ exitCode: 0, stdout: '' });
    const one = await req<SendResult>(alice, { type: 'send', threadId: null, to: ['agent-x'], body: 'first body\nmore' });
    const two = await req<SendResult>(alice, { type: 'send', threadId: null, to: ['agent-x'], body: 'second body' });
    // Silent and notify messages are not injected by the prompt hook.
    await req<SendResult>(alice, { type: 'send', threadId: null, to: ['agent-x'], body: 'quiet', mode: 'silent' });
    const outcome = await runHook('prompt', 'not json at all', envFor(stack, { MURMUR_HANDLE: 'agent-x' }));
    expect(outcome.exitCode).toBe(0);
    const parsed = JSON.parse(outcome.stdout) as HookOutput;
    expect(parsed.hookSpecificOutput.hookEventName).toBe('UserPromptSubmit');
    const context = parsed.hookSpecificOutput.additionalContext;
    expect(context).toContain(
      'murmur message ' + String(one.messageId) + ' in thread ' + String(one.threadId) +
        ' seq 0 from alice (reply with murmur_reply in_reply_to ' + String(one.messageId) + '):',
    );
    expect(context).toContain('first body\nmore');
    expect(context).toContain('murmur message ' + String(two.messageId));
    expect(context).toContain('second body');
    expect(context).not.toContain('quiet');
    // Newest first, blocks separated by a blank line.
    expect(context.indexOf('second body')).toBeLessThan(context.indexOf('first body'));
    expect(context.split('\n\n').length).toBeGreaterThanOrEqual(3);
    expect(stack.store.getDeliveries(one.messageId)[0]?.state).toBe('delivered');
    expect(stack.store.getDeliveries(two.messageId)[0]?.state).toBe('delivered');
    // Nothing is left to inject.
    const again = await runHook('prompt', '{}', envFor(stack, { MURMUR_HANDLE: 'agent-x' }));
    expect(again).toEqual({ exitCode: 0, stdout: '' });
  });

  it('renders the truncated delivery body for long messages', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, 'alice');
    await runHook('prompt', '{}', envFor(stack, { MURMUR_HANDLE: 'agent-long' }));
    const body = 'z'.repeat(MAX_BODY_CHARS + 10);
    await req<SendResult>(alice, { type: 'send', threadId: null, to: ['agent-long'], body });
    const outcome = await runHook('prompt', '{}', envFor(stack, { MURMUR_HANDLE: 'agent-long' }));
    const parsed = JSON.parse(outcome.stdout) as HookOutput;
    expect(parsed.hookSpecificOutput.additionalContext).toContain('[truncated: full body stored, read with murmur read]');
    expect(parsed.hookSpecificOutput.additionalContext).not.toContain('z'.repeat(MAX_BODY_CHARS + 1));
  });

  it('marks the session busy until stop sets it idle', async () => {
    const stack = await buildStack();
    const viewer = await connectPeer(stack, 'viewer');
    await runHook('prompt', '{}', envFor(stack, { MURMUR_HANDLE: 'busy-one' }));
    await settle(async () => (await presenceOf(viewer, 'busy-one')) === 'busy', 'busy-one busy');
    const outcome = await runHook('stop', '{}', envFor(stack, { MURMUR_HANDLE: 'busy-one' }));
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    await settle(async () => (await presenceOf(viewer, 'busy-one')) === 'idle', 'busy-one idle');
  });
});

describe('handle resolution', () => {
  it('prefers MURMUR_HANDLE, then the session id prefix, then claude-unknown', async () => {
    const stack = await buildStack();
    await runHook('stop', '{"session_id":"abcdefgh-ignored"}', envFor(stack, { MURMUR_HANDLE: 'explicit' }));
    expect(stack.store.getPeer('explicit')).not.toBeNull();
    expect(stack.store.getPeer('claude-abcdefgh')).toBeNull();
    // An empty MURMUR_HANDLE does not count as explicit.
    await runHook('stop', '{"session_id":"abcdefgh-ignored"}', envFor(stack, { MURMUR_HANDLE: '' }));
    expect(stack.store.getPeer('claude-abcdefgh')).not.toBeNull();
    await runHook('stop', '{"session_id":"short"}', envFor(stack));
    expect(stack.store.getPeer('claude-short')).not.toBeNull();
    // No usable session id in any of these.
    await runHook('stop', '{"session_id":""}', envFor(stack));
    await runHook('stop', '123', envFor(stack));
    await runHook('stop', '{"other":"x"}', envFor(stack));
    await runHook('stop', '', envFor(stack));
    expect(stack.store.getPeer('claude-unknown')).not.toBeNull();
    expect(stack.store.listPeers().map((p) => p.handle).sort()).toEqual([
      'claude-abcdefgh',
      'claude-short',
      'claude-unknown',
      'explicit',
    ]);
  });

  it('falls back to the process MURMUR_HOME when the env object has none', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HOME = stack.home;
    const outcome = await runHook('stop', '{}', { MURMUR_HANDLE: 'via-process-env' });
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    expect(stack.store.getPeer('via-process-env')).not.toBeNull();
  });
});

describe('failure tolerance', () => {
  it('returns exit 0 with empty stdout when the broker is down', async () => {
    const home = freshHome();
    const paths = getPaths(home);
    ensureHome(paths);
    for (const event of ['session-start', 'prompt', 'stop'] as const) {
      const outcome = await runHook(event, '{"session_id":"abc"}', { MURMUR_HOME: home, MURMUR_HANDLE: 'nobody' });
      expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    }
  });

  it('returns exit 0 with empty stdout when the token cannot be read', async () => {
    const home = freshHome();
    mkdirSync(home, { recursive: true });
    const outcome = await runHook('prompt', '{}', { MURMUR_HOME: home, MURMUR_HANDLE: 'nobody' });
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
  });

  it('tolerates malformed stdin against a live broker', async () => {
    const stack = await buildStack();
    const outcome = await runHook('session-start', '{"session_id": oops', envFor(stack, { MURMUR_HANDLE: 'tolerant' }));
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    expect(stack.store.getPeer('tolerant')).not.toBeNull();
  });

  it('gives up on the watchdog when the broker authenticates but stops replying', async () => {
    // A peer that answers the auth handshake and then goes silent on the
    // hook request. The hook must unblock itself after HOOK_DEADLINE_MS
    // (10s) with exit 0 and empty stdout instead of hanging forever.
    const dir = freshHome();
    const paths = getPaths(dir);
    ensureHome(paths);
    const server = createServer((socket: Socket) => {
      const reader = new LineReader();
      socket.setEncoding('utf8');
      socket.on('error', () => {});
      socket.on('data', (chunk: string) => {
        for (const line of reader.feed(chunk)) {
          let parsed: { type?: string; id?: number };
          try {
            parsed = JSON.parse(line) as { type?: string; id?: number };
          } catch {
            continue;
          }
          if (parsed.type === 'auth' && typeof parsed.id === 'number') {
            socket.write(JSON.stringify({ v: 1, id: parsed.id, ok: true, result: null }) + '\n');
          }
          // The hook request itself is never answered.
        }
      });
    });
    const socketPath = join(dir, 'bus.sock');
    await new Promise<void>((resolveListen) => {
      server.listen(socketPath, () => resolveListen());
    });
    try {
      const started = Date.now();
      const outcome = await runHook('prompt', '{}', { MURMUR_HOME: dir, MURMUR_HANDLE: 'stuck' });
      expect(Date.now() - started).toBeGreaterThanOrEqual(9900);
      expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    } finally {
      await new Promise<void>((resolveClose) => {
        server.close(() => resolveClose());
      });
    }
  });
});
