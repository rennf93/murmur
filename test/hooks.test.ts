import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import type { Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Broker } from '../src/broker.js';
import { MurmurClient } from '../src/client.js';
import { ensureHome, getPaths, readToken } from '../src/config.js';
import type { MurmurPaths } from '../src/config.js';
import { MurmurCore } from '../src/core.js';
import { HOOK_CLIENTS, runHook } from '../src/hooks.js';
import type { HookClient } from '../src/hooks.js';
import { currentHost } from '../src/proc.js';
import type { HostInfo } from '../src/proc.js';
import { LineReader, MAX_BODY_CHARS } from '../src/protocol.js';
import type { ClientMessage, RegisterResult, SendResult } from '../src/protocol.js';
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
interface StopOutput {
  decision: string;
  reason: string;
}

// Only currentHost is replaced: core and the broker still use the real process table helpers.
vi.mock('../src/proc.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/proc.js')>();
  return { ...actual, currentHost: vi.fn() };
});

const HOST: HostInfo = { hostPid: 4242, hostStart: 'Tue Oct 20 06:00:00 2026', ancestors: [4242] };
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
  // No real codex or Claude socket is ever contacted: both transports refuse.
  const broker = await Broker.start({ paths, store, core, nativeSend: () => Promise.resolve({ ok: false, error: 'no' }), wake: () => Promise.resolve(false) });
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

const CLIENT_OPT = (client: HookClient): { client: HookClient; host: HostInfo } => ({ client, host: HOST });

// Queues one push message for handle from a connected peer.
async function queueFor(stack: Stack, to: string, body: string): Promise<SendResult> {
  const alice = await connectPeer(stack, 'alice-' + String(Math.random()).slice(2, 8));
  return req<SendResult>(alice, { type: 'send', threadId: null, to: [to], body });
}

describe('session events', () => {
  it('session-start records a session row from the stdin fields and allocates a handle from client and cwd', async () => {
    const stack = await buildStack();
    const outcome = await runHook(
      'session-start',
      JSON.stringify({ session_id: 'sess-1', transcript_path: '/tmp/t.jsonl', cwd: '/work/My Proj' }),
      envFor(stack),
      CLIENT_OPT('codex'),
    );
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    const row = stack.store.sessionByHost(HOST.hostPid, HOST.hostStart);
    expect(row).toMatchObject({
      client: 'codex',
      cliSessionId: 'sess-1',
      handle: 'codex-my-proj',
      transcriptPath: '/tmp/t.jsonl',
      cwd: '/work/My Proj',
      state: 'idle',
    });
  });

  it('defaults the client to claude, and a bare session gets the plain client handle', async () => {
    const stack = await buildStack();
    await runHook('session-start', '{}', envFor(stack), { host: HOST });
    expect(stack.store.sessionByHost(HOST.hostPid, HOST.hostStart)?.handle).toBe('claude');
  });

  it('prompt marks the session busy, stop idle, and session-end ended with empty output', async () => {
    const stack = await buildStack();
    const stateOf = (): string | undefined => stack.store.sessionByHost(HOST.hostPid, HOST.hostStart)?.state;
    await runHook('prompt', '{}', envFor(stack, { MURMUR_HANDLE: 'busy-one' }), CLIENT_OPT('claude'));
    expect(stateOf()).toBe('busy');
    await runHook('stop', '{}', envFor(stack, { MURMUR_HANDLE: 'busy-one' }), CLIENT_OPT('claude'));
    expect(stateOf()).toBe('idle');
    const ended = await runHook('session-end', '{}', envFor(stack, { MURMUR_HANDLE: 'busy-one' }), CLIENT_OPT('claude'));
    expect(ended).toEqual({ exitCode: 0, stdout: '' });
    expect(stateOf()).toBe('ended');
  });

  it('MURMUR_HANDLE names the session; an empty one does not', async () => {
    const stack = await buildStack();
    await runHook('stop', '{}', envFor(stack, { MURMUR_HANDLE: 'explicit' }), CLIENT_OPT('claude'));
    expect(stack.store.sessionByHost(HOST.hostPid, HOST.hostStart)?.handle).toBe('explicit');
    const other: HostInfo = { hostPid: 4243, hostStart: HOST.hostStart, ancestors: [4243] };
    await runHook('stop', '{}', envFor(stack, { MURMUR_HANDLE: '' }), { client: 'claude', host: other });
    expect(stack.store.sessionByHost(4243, HOST.hostStart)?.handle).toBe('claude');
  });

  it('keeps transcriptPath for every client except zcode', async () => {
    const stack = await buildStack();
    for (const [i, client] of HOOK_CLIENTS.entries()) {
      const host: HostInfo = { hostPid: 5000 + i, hostStart: HOST.hostStart, ancestors: [] };
      await runHook('session-start', '{"transcript_path":"/tmp/x.jsonl"}', envFor(stack, { MURMUR_HANDLE: 'h-' + client }), { client, host });
      expect(stack.store.sessionByHost(host.hostPid, host.hostStart)?.transcriptPath).toBe(client === 'zcode' ? null : '/tmp/x.jsonl');
    }
  });

  it('tolerates malformed stdin against a live broker', async () => {
    const stack = await buildStack();
    for (const stdin of ['{"session_id": oops', '123', '', 'null', '{"session_id":""}']) {
      const outcome = await runHook('session-start', stdin, envFor(stack, { MURMUR_HANDLE: 'tolerant' }), CLIENT_OPT('claude'));
      expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    }
    expect(stack.store.getPeer('tolerant')).not.toBeNull();
  });
});

describe('native registration', () => {
  const native = { CLAUDE_CODE_MESSAGING_SOCKET: '/tmp/cc-socks/4242.sock', CLAUDE_CODE_MESSAGING_TOKEN: 'cc-secret' };

  it('registers the native transport only for claude session-start with both vars', async () => {
    const stack = await buildStack();
    await runHook('session-start', '{}', envFor(stack, { MURMUR_HANDLE: 'nat', ...native }), CLIENT_OPT('claude'));
    expect(stack.store.getNativeTransport('nat')).toEqual({ owner: 'nat', socketPath: '/tmp/cc-socks/4242.sock', token: 'cc-secret' });
  });

  it('skips it for other events, other clients, and a missing or empty var', async () => {
    const stack = await buildStack();
    const cases: Array<[string, HookClient, 'prompt' | 'session-start', Record<string, string>]> = [
      ['n-prompt', 'claude', 'prompt', native],
      ['n-codex', 'codex', 'session-start', native],
      ['n-nosock', 'claude', 'session-start', { CLAUDE_CODE_MESSAGING_TOKEN: 't' }],
      ['n-notoken', 'claude', 'session-start', { CLAUDE_CODE_MESSAGING_SOCKET: '/tmp/x.sock' }],
      ['n-emptysock', 'claude', 'session-start', { CLAUDE_CODE_MESSAGING_SOCKET: '', CLAUDE_CODE_MESSAGING_TOKEN: 't' }],
      ['n-emptytok', 'claude', 'session-start', { CLAUDE_CODE_MESSAGING_SOCKET: '/tmp/x.sock', CLAUDE_CODE_MESSAGING_TOKEN: '' }],
    ];
    for (const [i, [handle, client, event, vars]] of cases.entries()) {
      const host: HostInfo = { hostPid: 6000 + i, hostStart: HOST.hostStart, ancestors: [] };
      await runHook(event, '{}', envFor(stack, { MURMUR_HANDLE: handle, ...vars }), { client, host });
      expect(stack.store.getNativeTransport(handle)).toBeNull();
    }
  });
});

describe('output dialects', () => {
  const clientHost = (i: number): HostInfo => ({ hostPid: 7000 + i, hostStart: HOST.hostStart, ancestors: [] });

  it('hands queued messages back in each client dialect for each event', async () => {
    const stack = await buildStack();
    let n = 0;
    for (const client of HOOK_CLIENTS) {
      for (const event of ['stop', 'session-start', 'prompt'] as const) {
        n += 1;
        const handle = 'dl-' + String(n);
        const host = clientHost(n);
        // Create the session first (a stop leaves it idle and returns nothing).
        await runHook('stop', '{}', envFor(stack, { MURMUR_HANDLE: handle }), { client, host });
        const sent = await queueFor(stack, handle, 'body ' + String(n));
        const outcome = await runHook(event, '{}', envFor(stack, { MURMUR_HANDLE: handle }), { client, host });
        expect(outcome.exitCode).toBe(0);
        const parsed = JSON.parse(outcome.stdout) as Record<string, unknown>;
        const header = 'murmur: 1 message(s) for ' + handle + '\n\n[murmur m' + String(sent.messageId) + ' ';
        if (event === 'stop') {
          expect(parsed).toEqual({ decision: client === 'gemini' ? 'deny' : 'block', reason: expect.stringContaining(header) });
          expect((parsed as unknown as StopOutput).reason).toContain('body ' + String(n));
        } else {
          const name = event === 'session-start' ? 'SessionStart' : client === 'gemini' ? 'BeforeAgent' : 'UserPromptSubmit';
          const out = parsed as unknown as HookOutput;
          expect(out.hookSpecificOutput.hookEventName).toBe(name);
          expect(out.hookSpecificOutput.additionalContext).toContain(header);
          expect(out.hookSpecificOutput.additionalContext).toContain('body ' + String(n));
        }
        expect(stack.store.getDeliveries(sent.messageId)[0]?.state).toBe('delivered');
      }
    }
  });

  it('joins several messages and returns nothing once they are delivered', async () => {
    const stack = await buildStack();
    const env = envFor(stack, { MURMUR_HANDLE: 'agent-x' });
    await runHook('stop', '{}', env, CLIENT_OPT('claude'));
    const one = await queueFor(stack, 'agent-x', 'first body');
    const two = await queueFor(stack, 'agent-x', 'second body');
    const outcome = await runHook('prompt', '{}', env, CLIENT_OPT('claude'));
    const context = (JSON.parse(outcome.stdout) as HookOutput).hookSpecificOutput.additionalContext;
    expect(context.startsWith('murmur: 2 message(s) for agent-x')).toBe(true);
    expect(context).toContain('[murmur m' + String(one.messageId) + ' ');
    expect(context).toContain('[murmur m' + String(two.messageId) + ' ');
    expect(await runHook('prompt', '{}', env, CLIENT_OPT('claude'))).toEqual({ exitCode: 0, stdout: '' });
  });

  it('renders the truncated delivery body for long messages', async () => {
    const stack = await buildStack();
    const env = envFor(stack, { MURMUR_HANDLE: 'agent-long' });
    await runHook('stop', '{}', env, CLIENT_OPT('claude'));
    await queueFor(stack, 'agent-long', 'z'.repeat(MAX_BODY_CHARS + 10));
    const outcome = await runHook('prompt', '{}', env, CLIENT_OPT('claude'));
    const context = (JSON.parse(outcome.stdout) as HookOutput).hookSpecificOutput.additionalContext;
    expect(context).toContain('[truncated: full body stored, read it with murmur_thread full: true]');
    expect(context).not.toContain('z'.repeat(MAX_BODY_CHARS + 1));
  });

  it('returns empty output when the fetch finds nothing', async () => {
    const stack = await buildStack();
    const outcome = await runHook('session-start', '{}', envFor(stack, { MURMUR_HANDLE: 'quiet' }), CLIENT_OPT('claude'));
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
  });
});

describe('host resolution', () => {
  it('exits silently without creating anything when the host is null', async () => {
    const stack = await buildStack();
    const outcome = await runHook('session-start', '{}', envFor(stack, { MURMUR_HANDLE: 'nohost' }), { client: 'claude', host: null });
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    expect(stack.store.getPeer('nohost')).toBeNull();
  });

  it('uses currentHost when no host is passed, with and without opts', async () => {
    const stack = await buildStack();
    const mocked = vi.mocked(currentHost);
    mocked.mockReturnValue(HOST);
    await runHook('stop', '{}', envFor(stack, { MURMUR_HANDLE: 'via-current' }));
    expect(stack.store.sessionByHost(HOST.hostPid, HOST.hostStart)?.handle).toBe('via-current');
    mocked.mockReturnValue(null);
    const silent = await runHook('stop', '{}', envFor(stack, { MURMUR_HANDLE: 'via-null' }), { client: 'codex' });
    expect(silent).toEqual({ exitCode: 0, stdout: '' });
    expect(stack.store.getPeer('via-null')).toBeNull();
    expect(mocked).toHaveBeenCalledTimes(2);
    mocked.mockReset();
  });
});

describe('home resolution', () => {
  it('falls back to the process MURMUR_HOME when the env object has none', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HOME = stack.home;
    const outcome = await runHook('stop', '{}', { MURMUR_HANDLE: 'via-process-env' }, CLIENT_OPT('claude'));
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    expect(stack.store.getPeer('via-process-env')).not.toBeNull();
  });
});

// A stand-in broker that answers auth, then lets the script decide.
async function fakeBroker(
  dir: string,
  answer: (msg: { type?: string; id?: number }, reply: (result: unknown) => void) => void,
): Promise<() => Promise<void>> {
  const server = createServer((socket: Socket) => {
    const reader = new LineReader();
    socket.setEncoding('utf8');
    socket.on('error', () => {});
    socket.on('data', (chunk: string) => {
      for (const line of reader.feed(chunk)) {
        const msg = JSON.parse(line) as { type?: string; id?: number };
        const reply = (result: unknown): void => {
          socket.write(JSON.stringify({ v: 2, id: msg.id, ok: true, result }) + '\n');
        };
        if (msg.type === 'auth') {
          reply(null);
        } else {
          answer(msg, reply);
        }
      }
    });
  });
  await new Promise<void>((resolveListen) => {
    server.listen(join(dir, 'bus.sock'), () => resolveListen());
  });
  return () =>
    new Promise<void>((resolveClose) => {
      server.close(() => resolveClose());
    });
}

describe('failure tolerance', () => {
  it('returns exit 0 with empty stdout when the broker is down', async () => {
    const home = freshHome();
    ensureHome(getPaths(home));
    for (const event of ['session-start', 'prompt', 'stop', 'session-end'] as const) {
      const outcome = await runHook(event, '{"session_id":"abc"}', { MURMUR_HOME: home, MURMUR_HANDLE: 'nobody' }, CLIENT_OPT('claude'));
      expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    }
  });

  it('returns exit 0 with empty stdout when the token cannot be read', async () => {
    const home = freshHome();
    mkdirSync(home, { recursive: true });
    const outcome = await runHook('prompt', '{}', { MURMUR_HOME: home }, CLIENT_OPT('claude'));
    expect(outcome).toEqual({ exitCode: 0, stdout: '' });
  });

  it('treats a fetch-push result without texts as empty', async () => {
    const dir = freshHome();
    ensureHome(getPaths(dir));
    const close = await fakeBroker(dir, (msg, reply) => reply(msg.type === 'session' ? { handle: 'fake' } : {}));
    try {
      const outcome = await runHook('prompt', '{}', { MURMUR_HOME: dir }, CLIENT_OPT('claude'));
      expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    } finally {
      await close();
    }
  });

  it('gives up on the watchdog when the broker authenticates but stops replying', async () => {
    // The hook must unblock itself after HOOK_DEADLINE_MS (10s) with exit 0
    // and empty stdout instead of hanging forever.
    const dir = freshHome();
    ensureHome(getPaths(dir));
    const close = await fakeBroker(dir, () => {});
    try {
      const started = Date.now();
      const outcome = await runHook('prompt', '{}', { MURMUR_HOME: dir }, CLIENT_OPT('claude'));
      expect(Date.now() - started).toBeGreaterThanOrEqual(9900);
      expect(outcome).toEqual({ exitCode: 0, stdout: '' });
    } finally {
      await close();
    }
  });
});

describe('claude session titles', () => {
  // The shape Claude writes on /rename; agent-name entries are a different type.
  const entry = (customTitle: unknown): string => JSON.stringify({ type: 'custom-title', customTitle, sessionId: 's' }) + '\n';

  async function startWith(client: HookClient, transcript: string | null, stack?: Stack): Promise<string> {
    const st = stack ?? (await buildStack());
    const dir = freshHome();
    const path = join(dir, 'transcript.jsonl');
    if (transcript !== null) {
      writeFileSync(path, transcript);
    }
    await runHook('session-start', JSON.stringify({ session_id: 's', transcript_path: path, cwd: '/work/proj' }), envFor(st), CLIENT_OPT(client));
    return st.store.sessionByHost(HOST.hostPid, HOST.hostStart)?.handle as string;
  }

  it('takes the last customTitle from the transcript', async () => {
    const lines =
      entry('Old') +
      '{"type":"user","note":"custom-title but another entry"}\n' +
      '{"type":"agent-name","agentName":"clear-conversation","custom-title":1}\n' +
      '{"type":"custom-title" broken\n' +
      entry('Ledger - Claude 3') +
      entry(7);
    expect(await startWith('claude', lines)).toBe('ledger-claude-3');
  });

  it('reads only the tail of a large transcript, skipping the line it cuts', async () => {
    const filler = 'x'.repeat(1000) + '\n';
    const big = entry('Too Early') + filler.repeat(300) + entry('Tail Name');
    expect(await startWith('claude', big)).toBe('tail-name');
    const cut = 'y'.repeat(300_000) + entry('cut line').slice(5) + entry('Kept');
    expect(await startWith('claude', cut, await buildStack())).toBe('kept');
  });

  it('means no title for a missing file, no entry, or a client other than claude', async () => {
    expect(await startWith('claude', null)).toBe('claude-proj');
    expect(await startWith('claude', '{"type":"user"}\n', await buildStack())).toBe('claude-proj');
    expect(await startWith('codex', entry('Nope'), await buildStack())).toBe('codex-proj');
  });
});
