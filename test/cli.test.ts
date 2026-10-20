import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { Broker } from '../src/broker.js';
import * as cliModule from '../src/cli.js';
import { runCli } from '../src/cli.js';
import type { CliIo } from '../src/cli.js';
import { MurmurClient } from '../src/client.js';
import { ensureHome, getPaths, readToken } from '../src/config.js';
import type { MurmurPaths } from '../src/config.js';
import { MurmurCore } from '../src/core.js';
import { INSTALL_CLIENTS } from '../src/install.js';
import type { HostInfo, ProcInfo } from '../src/proc.js';
import type { ClientMessage, RegisterResult, SendResult, ServerEvent } from '../src/protocol.js';
import { Store } from '../src/store.js';

// runCli with injected io against a real broker stack. Three contained
// stand-ins keep the process clean: child_process.spawn (serve --daemon
// must not fork a real detached broker), runMcpServer (the mcp command
// would bind stdio and never return), and os.homedir (install must never
// touch the real ~/.claude.json).

const spawnState = vi.hoisted(() => ({
  calls: [] as Array<{ cmd: string; args: string[]; opts: Record<string, unknown> }>,
  unrefs: 0,
}));

vi.mock('node:child_process', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:child_process')>();
  return {
    ...actual,
    spawn: (cmd: string, args: string[], opts: Record<string, unknown>) => {
      spawnState.calls.push({ cmd, args, opts });
      return {
        unref: () => {
          spawnState.unrefs += 1;
        },
      };
    },
  };
});

const mcpState = vi.hoisted(() => ({ runs: 0, mode: 'ok' as 'ok' | 'throw-string' }));

vi.mock('../src/mcp.js', () => ({
  runMcpServer: async () => {
    mcpState.runs += 1;
    if (mcpState.mode === 'throw-string') {
      throw 'mcp exploded as a string';
    }
  },
}));

// currentHost is what identity depends on (hooks, send, reply, name); the
// broker keeps the real process helpers but reads a fixed table, so the
// sessions the tests announce are "alive" without real processes.
const hostState = vi.hoisted(() => ({ host: null as unknown }));

vi.mock('../src/proc.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/proc.js')>();
  return { ...actual, currentHost: () => hostState.host };
});

const osState = vi.hoisted(() => ({ home: '' }));

vi.mock('node:os', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:os')>();
  const fs = await import('node:fs');
  const path = await import('node:path');
  osState.home = fs.mkdtempSync(path.join(actual.tmpdir(), 'murmur-cli-home-'));
  return { ...actual, homedir: () => osState.home };
});

interface Capture {
  out: string[];
  err: string[];
  io: CliIo;
}

function capture(): Capture {
  const out: string[] = [];
  const err: string[] = [];
  return {
    out,
    err,
    io: {
      out: (s: string) => {
        out.push(s);
      },
      err: (s: string) => {
        err.push(s);
      },
    },
  };
}

function req<T>(client: MurmurClient, msg: Record<string, unknown>): Promise<T> {
  return client.request<T>(msg as unknown as Omit<ClientMessage, 'v' | 'id'>);
}

// Swaps process.stdin for a finite Readable so cmdHook's readAllStdin sees
// an end event (the worker's real stdin is a pipe that never ends).
async function withStdin<T>(text: string, fn: () => Promise<T>): Promise<T> {
  const original = Object.getOwnPropertyDescriptor(process, 'stdin');
  const readable = new Readable({
    read: () => {},
  });
  readable.push(text);
  readable.push(null);
  Object.defineProperty(process, 'stdin', { value: readable, configurable: true, enumerable: true });
  try {
    return await fn();
  } finally {
    if (original !== undefined) {
      Object.defineProperty(process, 'stdin', original);
    }
  }
}

// Announces an agent session to the broker the way a session-start hook does.
async function startSession(handle: string, host: HostInfo, client = 'claude'): Promise<void> {
  const conn = await MurmurClient.connect({ socketPath: paths.socketPath, token });
  clients.push(conn);
  await req(conn, {
    type: 'session',
    event: 'session-start',
    client,
    cliSessionId: null,
    host: { hostPid: host.hostPid, hostStart: host.hostStart },
    handle,
  });
}

async function settle(pred: () => boolean | Promise<boolean>, label: string): Promise<void> {
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

const FAKE_TABLE = new Map<number, ProcInfo>([
  [9001, { ppid: 1, start: 'S1', comm: 'claude' }],
  [9002, { ppid: 1, start: 'S2', comm: 'codex' }],
]);
const hostOf = (pid: number, start: string, ancestors: number[] = [pid]): HostInfo => ({ hostPid: pid, hostStart: start, ancestors });

let home = '';
let paths: MurmurPaths;
let token = '';
let store: Store;
let broker: Broker;
let initCapture: Capture;
const clients: MurmurClient[] = [];
const extraHomes: string[] = [];
const savedHome = process.env.MURMUR_HOME;
const savedHandle = process.env.MURMUR_HANDLE;

async function connectPeer(handle: string, display?: string): Promise<{ client: MurmurClient; events: ServerEvent[] }> {
  const events: ServerEvent[] = [];
  const client = await MurmurClient.connect({
    socketPath: paths.socketPath,
    token,
    onEvent: (ev) => {
      events.push(ev);
    },
  });
  clients.push(client);
  const msg: Record<string, unknown> = { type: 'register', handle, clientKind: 'mcp' };
  if (display !== undefined) {
    msg.display = display;
  }
  await req<RegisterResult>(client, msg);
  return { client, events };
}

beforeAll(async () => {
  home = mkdtempSync(join(tmpdir(), 'murmur-cli-'));
  process.env.MURMUR_HOME = home;
  paths = getPaths(home);
  // init runs before any broker exists: it creates the home, the token and
  // the owner peer on its own.
  initCapture = capture();
  const code = await runCli(['init'], initCapture.io);
  expect(code).toBe(0);
  store = Store.open(paths.dbPath);
  const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, undefined, () => FAKE_TABLE);
  broker = await Broker.start({ paths, store, core });
  token = readToken(paths);
});

afterEach(() => {
  hostState.host = null;
  process.env.MURMUR_HOME = home;
  if (savedHandle === undefined) {
    delete process.env.MURMUR_HANDLE;
  } else {
    process.env.MURMUR_HANDLE = savedHandle;
  }
  vi.restoreAllMocks();
});

afterAll(async () => {
  for (const client of clients.splice(0)) {
    client.close();
  }
  await broker.stop();
  store.close();
  rmSync(home, { recursive: true, force: true });
  for (const extra of extraHomes.splice(0)) {
    rmSync(extra, { recursive: true, force: true });
  }
  rmSync(osState.home, { recursive: true, force: true });
  if (savedHome === undefined) {
    delete process.env.MURMUR_HOME;
  } else {
    process.env.MURMUR_HOME = savedHome;
  }
});

describe('entry guard', () => {
  it('does not run the CLI on import when argv[1] points elsewhere', () => {
    const entryUrl = pathToFileURL(process.argv[1] ?? '').href;
    expect(entryUrl).not.toBe(new URL('../src/cli.ts', import.meta.url).href);
    expect(typeof cliModule.runCli).toBe('function');
    expect(Object.keys(cliModule)).toEqual(['runCli']);
  });

  it('runs the CLI and exits with its code when imported as the entry module', async () => {
    const cliPath = fileURLToPath(new URL('../src/cli.ts', import.meta.url));
    const exits: number[] = [];
    const exitSpy = vi.spyOn(process, 'exit').mockImplementation(((code?: number | string | null) => {
      exits.push(Number(code));
      return undefined as never;
    }) as (code?: number | string | null) => never);
    const stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
    const savedArgv = process.argv;
    try {
      // First, an import with argv[1] unset: the entry guard's fallback
      // spelling is evaluated and must not run the CLI.
      process.argv = ['murmur', undefined as unknown as string, 'definitely-not-a-command'];
      await import('../src/cli.ts?murmur-entry-unset');
      await new Promise((resolve) => {
        setTimeout(resolve, 25);
      });
      // Then the entry spelling: under vitest a query string on the dynamic
      // import may or may not show up in import.meta.url, so both spellings
      // of argv[1] are tried until the CLI runs.
      for (const entry of [cliPath, cliPath + '?murmur-entry-probe']) {
        if (exits.length > 0) {
          break;
        }
        process.argv = ['murmur', entry, 'definitely-not-a-command'];
        await import('../src/cli.ts?murmur-entry-probe=' + String(exits.length));
        await new Promise((resolve) => {
          setTimeout(resolve, 25);
        });
      }
    } finally {
      process.argv = savedArgv;
      exitSpy.mockRestore();
      stderrSpy.mockRestore();
    }
    expect(exits).toEqual([1]);
  });
});

describe('argv holes', () => {
  it('send stops flag parsing at a hole and reports missing targets', async () => {
    const cap = capture();
    const argv = ['send', '--mode', 'push', undefined as unknown as string];
    expect(await runCli(argv, cap.io)).toBe(1);
    expect(cap.err.join('')).toContain('murmur send: missing targets');
  });

  it('thread stops flag parsing at a hole and reports the missing id', async () => {
    const cap = capture();
    const argv = ['thread', '--full', undefined as unknown as string];
    expect(await runCli(argv, cap.io)).toBe(1);
    expect(cap.err.join('')).toContain('murmur thread: missing thread id');
  });
});

describe('init', () => {
  it('printed the home and created the owner peer, token and database', () => {
    expect(initCapture.out.join('')).toBe(home + '\n');
    expect(initCapture.err).toEqual([]);
    expect(existsSync(paths.tokenPath)).toBe(true);
    expect(existsSync(paths.dbPath)).toBe(true);
    const renn = store.getPeer('renn');
    expect(renn).not.toBeNull();
    expect(renn?.isOwner).toBe(true);
  });

  it('is idempotent and keeps the token', async () => {
    const before = readFileSync(paths.tokenPath, 'utf8');
    const cap = capture();
    expect(await runCli(['init'], cap.io)).toBe(0);
    expect(cap.out.join('')).toBe(home + '\n');
    expect(readFileSync(paths.tokenPath, 'utf8')).toBe(before);
    expect(store.getPeer('renn')?.isOwner).toBe(true);
  });
});

describe('usage and unknown commands', () => {
  it('prints usage and exits 1 with no command', async () => {
    const cap = capture();
    expect(await runCli([], cap.io)).toBe(1);
    expect(cap.out).toEqual([]);
    expect(cap.err.join('')).toContain('usage: murmur <command> [args]');
    expect(cap.err.join('')).toContain('install <claude|codex|gemini|zcode|opencode|hummin>');
  });

  it('names an unknown command and exits 1', async () => {
    const cap = capture();
    expect(await runCli(['frobnicate'], cap.io)).toBe(1);
    expect(cap.err.join('')).toContain('murmur: unknown command: frobnicate');
    expect(cap.err.join('')).toContain('usage: murmur <command> [args]');
  });

  it('falls back to the process streams when no io is injected', async () => {
    const stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
    const stdoutSpy = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
    expect(await runCli(['nope'])).toBe(1);
    expect(stderrSpy).toHaveBeenCalled();
    expect(String(stderrSpy.mock.calls[0]?.[0])).toContain('unknown command: nope');
    expect(await runCli(['init'])).toBe(0);
    expect(String(stdoutSpy.mock.calls[0]?.[0])).toBe(home + '\n');
    // Partial io: only out injected, err falls back to the stream.
    const out: string[] = [];
    expect(
      await runCli(['nope'], {
        out: (s: string) => {
          out.push(s);
        },
      }),
    ).toBe(1);
    expect(out).toEqual([]);
    expect(stderrSpy.mock.calls.length).toBeGreaterThanOrEqual(2);
  });
});

describe('send', () => {
  it('parses comma targets and a multi word body, printing one line per delivery', async () => {
    await connectPeer('alice');
    await connectPeer('bob', 'Bob B');
    const cap = capture();
    const code = await runCli(['send', 'alice, ,bob', 'hello', 'multi', 'word', 'body'], cap.io);
    expect(code).toBe(0);
    expect(cap.err).toEqual([]);
    const lines = cap.out.join('').split('\n').filter((l) => l.length > 0);
    expect(lines).toHaveLength(3);
    expect(lines[0]).toBe('alice: queued');
    expect(lines[1]).toBe('bob: queued');
    expect(lines[2]).toMatch(/^message \d+ thread \d+ seq 0$/);
    const match = /^message (\d+) thread (\d+) seq 0$/.exec(lines[2] ?? '');
    const messageId = Number(match?.[1]);
    const row = store.messageById(messageId);
    expect(row?.body).toBe('hello multi word body');
    expect(row?.sender).toBe('renn');
    expect(row?.mode).toBe('push');
    expect(store.getDeliveries(messageId).map((d) => d.recipient)).toEqual(['alice', 'bob']);
    expect(store.getPeer('renn')?.isOwner).toBe(true);
  });

  it('honors --mode, --refs and --thread flags anywhere in the arguments', async () => {
    const existing = store.threadMessages(1, null);
    expect(existing.length).toBeGreaterThan(0);
    const cap = capture();
    const code = await runCli(
      ['send', '--mode', 'notify', 'bob', 'second', '--refs', 'alice, carol,', 'message', 'here', '--thread', '1'],
      cap.io,
    );
    expect(code).toBe(0);
    const lines = cap.out.join('').split('\n').filter((l) => l.length > 0);
    expect(lines[0]).toBe('bob: queued');
    expect(lines[1]).toMatch(/^message \d+ thread 1 seq 1$/);
    const messageId = Number(/^message (\d+)/.exec(lines[1] ?? '')?.[1]);
    const row = store.messageById(messageId);
    expect(row?.body).toBe('second message here');
    expect(row?.mode).toBe('notify');
    expect(row?.threadId).toBe(1);
    expect(JSON.parse(row?.refsJson ?? '[]')).toEqual(['alice', 'carol']);
  });

  it('rejects flags without values and bad thread ids', async () => {
    const noValue = capture();
    expect(await runCli(['send', 'bob', 'hi', '--mode'], noValue.io)).toBe(1);
    expect(noValue.err.join('')).toContain('murmur send: flag --mode needs a value');
    expect(noValue.err.join('')).toContain('usage: murmur send <targets> <body...>');
    const badThread = capture();
    expect(await runCli(['send', 'bob', 'hi', '--thread', 'x'], badThread.io)).toBe(1);
    expect(badThread.err.join('')).toContain('murmur send: --thread needs a non-negative integer, got: x');
    const negative = capture();
    expect(await runCli(['send', '--thread', '-4', 'bob', 'hi'], negative.io)).toBe(1);
    expect(negative.err.join('')).toContain('non-negative integer, got: -4');
  });

  it('requires targets and a body', async () => {
    const noTargets = capture();
    expect(await runCli(['send'], noTargets.io)).toBe(1);
    expect(noTargets.err.join('')).toContain('murmur send: missing targets');
    const noBody = capture();
    expect(await runCli(['send', 'bob'], noBody.io)).toBe(1);
    expect(noBody.err.join('')).toContain('murmur send: missing body');
    const onlyFlags = capture();
    expect(await runCli(['send', '--mode', 'push'], onlyFlags.io)).toBe(1);
    expect(onlyFlags.err.join('')).toContain('missing targets');
  });

  it('exits 1 with the broker message on a MurmurError', async () => {
    const cap = capture();
    expect(await runCli(['send', 'nobody-here', 'hi', 'there'], cap.io)).toBe(1);
    expect(cap.out).toEqual([]);
    expect(cap.err.join('')).toBe('unknown handle: nobody-here\n');
  });

  it('registers and sends as another handle with --as', async () => {
    // A dedicated recipient so the unread counts the status test pins stay
    // untouched.
    await connectPeer('probe-b');
    const cap = capture();
    const code = await runCli(['send', '--as', 'probe-a', 'probe-b', 'hello', 'from', 'probe'], cap.io);
    expect(code).toBe(0);
    expect(cap.err).toEqual([]);
    const lines = cap.out.join('').split('\n').filter((l) => l.length > 0);
    expect(lines[0]).toBe('probe-b: queued');
    expect(lines[1]).toMatch(/^message \d+ thread \d+ seq 0$/);
    const messageId = Number(/^message (\d+)/.exec(lines[1] ?? '')?.[1]);
    // The register went out under probe-a: a fresh peer row with no display
    // name, and the message row records probe-a as the sender.
    expect(store.getPeer('probe-a')?.display).toBeNull();
    expect(store.getPeer('probe-a')?.isOwner).toBe(false);
    expect(store.messageById(messageId)?.sender).toBe('probe-a');
  });

  it('rejects an invalid --as handle with exit 1 before connecting', async () => {
    const cap = capture();
    expect(await runCli(['send', '--as', 'Bad_Handle', 'alice', 'hi'], cap.io)).toBe(1);
    expect(cap.err.join('')).toBe('invalid handle: Bad_Handle\n');
    expect(cap.out).toEqual([]);
    expect(store.getPeer('Bad_Handle')).toBeNull();
  });
});

describe('status', () => {
  it('prints one line per peer with presence, unread and optional display', async () => {
    const cap = capture();
    expect(await runCli(['status'], cap.io)).toBe(0);
    const lines = cap.out.join('').split('\n').filter((l) => l.length > 0);
    const alice = lines.find((l) => l.startsWith('alice '));
    const bob = lines.find((l) => l.startsWith('bob '));
    const renn = lines.find((l) => l.startsWith('renn '));
    expect(alice).toBe('alice online unread 1');
    expect(bob).toBe('bob online unread 2 Bob B');
    // The CLI connection for send has already closed: renn is seen recently
    // but has no live connection.
    expect(renn).toBe('renn presumed-offline unread 0');
  });
});

describe('thread', () => {
  it('prints a digest, full bodies and honors --since', async () => {
    const cap = capture();
    expect(await runCli(['send', 'alice', 'line one\nline two', 'tail'], cap.io)).toBe(0);
    const threadId = Number(/thread (\d+) seq/.exec(cap.out.join(''))?.[1]);
    const digest = capture();
    expect(await runCli(['thread', String(threadId)], digest.io)).toBe(0);
    expect(digest.out.join('')).toBe('0 renn line one\n');
    const full = capture();
    expect(await runCli(['thread', String(threadId), '--full'], full.io)).toBe(0);
    expect(full.out.join('')).toBe('0 renn line one\nline two tail\n');
    const since = capture();
    expect(await runCli(['thread', '1', '--since', '0'], since.io)).toBe(0);
    expect(since.out.join('')).toBe('1 renn second message here\n');
    const all = capture();
    expect(await runCli(['thread', '1'], all.io)).toBe(0);
    expect(all.out.join('').split('\n').filter((l) => l.length > 0)).toEqual([
      '0 renn hello multi word body',
      '1 renn second message here',
    ]);
  });

  it('reads one question and its replies with --message', async () => {
    const cap = capture();
    expect(await runCli(['send', 'alice', 'q-one', 'body'], cap.io)).toBe(0);
    const [, messageId, threadId] = /message (\d+) thread (\d+) seq/.exec(cap.out.join('')) ?? [];
    const other = capture();
    expect(await runCli(['send', '--thread', String(threadId), 'alice', 'q-two', 'body'], other.io)).toBe(0);
    const one = capture();
    expect(await runCli(['thread', String(threadId), '--message', String(messageId)], one.io)).toBe(0);
    expect(one.out.join('')).toBe('0 renn q-one body\n');
    const foreign = capture();
    expect(await runCli(['thread', String(threadId), '--message', '987654'], foreign.io)).toBe(1);
    expect(foreign.err.join('')).toBe('message 987654 is not in thread ' + String(threadId) + '\n');
    const noValue = capture();
    expect(await runCli(['thread', String(threadId), '--message'], noValue.io)).toBe(1);
    expect(noValue.err.join('')).toContain('--message needs a non-negative integer');
    const bad = capture();
    expect(await runCli(['thread', String(threadId), '--message', 'z'], bad.io)).toBe(1);
    expect(bad.err.join('')).toContain('--message needs a non-negative integer');
  });

  it('validates its arguments', async () => {
    const missing = capture();
    expect(await runCli(['thread'], missing.io)).toBe(1);
    expect(missing.err.join('')).toContain('murmur thread: missing thread id');
    const notNumber = capture();
    expect(await runCli(['thread', 'abc'], notNumber.io)).toBe(1);
    expect(notNumber.err.join('')).toContain('thread id must be a non-negative integer, got: abc');
    const sinceNoValue = capture();
    expect(await runCli(['thread', '1', '--since'], sinceNoValue.io)).toBe(1);
    expect(sinceNoValue.err.join('')).toContain('murmur thread: --since needs a value');
    const sinceBad = capture();
    expect(await runCli(['thread', '1', '--since', 'z'], sinceBad.io)).toBe(1);
    expect(sinceBad.err.join('')).toContain('--since needs a non-negative integer, got: z');
    const unknown = capture();
    expect(await runCli(['thread', '98765'], unknown.io)).toBe(1);
    expect(unknown.err.join('')).toBe('unknown thread: 98765\n');
  });
});

describe('groups', () => {
  it('creates, adds, removes, lists and deletes', async () => {
    const create = capture();
    expect(await runCli(['groups', 'create', 'dev'], create.io)).toBe(0);
    expect(create.out.join('')).toBe('dev:\n');
    const add = capture();
    expect(await runCli(['groups', 'add', 'dev', 'alice', 'bob'], add.io)).toBe(0);
    expect(add.out.join('')).toBe('dev: alice, bob\n');
    const remove = capture();
    expect(await runCli(['groups', 'remove', 'dev', 'bob'], remove.io)).toBe(0);
    expect(remove.out.join('')).toBe('dev: alice\n');
    const list = capture();
    expect(await runCli(['groups'], list.io)).toBe(0);
    expect(list.out.join('')).toBe('dev: alice\n');
    const explicitList = capture();
    expect(await runCli(['groups', 'list'], explicitList.io)).toBe(0);
    expect(explicitList.out.join('')).toBe('dev: alice\n');
    const del = capture();
    expect(await runCli(['groups', 'delete', 'dev'], del.io)).toBe(0);
    expect(del.out.join('')).toBe('');
  });

  it('validates its arguments', async () => {
    const noName = capture();
    expect(await runCli(['groups', 'create'], noName.io)).toBe(1);
    expect(noName.err.join('')).toContain('murmur groups create: missing group name');
    const noMembers = capture();
    expect(await runCli(['groups', 'add', 'dev'], noMembers.io)).toBe(1);
    expect(noMembers.err.join('')).toContain('murmur groups add: need a group name and at least one member');
    const unknownAction = capture();
    expect(await runCli(['groups', 'frobnicate'], unknownAction.io)).toBe(1);
    expect(unknownAction.err.join('')).toContain('murmur groups: unknown action: frobnicate');
    const unknownGroup = capture();
    expect(await runCli(['groups', 'delete', 'nope'], unknownGroup.io)).toBe(1);
    expect(unknownGroup.err.join('')).toBe('unknown group: nope\n');
  });
});

describe('slash', () => {
  it('bare prints the usage line, then the status view, then the groups view', async () => {
    const create = capture();
    expect(await runCli(['groups', 'create', 'demo'], create.io)).toBe(0);
    const add = capture();
    expect(await runCli(['groups', 'add', 'demo', 'alice', 'bob'], add.io)).toBe(0);
    // Both standalone views are captured right before the slash run, so the
    // peer and group sections are checked against what those commands print.
    const status = capture();
    expect(await runCli(['status'], status.io)).toBe(0);
    const groups = capture();
    expect(await runCli(['groups'], groups.io)).toBe(0);
    const peerLines = status.out.join('').split('\n').filter((l) => l.length > 0);
    const groupLines = groups.out.join('').split('\n').filter((l) => l.length > 0);
    expect(peerLines.length).toBeGreaterThanOrEqual(3);
    expect(groupLines).toEqual(['demo: alice, bob']);
    const cap = capture();
    expect(await runCli(['slash'], cap.io)).toBe(0);
    expect(cap.err).toEqual([]);
    expect(cap.out.join('')).toBe(
      [
        '/murmur <group | handle[,more]> <message>   (sent as renn, via the session you type it in; run bare like this for peers and groups)',
        '',
        'peers:',
        ...peerLines,
        '',
        'groups:',
        ...groupLines,
      ].join('\n') + '\n',
    );
  });

  it('delegates status and groups to the existing views', async () => {
    const viaSlash = capture();
    expect(await runCli(['slash', 'status'], viaSlash.io)).toBe(0);
    const direct = capture();
    expect(await runCli(['status'], direct.io)).toBe(0);
    expect(viaSlash.err).toEqual([]);
    expect(viaSlash.out.join('')).toBe(direct.out.join(''));
    expect(viaSlash.out.join('')).toMatch(/^renn \S+ unread 0/m);
    const groupsViaSlash = capture();
    expect(await runCli(['slash', 'groups'], groupsViaSlash.io)).toBe(0);
    expect(groupsViaSlash.out.join('')).toBe('demo: alice, bob\n');
    const groupsDirect = capture();
    expect(await runCli(['groups', 'list'], groupsDirect.io)).toBe(0);
    expect(groupsViaSlash.out.join('')).toBe(groupsDirect.out.join(''));
    // Sub-actions ride through the same groups parser, including its errors.
    const bad = capture();
    expect(await runCli(['slash', 'groups', 'frobnicate'], bad.io)).toBe(1);
    expect(bad.err.join('')).toContain('murmur groups: unknown action: frobnicate');
  });

  it('sends through with the send parsing and flags when given any other arguments', async () => {
    await connectPeer('slash-target');
    const cap = capture();
    const code = await runCli(['slash', 'slash-target,@demo', 'sent', 'through', 'slash', '--refs', 'alice'], cap.io);
    expect(code).toBe(0);
    expect(cap.err).toEqual([]);
    const lines = cap.out.join('').split('\n').filter((l) => l.length > 0);
    const last = lines[lines.length - 1] ?? '';
    expect(lines.slice(0, -1).sort()).toEqual(['alice: queued', 'bob: queued', 'slash-target: queued']);
    expect(last).toMatch(/^message \d+ thread \d+ seq 0$/);
    // A real send happened: the message row and its deliveries are stored
    // under the owner identity, exactly as murmur send would have done.
    const messageId = Number(/^message (\d+)/.exec(last)?.[1]);
    const row = store.messageById(messageId);
    expect(row?.sender).toBe('renn');
    expect(row?.body).toBe('sent through slash');
    expect(JSON.parse(row?.refsJson ?? '[]')).toEqual(['alice']);
    expect(
      store
        .getDeliveries(messageId)
        .map((d) => d.recipient)
        .sort(),
    ).toEqual(['alice', 'bob', 'slash-target']);
    // The send error path is unchanged too.
    const unknown = capture();
    expect(await runCli(['slash', '@nonexistent-group', 'x'], unknown.io)).toBe(1);
    expect(unknown.err.join('')).toBe('unknown group: nonexistent-group\n');
    expect(unknown.out).toEqual([]);
    const noBody = capture();
    expect(await runCli(['slash', 'alice'], noBody.io)).toBe(1);
    expect(noBody.err.join('')).toContain('murmur send: missing body');
  });
});

const HOOK_USAGE = 'usage: murmur hook <session-start|prompt|stop|session-end> [--client claude|codex|gemini|zcode]\n';

describe('hook', () => {
  it('rejects an unknown hook event', async () => {
    const cap = capture();
    expect(await runCli(['hook', 'bogus'], cap.io)).toBe(1);
    expect(cap.err.join('')).toBe(HOOK_USAGE);
    const none = capture();
    expect(await runCli(['hook'], none.io)).toBe(1);
  });

  it('parses --client and rejects an unknown or valueless client before reading stdin', async () => {
    for (const argv of [['hook', 'prompt', '--client', 'vim'], ['hook', 'prompt', '--client'], ['hook', '--client', 'codex']]) {
      const cap = capture();
      expect(await runCli(argv, cap.io)).toBe(1);
      expect(cap.err.join('')).toBe(HOOK_USAGE);
    }
    // A valid client reaches the hook; with no host it is silent.
    const cap = capture();
    expect(await withStdin('{}', () => runCli(['hook', 'session-end', '--client', 'gemini'], cap.io))).toBe(0);
    expect(cap.out.join('')).toBe('');
  });

  it('fails cleanly when stdin errors', async () => {
    const cap = capture();
    const original = Object.getOwnPropertyDescriptor(process, 'stdin');
    const failing = new Readable({
      read() {
        this.destroy(new Error('stdin exploded'));
      },
    });
    Object.defineProperty(process, 'stdin', { value: failing, configurable: true, enumerable: true });
    try {
      expect(await runCli(['hook', 'prompt'], cap.io)).toBe(1);
      expect(cap.err.join('')).toBe('stdin exploded\n');
    } finally {
      if (original !== undefined) {
        Object.defineProperty(process, 'stdin', original);
      }
    }
  });

  it('prompt prints the fetch-push additionalContext JSON line', async () => {
    await connectPeer('hooked');
    const alice = clients[0];
    expect(alice).toBeDefined();
    const sent = await req<SendResult>(alice as MurmurClient, {
      type: 'send',
      threadId: null,
      to: ['hooked'],
      body: 'injected for the prompt',
    });
    process.env.MURMUR_HANDLE = 'hooked';
    hostState.host = hostOf(9001, 'S1');
    const cap = capture();
    const code = await withStdin('{"session_id":"abcdef123456"}', () => runCli(['hook', 'prompt', '--client', 'codex'], cap.io));
    expect(code).toBe(0);
    const text = cap.out.join('');
    expect(text.endsWith('\n')).toBe(true);
    const parsed = JSON.parse(text) as { hookSpecificOutput: { hookEventName: string; additionalContext: string } };
    expect(parsed.hookSpecificOutput.hookEventName).toBe('UserPromptSubmit');
    expect(parsed.hookSpecificOutput.additionalContext).toContain(
      'murmur: 1 message(s) for hooked\n\n[murmur m' + String(sent.messageId) + ' t' + String(sent.threadId) + ' s0 from alice',
    );
    expect(parsed.hookSpecificOutput.additionalContext).toContain('injected for the prompt');
    expect(store.getDeliveries(sent.messageId)[0]?.state).toBe('delivered');
    expect(store.sessionByHost(9001, 'S1')).toMatchObject({ client: 'codex', handle: 'hooked', cliSessionId: 'abcdef123456' });
    // A second prompt has nothing to inject and prints nothing.
    const empty = capture();
    expect(await withStdin('', () => runCli(['hook', 'prompt'], empty.io))).toBe(0);
    expect(empty.out.join('')).toBe('');
    const stop = capture();
    expect(await withStdin('not json at all', () => runCli(['hook', 'stop'], stop.io))).toBe(0);
    expect(stop.out.join('')).toBe('');
  });
});

describe('serve', () => {
  it('--daemon re-spawns the entry detached without blocking', async () => {
    const cap = capture();
    expect(await runCli(['serve', '--daemon'], cap.io)).toBe(0);
    expect(cap.out).toEqual([]);
    expect(spawnState.calls).toHaveLength(1);
    expect(spawnState.calls[0]?.cmd).toBe(process.execPath);
    expect(spawnState.calls[0]?.args).toEqual([process.argv[1] ?? '', 'serve']);
    expect(spawnState.calls[0]?.opts).toMatchObject({ detached: true, stdio: 'ignore' });
    expect(spawnState.unrefs).toBe(1);
  });

  it('--daemon falls back to an empty entry path when argv[1] is unset', async () => {
    const cap = capture();
    const saved = process.argv[1];
    process.argv[1] = undefined as unknown as string;
    try {
      expect(await runCli(['serve', '--daemon'], cap.io)).toBe(0);
    } finally {
      process.argv[1] = saved;
    }
    const last = spawnState.calls[spawnState.calls.length - 1];
    expect(last?.args).toEqual(['', 'serve']);
    expect(spawnState.unrefs).toBe(2);
  });

  // Runs `murmur serve` in this process against serveHome until SIGTERM,
  // calling during() once the broker accepts connections.
  async function serveOnce(serveHome: string, during: (servePaths: MurmurPaths, out: string) => Promise<void>): Promise<void> {
    const servePaths = getPaths(serveHome);
    process.env.MURMUR_HOME = serveHome;
    const termBefore = process.listeners('SIGTERM');
    const intBefore = process.listeners('SIGINT');
    const cap = capture();
    const running = runCli(['serve'], cap.io);
    await settle(() => existsSync(servePaths.socketPath), 'serve socket');
    // The socket file appears before the broker instance is fully started;
    // a dial in that window is destroyed on purpose, so retry until the
    // broker accepts, the way a real client does.
    let probe: MurmurClient | null = null;
    for (let attempt = 0; attempt < 100 && probe === null; attempt += 1) {
      try {
        probe = await MurmurClient.connect({ socketPath: servePaths.socketPath, token: readToken(servePaths) });
      } catch {
        await new Promise((resolve) => {
          setImmediate(resolve);
        });
      }
    }
    if (probe === null) {
      throw new Error('serve broker never accepted a connection');
    }
    await expect(req(probe, { type: 'status' })).resolves.toBeDefined();
    probe.close();
    await during(servePaths, cap.out.join(''));
    process.emit('SIGTERM', 'SIGTERM');
    // A second signal while stopping is ignored.
    process.emit('SIGINT', 'SIGINT');
    expect(await running).toBe(0);
    await settle(() => !existsSync(servePaths.socketPath), 'serve socket removed');
    for (const listener of process.listeners('SIGTERM')) {
      if (!termBefore.includes(listener)) {
        process.off('SIGTERM', listener);
      }
    }
    for (const listener of process.listeners('SIGINT')) {
      if (!intBefore.includes(listener)) {
        process.off('SIGINT', listener);
      }
    }
  }

  it('runs a broker in the foreground, seeds the owner when init never ran, and serves real traffic', async () => {
    const serveHome = mkdtempSync(join(tmpdir(), 'murmur-cli-serve-'));
    extraHomes.push(serveHome);
    await serveOnce(serveHome, async (servePaths, printed) => {
      expect(printed).toBe(servePaths.socketPath + '\n');
      const status = capture();
      expect(await runCli(['status'], status.io)).toBe(0);
      expect(status.out.join('')).toContain('renn ');
      const send = capture();
      expect(await runCli(['send', 'renn', 'hello', 'serve'], send.io)).toBe(0);
      expect(send.out.join('')).toContain('renn: queued');
      expect(send.out.join('')).toMatch(/message \d+ thread \d+ seq 0/);
      // A hook on the served broker pulls a queued message (a state change).
      expect(await runCli(['send', '--as', 'probe-a', 'renn', 'hi'], capture().io)).toBe(0);
      expect(await runCli(['send', '--as', 'probe-b', 'probe-a', 'for', 'the', 'hook'], capture().io)).toBe(0);
      process.env.MURMUR_HANDLE = 'probe-a';
      hostState.host = hostOf(9001, 'S1');
      const prompt = capture();
      expect(await withStdin('', () => runCli(['hook', 'prompt'], prompt.io))).toBe(0);
      expect(prompt.out.join('')).toContain('for the hook');
    });
    const seeded = Store.open(getPaths(serveHome).dbPath);
    try {
      expect(seeded.listPeers().filter((peer) => peer.isOwner).map((peer) => [peer.handle, peer.display])).toEqual([
        ['renn', 'renn (owner)'],
      ]);
    } finally {
      seeded.close();
    }
  });

  it('keeps an existing owner peer when init already ran', async () => {
    const serveHome = mkdtempSync(join(tmpdir(), 'murmur-cli-serve-'));
    extraHomes.push(serveHome);
    process.env.MURMUR_HOME = serveHome;
    expect(await runCli(['init'], capture().io)).toBe(0);
    await serveOnce(serveHome, async () => {});
    const kept = Store.open(getPaths(serveHome).dbPath);
    try {
      expect(kept.listPeers().filter((peer) => peer.isOwner).map((peer) => [peer.handle, peer.display])).toEqual([['renn', null]]);
    } finally {
      kept.close();
    }
  });
});

describe('mcp', () => {
  it('delegates to runMcpServer and exits 0', async () => {
    const cap = capture();
    expect(await runCli(['mcp'], cap.io)).toBe(0);
    expect(mcpState.runs).toBe(1);
  });

  it('rethrows failures that are not Error instances untouched', async () => {
    mcpState.mode = 'throw-string';
    try {
      await expect(runCli(['mcp'], capture().io)).rejects.toBe('mcp exploded as a string');
    } finally {
      mcpState.mode = 'ok';
    }
  });
});

describe('install', () => {
  const USAGE = 'usage: murmur install <' + INSTALL_CLIENTS.join('|') + '>\n';

  it('rejects an unknown client', async () => {
    const cap = capture();
    expect(await runCli(['install', 'vim'], cap.io)).toBe(1);
    expect(cap.err.join('')).toBe(USAGE);
    const none = capture();
    expect(await runCli(['install'], none.io)).toBe(1);
    expect(none.err.join('')).toBe(USAGE);
  });

  it('exits 1 with the plain error when ~/.claude.json is not valid JSON', async () => {
    const claudeJson = join(osState.home, '.claude.json');
    writeFileSync(claudeJson, '{ not json', 'utf8');
    const cap = capture();
    expect(await runCli(['install', 'claude'], cap.io)).toBe(1);
    expect(cap.err.join('')).toContain('murmur install: ' + claudeJson + ' is not valid JSON');
    rmSync(claudeJson);
  });

  it('writes the claude wiring into the (stubbed) home and prints each file', async () => {
    const cap = capture();
    expect(await runCli(['install', 'claude'], cap.io)).toBe(0);
    const files = cap.out.join('').split('\n').filter((l) => l.length > 0);
    expect(files).toEqual([
      join(osState.home, '.claude', 'commands', 'murmur.md'),
      join(osState.home, '.claude.json'),
      join(osState.home, '.claude', 'settings.json'),
    ]);
    const claudeJson = JSON.parse(readFileSync(join(osState.home, '.claude.json'), 'utf8')) as {
      mcpServers: Record<string, unknown>;
    };
    expect(claudeJson.mcpServers.murmur).toEqual({ command: 'murmur', args: ['mcp'], alwaysLoad: true });
  });

  it.each([...INSTALL_CLIENTS])('accepts %s and prints what it wrote', async (client) => {
    const cap = capture();
    expect(await runCli(['install', client], cap.io)).toBe(0);
    expect(cap.err).toEqual([]);
    const lines = cap.out.join('').split('\n').filter((l) => l.length > 0);
    expect(lines.length).toBeGreaterThan(0);
    const written = lines.filter((l) => l.startsWith(osState.home));
    expect(written.length).toBeGreaterThan(0);
    for (const file of written) {
      expect(existsSync(file)).toBe(true);
    }
    // Everything that is not a path is a step for the user (codex trust).
    expect(lines.length - written.length).toBe(client === 'codex' ? 1 : 0);
  });
});

describe('send identity', () => {
  it('speaks as the nearest live session among the ancestors, else as the owner', async () => {
    await connectPeer('id-target');
    await startSession('id-claude', hostOf(9001, 'S1'));
    hostState.host = hostOf(9100, 'X', [9100, 9001, 1]);
    const viaSession = capture();
    expect(await runCli(['send', 'id-target', 'from', 'a', 'session'], viaSession.io)).toBe(0);
    const id = Number(/^message (\d+)/m.exec(viaSession.out.join(''))?.[1]);
    expect(store.messageById(id)?.sender).toBe('id-claude');
    // No ancestor is a live session host: the owner speaks.
    hostState.host = hostOf(9100, 'X', [9100, 9200]);
    const viaOwner = capture();
    expect(await runCli(['send', 'id-target', 'owner', 'here'], viaOwner.io)).toBe(0);
    expect(store.messageById(Number(/^message (\d+)/m.exec(viaOwner.out.join(''))?.[1]))?.sender).toBe('renn');
    // No host at all (ps unavailable) also falls back to the owner.
    hostState.host = null;
    const noHost = capture();
    expect(await runCli(['send', 'id-target', 'still', 'owner'], noHost.io)).toBe(0);
    expect(store.messageById(Number(/^message (\d+)/m.exec(noHost.out.join(''))?.[1]))?.sender).toBe('renn');
    // --as beats the session.
    hostState.host = hostOf(9001, 'S1');
    const asFlag = capture();
    expect(await runCli(['send', '--as', 'id-explicit', 'id-target', 'explicit'], asFlag.io)).toBe(0);
    expect(store.messageById(Number(/^message (\d+)/m.exec(asFlag.out.join(''))?.[1]))?.sender).toBe('id-explicit');
  });
});

describe('reply', () => {
  it('replies into the thread as a session with a verdict and refs, and as the owner without a host', async () => {
    const alice = clients[0] as MurmurClient;
    const owner = await connectPeer('reply-peer');
    const sent = await req<SendResult>(alice, { type: 'send', threadId: null, to: ['renn', 'reply-peer'], body: 'question' });
    expect(owner.client).toBeDefined();
    await startSession('reply-claude', hostOf(9002, 'S2'), 'codex');
    // The session reply goes to the whole thread minus the replier.
    hostState.host = hostOf(9002, 'S2');
    const cap = capture();
    expect(await runCli(['reply', String(sent.messageId), 'I', 'agree', '--verdict', 'agree', '--refs', 'a, b,'], cap.io)).toBe(0);
    expect(cap.err).toEqual([]);
    const match = /^message (\d+) thread (\d+) seq (\d+)$/.exec(cap.out.join('').trim());
    expect(match).not.toBeNull();
    const row = store.messageById(Number(match?.[1]));
    expect(row).toMatchObject({ sender: 'reply-claude', body: 'I agree', verdict: 'agree', isReply: true, inReplyTo: sent.messageId });
    expect(JSON.parse(row?.refsJson ?? '[]')).toEqual(['a', 'b']);
    // The owner, no host.
    hostState.host = null;
    const asOwner = capture();
    expect(await runCli(['reply', String(sent.messageId), 'plain', 'reply'], asOwner.io)).toBe(0);
    expect(store.messageById(Number(/^message (\d+)/.exec(asOwner.out.join(''))?.[1]))).toMatchObject({ sender: 'renn', verdict: null });
    // --as speaks as that peer.
    const asFlag = capture();
    expect(await runCli(['reply', '--as', 'reply-peer', String(sent.messageId), 'from', 'flag'], asFlag.io)).toBe(0);
    expect(store.messageById(Number(/^message (\d+)/.exec(asFlag.out.join(''))?.[1]))?.sender).toBe('reply-peer');
  });

  it('rejects bad input without reaching the broker, and relays broker errors', async () => {
    const noValue = capture();
    expect(await runCli(['reply', '1', 'x', '--verdict'], noValue.io)).toBe(1);
    expect(noValue.err.join('')).toContain('murmur reply: flag --verdict needs a value');
    const badAs = capture();
    expect(await runCli(['reply', '--as', 'Bad_One', '1', 'x'], badAs.io)).toBe(1);
    expect(badAs.err.join('')).toBe('invalid handle: Bad_One\n');
    for (const argv of [['reply'], ['reply', 'abc', 'x'], ['reply', '-3', 'x'], ['reply', '1']]) {
      const cap = capture();
      expect(await runCli(argv, cap.io)).toBe(1);
      expect(cap.err.join('')).toContain('murmur reply: need a message id and a body');
    }
    const unknown = capture();
    expect(await runCli(['reply', '987654', 'x'], unknown.io)).toBe(1);
    expect(unknown.out).toEqual([]);
    expect(unknown.err.join('')).toContain('987654');
  });
});

describe('name', () => {
  it('validates the handle and the host before connecting', async () => {
    for (const argv of [['name'], ['name', 'Bad_Handle']]) {
      const cap = capture();
      expect(await runCli(argv, cap.io)).toBe(1);
      expect(cap.err.join('')).toContain('murmur name: need a valid handle');
    }
    hostState.host = null;
    const noHost = capture();
    expect(await runCli(['name', 'fine-name'], noHost.io)).toBe(1);
    expect(noHost.err.join('')).toBe('murmur name: cannot find the agent session this runs in\n');
  });

  it('renames the session of the current host and refuses the owner handle', async () => {
    await startSession('name-before', hostOf(9001, 'S1'));
    hostState.host = hostOf(9001, 'S1');
    const cap = capture();
    expect(await runCli(['name', 'name-after'], cap.io)).toBe(0);
    expect(cap.out.join('')).toBe('name-after\n');
    expect(store.sessionByHost(9001, 'S1')?.handle).toBe('name-after');
    const refused = capture();
    expect(await runCli(['name', 'renn'], refused.io)).toBe(1);
    expect(refused.err.join('')).toContain('taken by a live session or the owner');
  });

  it('fails instead of printing an empty name when no hook announced the host yet', async () => {
    hostState.host = hostOf(9002, 'S9');
    const cap = capture();
    expect(await runCli(['name', 'too-early'], cap.io)).toBe(1);
    expect(cap.out.join('')).toBe('');
    expect(cap.err.join('')).toContain('has not announced itself yet');
    expect(store.getPeer('too-early')).toBeNull();
  });
});

describe('slash input forms', () => {
  it('--stdin reads "<targets> <body...>" from stdin and sends as the owner, body verbatim', async () => {
    await connectPeer('stdin-target');
    hostState.host = hostOf(9001, 'S1');
    const cap = capture();
    const code = await withStdin("stdin-target hello 'quoted' $(not run)\nsecond line\n", () => runCli(['slash', '--stdin'], cap.io));
    expect(code).toBe(0);
    const lines = cap.out.join('').split('\n').filter((l) => l.length > 0);
    expect(lines[0]).toBe('stdin-target: queued');
    const row = store.messageById(Number(/^message (\d+)/.exec(lines[1] ?? '')?.[1]));
    // Owner identity even though the host is a live session.
    expect(row).toMatchObject({ sender: 'renn', body: "hello 'quoted' $(not run)\nsecond line" });
    expect(store.getPeer('renn')?.display).toBe('renn (owner)');
  });

  it('--claude-escaped undoes Claude\'s "\\!" before a word-initial "!" and leaves other backslashes', async () => {
    await connectPeer('bang-target');
    const cap = capture();
    // What Claude hands over for "!first ok\n!line a!b c\!d \\x".
    const input = 'bang-target \\!first ok\n\\!line a!b c\\!d \\\\x\n';
    expect(await withStdin(input, () => runCli(['slash', '--stdin', '--claude-escaped'], cap.io))).toBe(0);
    const id = Number(/message (\d+)/.exec(cap.out.join(''))?.[1]);
    expect(store.messageById(id)?.body).toBe('!first ok\n!line a!b c\\!d \\\\x');
  });

  it('a single argument with whitespace is split into targets and body', async () => {
    await connectPeer('oneword-target');
    const cap = capture();
    expect(await runCli(['slash', 'oneword-target   spaced  body here'], cap.io)).toBe(0);
    const lines = cap.out.join('').split('\n').filter((l) => l.length > 0);
    expect(store.messageById(Number(/^message (\d+)/.exec(lines[1] ?? '')?.[1]))?.body).toBe('spaced  body here');
  });

  it('a lone word from stdin or one argument runs the matching view or reports the missing body', async () => {
    const viaStdin = capture();
    expect(await withStdin('status\n', () => runCli(['slash', '--stdin'], viaStdin.io))).toBe(0);
    const direct = capture();
    expect(await runCli(['status'], direct.io)).toBe(0);
    expect(viaStdin.out.join('')).toBe(direct.out.join(''));
    const groups = capture();
    expect(await withStdin('groups', () => runCli(['slash', '--stdin'], groups.io))).toBe(0);
    const missing = capture();
    expect(await withStdin('alice', () => runCli(['slash', '--stdin'], missing.io))).toBe(1);
    expect(missing.err.join('')).toContain('murmur send: missing body');
  });

  it('empty stdin or a whitespace-only argument falls through to the bare discovery view', async () => {
    for (const run of [
      () => withStdin('  \n', () => runCli(['slash', '--stdin'], cap.io)),
      () => runCli(['slash', '   '], cap.io),
    ]) {
      cap = capture();
      expect(await run()).toBe(0);
      expect(cap.out.join('')).toContain('\npeers:\n');
      expect(cap.out.join('')).toContain('\ngroups:\n');
    }
  });
  let cap: Capture;
});

describe('slash via and prune', () => {
  it('stamps the session the command was typed in as via, and none from a plain terminal', async () => {
    await connectPeer('via-target');
    await startSession('via-claude', hostOf(9002, 'S2'), 'codex');
    const viaOf = async (host: HostInfo | null): Promise<string | null | undefined> => {
      hostState.host = host;
      const cap = capture();
      expect(await runCli(['slash', 'via-target', 'hello', String(Math.random())], cap.io)).toBe(0);
      return store.messageById(Number(/message (\d+) thread/.exec(cap.out.join(''))?.[1]))?.via;
    };
    expect(await viaOf(hostOf(9100, 'X', [9100, 9002, 1]))).toBe('via-claude');
    expect(await viaOf(hostOf(9100, 'X', [9100, 9200]))).toBeNull();
    expect(await viaOf(null)).toBeNull();
  });

  it('prune prints the removed handles one per line, then says there is nothing to prune', async () => {
    store.upsertPeer('claude-deadbeef', null, null, false, 1);
    const first = capture();
    expect(await runCli(['prune'], first.io)).toBe(0);
    expect(first.out.join('')).toContain('claude-deadbeef\n');
    expect(first.out.join('')).not.toContain('nothing to prune');
    const second = capture();
    expect(await runCli(['prune'], second.io)).toBe(0);
    expect(second.out.join('')).toBe('nothing to prune\n');
  });
});

describe('broker down', () => {
  it('exits 2 with the start hint when no broker listens', async () => {
    const downHome = mkdtempSync(join(tmpdir(), 'murmur-cli-down-'));
    extraHomes.push(downHome);
    ensureHome(getPaths(downHome));
    process.env.MURMUR_HOME = downHome;
    const status = capture();
    expect(await runCli(['status'], status.io)).toBe(2);
    expect(status.err.join('')).toBe('murmur broker is down; start it with: murmur serve\n');
    expect(status.out).toEqual([]);
    const send = capture();
    expect(await runCli(['send', 'bob', 'hi'], send.io)).toBe(2);
    expect(send.err.join('')).toBe('murmur broker is down; start it with: murmur serve\n');
    const thread = capture();
    expect(await runCli(['thread', '1'], thread.io)).toBe(2);
    const groups = capture();
    expect(await runCli(['groups'], groups.io)).toBe(2);
    // Bare slash falls through to the same message before printing anything.
    const slash = capture();
    expect(await runCli(['slash'], slash.io)).toBe(2);
    expect(slash.err.join('')).toBe('murmur broker is down; start it with: murmur serve\n');
    expect(slash.out).toEqual([]);
  });
});
