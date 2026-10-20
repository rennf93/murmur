import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Broker } from '../src/broker.js';
import { MurmurClient } from '../src/client.js';
import { ensureHome, getPaths, readToken } from '../src/config.js';
import type { MurmurPaths } from '../src/config.js';
import { MurmurCore } from '../src/core.js';
import { buildMcpServer, clientFromEnv, identityFor, runMcpServer } from '../src/mcp.js';
import type { HostInfo, ProcInfo } from '../src/proc.js';
import type {
  AckResult,
  ClientMessage,
  GroupsResult,
  RegisterResult,
  ReplyResult,
  SendResult,
  ServerEvent,
  StatusResult,
  ThreadResult,
  WaitResult,
} from '../src/protocol.js';
import { Store } from '../src/store.js';

// MCP conformance: the real SDK Client talks to the McpServer from
// buildMcpServer over an in-memory transport pair while the server side
// drives a real broker through a real MurmurClient connection.

// runMcpServer binds a StdioServerTransport to the process streams, which a
// test worker cannot offer; this stand-in keeps the same contract (start,
// send, close plus the handler slots the SDK assigns) without touching stdio.
const fakeStdio = vi.hoisted(() => ({ constructed: 0, started: 0 }));

vi.mock('@modelcontextprotocol/sdk/server/stdio.js', () => ({
  StdioServerTransport: class FakeStdioServerTransport {
    onclose: (() => void) | undefined;
    onerror: ((error: Error) => void) | undefined;
    onmessage: ((message: unknown) => void) | undefined;
    constructor() {
      fakeStdio.constructed += 1;
    }
    async start(): Promise<void> {
      fakeStdio.started += 1;
    }
    async send(): Promise<void> {}
    async close(): Promise<void> {}
  },
}));

// currentHost is what runMcpServer's identity comes from; the broker keeps
// the real process helpers but reads a fixed table so announced sessions are
// "alive" without real processes.
const hostState = vi.hoisted(() => ({ host: null as unknown }));

vi.mock('../src/proc.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/proc.js')>();
  return { ...actual, currentHost: () => hostState.host };
});

const FAKE_TABLE = new Map<number, ProcInfo>([[9001, { ppid: 1, start: 'S1', comm: 'claude' }]]);
const HOST: HostInfo = { hostPid: 9001, hostStart: 'S1', ancestors: [9001] };

const TOOL_NAMES = [
  'murmur_name',
  'murmur_send',
  'murmur_reply',
  'murmur_ack',
  'murmur_thread',
  'murmur_wait',
  'murmur_groups',
  'murmur_status',
];

interface Stack {
  home: string;
  paths: MurmurPaths;
  token: string;
  store: Store;
  core: MurmurCore;
  broker: Broker;
}

interface Pair {
  sdk: Client;
  murmur: MurmurClient;
  events: ServerEvent[];
}

const stacks: Stack[] = [];
const homes: string[] = [];
const clients: MurmurClient[] = [];
const pairs: Pair[] = [];
const savedEnv: { home: string | undefined; handle: string | undefined } = {
  home: process.env.MURMUR_HOME,
  handle: process.env.MURMUR_HANDLE,
};

function freshHome(): string {
  const home = mkdtempSync(join(tmpdir(), 'murmur-mcp-'));
  homes.push(home);
  return home;
}

async function buildStack(): Promise<Stack> {
  const home = freshHome();
  const paths = getPaths(home);
  ensureHome(paths);
  const store = Store.open(paths.dbPath);
  const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, undefined, () => FAKE_TABLE);
  const broker = await Broker.start({ paths, store, core });
  const stack: Stack = { home, paths, token: readToken(paths), store, core, broker };
  stacks.push(stack);
  return stack;
}

async function connectRaw(stack: Stack, handle: string): Promise<{ client: MurmurClient; events: ServerEvent[] }> {
  const events: ServerEvent[] = [];
  const client = await MurmurClient.connect({
    socketPath: stack.paths.socketPath,
    token: stack.token,
    onEvent: (ev) => {
      events.push(ev);
    },
  });
  clients.push(client);
  await req<RegisterResult>(client, { type: 'register', handle, clientKind: 'cli' });
  return { client, events };
}

function req<T>(client: MurmurClient, msg: Record<string, unknown>): Promise<T> {
  return client.request<T>(msg as unknown as Omit<ClientMessage, 'v' | 'id'>);
}

async function pair(stack: Stack, identity?: Parameters<typeof buildMcpServer>[1]): Promise<Pair> {
  const events: ServerEvent[] = [];
  const murmur = await MurmurClient.connect({
    socketPath: stack.paths.socketPath,
    token: stack.token,
    onEvent: (ev) => {
      events.push(ev);
    },
  });
  const server = buildMcpServer(murmur, identity);
  const sdk = new Client({ name: 'murmur-conformance-client', version: '0.0.1' });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await sdk.connect(clientTransport);
  const made: Pair = { sdk, murmur, events };
  pairs.push(made);
  return made;
}

async function call<T>(sdk: Client, name: string, args: Record<string, unknown> = {}): Promise<T> {
  const result = (await sdk.callTool({ name, arguments: args })) as CallToolResult;
  expect(result.isError ?? false).toBe(false);
  const first = result.content[0];
  expect(first?.type).toBe('text');
  return JSON.parse((first as { type: 'text'; text: string }).text) as T;
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

afterEach(async () => {
  hostState.host = null;
  vi.restoreAllMocks();
  for (const made of pairs.splice(0)) {
    await made.sdk.close();
    made.murmur.close();
  }
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
  if (savedEnv.home === undefined) {
    delete process.env.MURMUR_HOME;
  } else {
    process.env.MURMUR_HOME = savedEnv.home;
  }
  if (savedEnv.handle === undefined) {
    delete process.env.MURMUR_HANDLE;
  } else {
    process.env.MURMUR_HANDLE = savedEnv.handle;
  }
});

describe('identity helpers', () => {
  it('clientFromEnv picks the client from its marker, claude first', () => {
    expect(clientFromEnv({ CLAUDECODE: '1', GEMINI_CLI: '1', CODEX_HOME: '/x' })).toBe('claude');
    expect(clientFromEnv({ GEMINI_CLI: '1', CODEX_HOME: '/x' })).toBe('gemini');
    expect(clientFromEnv({ CODEX_THREAD_ID: 't' })).toBe('codex');
    expect(clientFromEnv({ PATH: '/bin' })).toBe('other');
    expect(clientFromEnv({})).toBe('other');
  });

  it('identityFor is empty without a host and carries host, client and cwd with one', () => {
    expect(identityFor(null, { CLAUDECODE: '1' }, '/work')).toEqual({});
    expect(identityFor(HOST, { GEMINI_CLI: '1' }, '/work')).toEqual({
      host: { hostPid: 9001, hostStart: 'S1' },
      client: 'gemini',
      cwd: '/work',
    });
  });
});

describe('SDK conformance over InMemoryTransport', () => {
  it('completes initialize and lists exactly the 8 murmur tools with instructions', async () => {
    const stack = await buildStack();
    const { sdk } = await pair(stack);
    expect(sdk.getServerVersion()).toMatchObject({ name: 'murmur', version: '0.2.0' });
    expect(sdk.getInstructions()).toContain('murmur_reply');
    const listed = await sdk.listTools();
    expect(listed.tools.map((t) => t.name).sort()).toEqual([...TOOL_NAMES].sort());
    expect(listed.tools).toHaveLength(8);
    for (const tool of listed.tools) {
      expect(tool.description?.length ?? 0).toBeGreaterThan(0);
      expect(tool.inputSchema.type).toBe('object');
    }
    const send = listed.tools.find((t) => t.name === 'murmur_send');
    expect(send?.inputSchema.required).toEqual(expect.arrayContaining(['to', 'body']));
  });

  it('murmur_name registers a handle with display and role when there is no host identity', async () => {
    const stack = await buildStack();
    const { sdk } = await pair(stack);
    const named = await call<RegisterResult>(sdk, 'murmur_name', { handle: 'mcpagent', display: 'MCP Agent', role: 'tester' });
    expect(named).toEqual({ handle: 'mcpagent', owner: false });
    expect(stack.store.getPeer('mcpagent')).toMatchObject({ display: 'MCP Agent', role: 'tester' });
    const bare = await call<RegisterResult>(sdk, 'murmur_name', { handle: 'mcpagent' });
    expect(bare.handle).toBe('mcpagent');
  });

  it('murmur_name renames the session of its host identity', async () => {
    const stack = await buildStack();
    const viewer = await connectRaw(stack, 'viewer');
    await req(viewer.client, { type: 'session', event: 'session-start', client: 'claude', cliSessionId: null, host: { hostPid: 9001, hostStart: 'S1' }, handle: 'before-name' });
    const { sdk } = await pair(stack, { host: { hostPid: 9001, hostStart: 'S1' }, client: 'claude', cwd: '/work' });
    const named = await call<RegisterResult>(sdk, 'murmur_name', { handle: 'after-name', display: 'Renamed' });
    expect(named).toEqual({ handle: 'after-name', owner: false });
    expect(stack.store.sessionByHost(9001, 'S1')?.handle).toBe('after-name');
    expect(stack.store.getPeer('after-name')?.display).toBe('Renamed');
    expect(stack.store.getPeer('before-name')).toBeNull();
  });

  it('sends with options, and wait with timeoutMs 0 returns at once while one second times out', async () => {
    const stack = await buildStack();
    const { sdk } = await pair(stack);
    await connectRaw(stack, 'peerone');
    await call<RegisterResult>(sdk, 'murmur_name', { handle: 'mcpagent' });
    const sent = await call<SendResult>(sdk, 'murmur_send', {
      to: ['peerone'],
      body: 'hello from mcp',
      references: ['renn'],
      mode: 'push',
    });
    expect(sent.seq).toBe(0);
    expect(sent.deliveries.map((d) => d.recipient)).toEqual(['peerone']);
    const again = await call<SendResult>(sdk, 'murmur_send', { threadId: sent.threadId, to: ['peerone'], body: 'again' });
    expect(again).toMatchObject({ threadId: sent.threadId, seq: 1 });
    const now = Date.now();
    const instant = await call<WaitResult>(sdk, 'murmur_wait', { threadId: sent.threadId, until: 'all_replied', timeoutMs: 0 });
    expect(Date.now() - now).toBeLessThan(500);
    expect(instant).toMatchObject({ threadId: sent.threadId, conditionMet: false, timedOut: true });
    expect(instant.matrix[0]).toMatchObject({ recipient: 'peerone', verdict: null });
    const started = Date.now();
    const waited = await call<WaitResult>(sdk, 'murmur_wait', { threadId: sent.threadId, until: 'all_replied', timeoutMs: 1000 });
    expect(Date.now() - started).toBeGreaterThanOrEqual(900);
    expect(waited).toMatchObject({ conditionMet: false, timedOut: true });
  });

  it('exercises reply, ack, thread digest vs full, groups, status and a default-timeout wait', async () => {
    const stack = await buildStack();
    const { sdk, murmur } = await pair(stack);
    const peer = await connectRaw(stack, 'peerone');
    await call<RegisterResult>(sdk, 'murmur_name', { handle: 'mcpagent' });
    const incoming = await req<SendResult>(peer.client, {
      type: 'send',
      threadId: null,
      to: ['mcpagent'],
      body: 'question for the agent\nwith a second line',
    });
    // Only a delivered (or sent) message can be acknowledged.
    expect(await call<AckResult>(sdk, 'murmur_ack', { ids: [incoming.messageId] })).toEqual({ acknowledged: [] });
    await req(murmur, { type: 'delivered', ids: [incoming.messageId] });
    const acked = await call<AckResult>(sdk, 'murmur_ack', { ids: [incoming.messageId] });
    expect(acked).toEqual({ acknowledged: [incoming.messageId] });
    const plain = await call<ReplyResult>(sdk, 'murmur_reply', { messageId: incoming.messageId, body: 'INFO: and more' });
    expect(plain).toMatchObject({ threadId: incoming.threadId, seq: 1 });
    const replied = await call<ReplyResult>(sdk, 'murmur_reply', {
      messageId: incoming.messageId,
      body: 'AGREE: looks right',
      verdict: 'agree',
      references: ['peerone'],
    });
    expect(replied.seq).toBe(2);
    const digest = await call<ThreadResult>(sdk, 'murmur_thread', { threadId: incoming.threadId });
    expect(digest.messages.map((m) => m.seq)).toEqual([0, 1, 2]);
    expect(digest.messages[0]?.firstLine).toBe('question for the agent');
    expect(digest.messages.every((m) => m.body === undefined)).toBe(true);
    expect(digest.messages[1]?.verdict).toBeNull();
    expect(digest.messages[2]?.verdict).toBe('agree');
    expect(digest.messages[2]?.references).toEqual(['peerone']);
    const full = await call<ThreadResult>(sdk, 'murmur_thread', { threadId: incoming.threadId, full: true });
    expect(full.messages[0]?.body).toBe('question for the agent\nwith a second line');
    const since = await call<ThreadResult>(sdk, 'murmur_thread', { threadId: incoming.threadId, sinceSeq: 1, full: true });
    expect(since.messages.map((m) => m.seq)).toEqual([2]);
    expect(since.messages[0]?.body).toBe('AGREE: looks right');
    // The ask is answered, so a wait without timeoutMs (and one past the cap)
    // returns at once with the latest verdict.
    for (const extra of [{}, { timeoutMs: 99999999 }]) {
      const waited = await call<WaitResult>(sdk, 'murmur_wait', { threadId: incoming.threadId, until: 'any_replied', ...extra });
      expect(waited).toMatchObject({ conditionMet: true, timedOut: false });
      expect(waited.matrix[0]).toMatchObject({ recipient: 'mcpagent', state: 'replied', verdict: 'agree' });
    }
    const created = await call<GroupsResult[]>(sdk, 'murmur_groups', { action: 'create', name: 'mcpgroup' });
    expect(created.map((g) => g.name)).toEqual(['mcpgroup']);
    const added = await call<GroupsResult[]>(sdk, 'murmur_groups', { action: 'add', name: 'mcpgroup', members: ['peerone', 'mcpagent'] });
    expect(added[0]?.members).toEqual(['mcpagent', 'peerone']);
    const listed = await call<GroupsResult[]>(sdk, 'murmur_groups', { action: 'list' });
    expect(listed).toEqual(added);
    const status = await call<StatusResult>(sdk, 'murmur_status');
    expect(status.peers.find((p) => p.handle === 'mcpagent')).toMatchObject({ presence: 'online' });
    // A cli connection carries identity but is not presence.
    expect(status.peers.find((p) => p.handle === 'peerone')?.presence).not.toBe('online');
  });

  it('surfaces broker errors as tool errors and rejects invalid arguments', async () => {
    const stack = await buildStack();
    const { sdk } = await pair(stack);
    await call<RegisterResult>(sdk, 'murmur_name', { handle: 'mcpagent' });
    const failed = (await sdk.callTool({ name: 'murmur_send', arguments: { to: ['nobody-here'], body: 'hi' } })) as CallToolResult;
    expect(failed.isError).toBe(true);
    expect((failed.content[0] as { type: 'text'; text: string }).text).toContain('unknown handle: nobody-here');
    const missingBody = (await sdk.callTool({ name: 'murmur_send', arguments: { to: ['x'] } })) as CallToolResult;
    expect(missingBody.isError).toBe(true);
    expect((missingBody.content[0] as { type: 'text'; text: string }).text).toContain('Input validation error');
    const badUntil = (await sdk.callTool({ name: 'murmur_wait', arguments: { threadId: 1, until: 'never' } })) as CallToolResult;
    expect(badUntil.isError).toBe(true);
    const negative = (await sdk.callTool({ name: 'murmur_wait', arguments: { threadId: 1, until: 'any_replied', timeoutMs: -1 } })) as CallToolResult;
    expect(negative.isError).toBe(true);
  });
});

describe('runMcpServer', () => {
  function exitThrows(): { exits: number[]; stderr: string[] } {
    const exits: number[] = [];
    const stderr: string[] = [];
    vi.spyOn(process, 'exit').mockImplementation(((code?: number | string | null) => {
      exits.push(Number(code));
      throw new Error('process.exit(' + String(code) + ')');
    }) as (code?: number | string | null) => never);
    vi.spyOn(process.stderr, 'write').mockImplementation(((chunk: string | Uint8Array) => {
      stderr.push(String(chunk));
      return true;
    }) as typeof process.stderr.write);
    return { exits, stderr };
  }

  // Starts the server (it parks on the broker connection by design) and
  // returns once stdio is up; stop() fires the exit teardown.
  async function launch(stack: Stack): Promise<{ stop: () => Promise<void> }> {
    process.env.MURMUR_HOME = stack.home;
    const before = fakeStdio.started;
    let failure: unknown = null;
    const running = runMcpServer().catch((err: unknown) => {
      failure = err;
    });
    await settle(() => fakeStdio.started === before + 1, 'stdio transport started');
    expect(failure).toBeNull();
    return {
      stop: async () => {
        process.emit('exit', 0);
        await running;
      },
    };
  }

  async function announce(stack: Stack, handle: string): Promise<void> {
    const viewer = await connectRaw(stack, 'announcer');
    await req(viewer.client, {
      type: 'session',
      event: 'session-start',
      client: 'claude',
      cliSessionId: null,
      host: { hostPid: 9001, hostStart: 'S1' },
      handle,
    });
  }

  async function presenceOf(stack: Stack, handle: string): Promise<string | undefined> {
    const viewer = await MurmurClient.connect({ socketPath: stack.paths.socketPath, token: stack.token });
    clients.push(viewer);
    const status = await req<StatusResult>(viewer, { type: 'status' });
    return status.peers.find((p) => p.handle === handle)?.presence;
  }

  it('exits 1 with a diagnostic when the broker is down', async () => {
    const home = freshHome();
    const paths = getPaths(home);
    ensureHome(paths);
    process.env.MURMUR_HOME = home;
    const { exits, stderr } = exitThrows();
    await expect(runMcpServer()).rejects.toThrow('process.exit(1)');
    expect(exits).toEqual([1]);
    expect(stderr.join('')).toContain('murmur mcp: cannot connect to broker at ' + paths.socketPath + ' (broker_down)');
    expect(fakeStdio.constructed).toBe(0);
  });

  it('with no host registers the anon handle and stays up', async () => {
    const stack = await buildStack();
    delete process.env.MURMUR_HANDLE;
    const handle = 'anon-' + String(process.pid);
    const exitSpy = vi.spyOn(process, 'exit');
    const up = await launch(stack);
    expect(fakeStdio.constructed).toBeGreaterThanOrEqual(1);
    expect(stack.store.getPeer(handle)).not.toBeNull();
    expect(exitSpy).not.toHaveBeenCalled();
    expect(await presenceOf(stack, handle)).toBe('online');
    await up.stop();
    await settle(async () => (await presenceOf(stack, handle)) !== 'online', 'anon handle offline after exit teardown');
  });

  it('with no host uses a valid MURMUR_HANDLE and ignores an invalid one', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HANDLE = 'explicit-one';
    const first = await launch(stack);
    expect(stack.store.getPeer('explicit-one')).not.toBeNull();
    await first.stop();
    process.env.MURMUR_HANDLE = 'Not A Valid Handle';
    const second = await launch(stack);
    expect(stack.store.getPeer('Not A Valid Handle')).toBeNull();
    expect(stack.store.getPeer('anon-' + String(process.pid))).not.toBeNull();
    await second.stop();
  });

  it('with a host registers by host identity: pending until the session announces itself', async () => {
    const stack = await buildStack();
    delete process.env.MURMUR_HANDLE;
    hostState.host = HOST;
    const up = await launch(stack);
    expect(stack.store.getPeer('anon-' + String(process.pid))).toBeNull();
    await announce(stack, 'late-session');
    await settle(async () => (await presenceOf(stack, 'late-session')) === 'idle', 'late-session bound and idle');
    await up.stop();
  });

  it('with a host and a wanted handle, renames the announced session to it', async () => {
    const stack = await buildStack();
    await announce(stack, 'old-name');
    process.env.MURMUR_HANDLE = 'new-name';
    hostState.host = HOST;
    const up = await launch(stack);
    expect(stack.store.sessionByHost(9001, 'S1')?.handle).toBe('new-name');
    expect(stack.store.getPeer('old-name')).toBeNull();
    await up.stop();
  });

  it('with a host, retries without the wanted handle when the broker rejects it', async () => {
    const stack = await buildStack();
    stack.store.upsertPeer('renn', null, null, true, Date.now());
    await announce(stack, 'kept-name');
    // The owner handle cannot be claimed by a session.
    process.env.MURMUR_HANDLE = 'renn';
    hostState.host = HOST;
    const up = await launch(stack);
    expect(stack.store.sessionByHost(9001, 'S1')?.handle).toBe('kept-name');
    await settle(async () => (await presenceOf(stack, 'kept-name')) === 'idle', 'kept-name bound after retry');
    await up.stop();
  });
});
