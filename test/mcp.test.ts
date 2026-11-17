import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Broker } from '../src/broker.js';
import { MurmurClient } from '../src/client.js';
import { ensureHome, getPaths, readToken } from '../src/config.js';
import type { MurmurPaths } from '../src/config.js';
import { MurmurCore } from '../src/core.js';
import { buildMcpServer, clientFromEnv, deliverLocally, identityFor, pushTarget, runMcpServer } from '../src/mcp.js';
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
  ThreadListEntry,
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
const savedEnv: { home: string | undefined; handle: string | undefined; socket: string | undefined; token: string | undefined } = {
  home: process.env.MURMUR_HOME,
  handle: process.env.MURMUR_HANDLE,
  socket: process.env.CLAUDE_CODE_MESSAGING_SOCKET,
  token: process.env.CLAUDE_CODE_MESSAGING_TOKEN,
};

function restoreEnv(key: string, value: string | undefined): void {
  if (value === undefined) {
    delete process.env[key];
  } else {
    process.env[key] = value;
  }
}

function freshHome(): string {
  const home = mkdtempSync(join(tmpdir(), 'murmur-mcp-'));
  homes.push(home);
  return home;
}

async function buildStack(now?: () => number): Promise<Stack> {
  const home = freshHome();
  const paths = getPaths(home);
  ensureHome(paths);
  const store = Store.open(paths.dbPath);
  const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, now, () => FAKE_TABLE);
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
  restoreEnv('CLAUDE_CODE_MESSAGING_SOCKET', savedEnv.socket);
  restoreEnv('CLAUDE_CODE_MESSAGING_TOKEN', savedEnv.token);
});

describe('local push in a remote hub home', () => {
  const MESSAGING = { CLAUDE_CODE_MESSAGING_SOCKET: '/tmp/cc.sock', CLAUDE_CODE_MESSAGING_TOKEN: 'ntok' };

  it('pushTarget is set only in a remote hub home with both messaging variables', () => {
    const home = freshHome();
    expect(pushTarget(MESSAGING, home)).toBeNull();
    writeFileSync(join(home, 'remote'), '');
    expect(pushTarget(MESSAGING, home)).toEqual({ socketPath: '/tmp/cc.sock', token: 'ntok' });
    expect(pushTarget({ CLAUDE_CODE_MESSAGING_SOCKET: '/tmp/cc.sock' }, home)).toBeNull();
    expect(pushTarget({ CLAUDE_CODE_MESSAGING_TOKEN: 'ntok' }, home)).toBeNull();
  });

  it('deliverLocally sends a push, confirms it only when accepted, and sends notices as text', async () => {
    const target = { socketPath: '/tmp/cc.sock', token: 'ntok' };
    const sends: Array<{ socketPath: string; token: string; text: string }> = [];
    let accept = true;
    const send = async (socketPath: string, token: string, text: string): Promise<{ ok: boolean; error: string | null }> => {
      sends.push({ socketPath, token, text });
      return accept ? { ok: true, error: null } : { ok: false, error: 'refused' };
    };
    const requests: unknown[] = [];
    const client = {
      request: async <T>(msg: unknown): Promise<T> => {
        requests.push(msg);
        return { delivered: [] } as T;
      },
    };
    const push = { v: 2, type: 'event', event: 'push', payload: { message: { id: 7 }, mode: 'push', footer: '', text: '[murmur m7] hi' } } as unknown as ServerEvent;
    await deliverLocally(push, target, client, send);
    expect(sends).toEqual([{ ...target, text: '[murmur m7] hi' }]);
    expect(requests).toEqual([{ type: 'delivered', ids: [7] }]);
    accept = false;
    await deliverLocally(push, target, client, send);
    expect(requests).toHaveLength(1);
    const notice = {
      v: 2,
      type: 'event',
      event: 'notification',
      payload: [
        { kind: 'reply_arrived', threadId: 1, fromHandle: 'a', detail: 'reply from a in thread 1' },
        { kind: 'peer_offline', threadId: 2, fromHandle: 'b', detail: 'peer b went offline' },
      ],
    } as unknown as ServerEvent;
    await deliverLocally(notice, target, client, send);
    expect(sends[2]).toEqual({ ...target, text: '[murmur notice] reply from a in thread 1; peer b went offline' });
    expect(requests).toHaveLength(1);
    // A broker connection that went away does not turn into a rejection.
    accept = true;
    const gone = { request: async (): Promise<never> => Promise.reject(new Error('broker_closed')) };
    await expect(deliverLocally(push, target, gone, send)).resolves.toBeUndefined();
  });
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
    expect(identityFor(HOST, { GEMINI_CLI: '1' }, '/work', { machine: 'omen-boot', machineName: 'omen' })).toEqual({
      host: { hostPid: 9001, hostStart: 'S1', machine: 'omen-boot', machineName: 'omen' },
      client: 'gemini',
      cwd: '/work',
    });
  });
});

describe('SDK conformance over InMemoryTransport', () => {
  it('completes initialize and lists exactly the 8 murmur tools with instructions', async () => {
    const stack = await buildStack();
    const { sdk } = await pair(stack);
    const { version } = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as { version: string };
    expect(sdk.getServerVersion()).toMatchObject({ name: 'murmur', version });
    const instructions = sdk.getInstructions() ?? '';
    expect(instructions).toContain('murmur_reply');
    expect(instructions).toContain('ONE murmur_send');
    expect(instructions).toContain('SendMessage');
    expect(instructions).toContain('plain text never reaches the sender');
    expect(instructions).toContain('To offer options, ask for a CHOICE reply.');
    expect(instructions.length).toBeLessThan(450);
    const listed = await sdk.listTools();
    expect(listed.tools.map((t) => t.name).sort()).toEqual([...TOOL_NAMES].sort());
    expect(listed.tools).toHaveLength(8);
    const replyTool = listed.tools.find((t) => t.name === 'murmur_reply');
    expect(replyTool?.description).toContain('CHOICE <option>');
    expect((replyTool?.inputSchema.properties?.verdict as { enum?: string[] } | undefined)?.enum).toEqual(['agree', 'disagree', 'info', 'choice']);
    for (const tool of listed.tools) {
      expect(tool.description?.length ?? 0).toBeGreaterThan(0);
      expect(tool.inputSchema.type).toBe('object');
      expect(tool._meta).toEqual({ 'anthropic/alwaysLoad': true });
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
    // A snapshot did not wait, so it carries no advice about waiting.
    expect('note' in instant).toBe(false);
    expect(instant.matrix[0]).toMatchObject({ recipient: 'peerone', verdict: null });
    const started = Date.now();
    const waited = await call<WaitResult>(sdk, 'murmur_wait', { threadId: sent.threadId, until: 'all_replied', timeoutMs: 1000 });
    expect(Date.now() - started).toBeGreaterThanOrEqual(900);
    expect(waited).toMatchObject({ conditionMet: false, timedOut: true });
    expect(waited.note).toContain('Do not start work that could overlap theirs: ask the user, or murmur_wait again.');
  });

  it('murmur_send with wait sends and returns the wait result in one call, resolving a display name', async () => {
    const stack = await buildStack();
    const { sdk } = await pair(stack);
    const peer = await connectRaw(stack, 'peerone');
    await call<RegisterResult>(sdk, 'murmur_name', { handle: 'mcpagent' });
    stack.store.upsertPeer('peerone', 'Peer One', null, false, Date.now());
    const both = await call<{ sent: SendResult; wait: WaitResult }>(sdk, 'murmur_send', {
      to: ['peer one'], body: 'quick?', wait: 'any_replied', timeoutMs: 0,
    });
    expect(both.sent.deliveries.map((d) => d.recipient)).toEqual(['peerone']);
    expect(both.wait).toMatchObject({ threadId: both.sent.threadId, conditionMet: false, timedOut: true });
    const dflt = call<{ sent: SendResult; wait: WaitResult }>(sdk, 'murmur_send', { to: ['peerone'], body: 'again?', wait: 'all_replied' });
    await vi.waitFor(() => expect(stack.store.messageById(both.sent.messageId + 1)).not.toBeNull());
    await req(peer.client, { type: 'reply', messageId: both.sent.messageId + 1, body: 'AGREE ok', verdict: 'agree' });
    expect((await dflt).wait).toMatchObject({ conditionMet: true, timedOut: false });
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
    const question = await call<ThreadResult>(sdk, 'murmur_thread', { threadId: incoming.threadId, messageId: incoming.messageId });
    expect(question.messages.map((m) => [m.seq, m.body])).toEqual([
      [0, 'question for the agent\nwith a second line'],
      [1, 'INFO: and more'],
      [2, 'AGREE: looks right'],
    ]);
    const picked = await call<ReplyResult>(sdk, 'murmur_reply', { messageId: incoming.messageId, body: 'CHOICE ci', verdict: 'choice' });
    const pickedRow = await call<ThreadResult>(sdk, 'murmur_thread', { threadId: incoming.threadId, sinceSeq: 2 });
    expect(pickedRow.messages.map((m) => [m.seq, m.verdict])).toEqual([[picked.seq, 'choice']]);
    // The ask is answered, so a wait without timeoutMs (and one past the cap)
    // returns at once with the latest verdict.
    for (const extra of [{}, { timeoutMs: 99999999 }]) {
      const waited = await call<WaitResult>(sdk, 'murmur_wait', { threadId: incoming.threadId, until: 'any_replied', ...extra });
      expect(waited).toMatchObject({ conditionMet: true, timedOut: false });
      expect(waited.matrix[0]).toMatchObject({ recipient: 'mcpagent', state: 'replied', verdict: 'choice' });
    }
    const listing = await call<ThreadListEntry[]>(sdk, 'murmur_thread', {});
    expect(listing[0]).toMatchObject({ threadId: incoming.threadId, messages: 4, first: 'question for the agent' });
    expect(listing[0]?.participants).toEqual(['mcpagent', 'peerone']);
    expect(await call<ThreadListEntry[]>(sdk, 'murmur_thread', { limit: 1 })).toHaveLength(1);
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
  // returns once stdio is up; stop() fires the exit teardown, and stdin is
  // the stream the server watches for its client going away.
  async function launch(stack: Stack): Promise<{ stop: () => Promise<void>; stdin: PassThrough; running: Promise<void> }> {
    process.env.MURMUR_HOME = stack.home;
    const before = fakeStdio.started;
    let failure: unknown = null;
    const stdin = new PassThrough();
    const running = runMcpServer(stdin).catch((err: unknown) => {
      failure = err;
    });
    await settle(() => fakeStdio.started === before + 1, 'stdio transport started');
    expect(failure).toBeNull();
    return {
      stdin,
      running,
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

  it('returns and takes the session offline when its client closes stdin', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HANDLE = 'stdin-closes';
    const up = await launch(stack);
    expect(await presenceOf(stack, 'stdin-closes')).toBe('online');
    // A CLI that exits or is killed closes the server's stdin.
    up.stdin.resume();
    up.stdin.end();
    await up.running;
    await settle(async () => (await presenceOf(stack, 'stdin-closes')) !== 'online', 'offline once stdin closed');
  });

  it('in a remote hub home listens and hands pushes to the local messaging socket', async () => {
    const stack = await buildStack();
    writeFileSync(join(stack.home, 'remote'), '');
    const sockPath = join(freshHome(), 'cc.sock');
    const frames: string[] = [];
    const native = createServer((sock) => {
      let buf = '';
      sock.on('data', (chunk) => {
        buf += chunk.toString();
      });
      sock.on('end', () => {
        frames.push(buf);
        sock.end();
      });
    });
    await new Promise<void>((resolve) => {
      native.listen(sockPath, resolve);
    });
    process.env.CLAUDE_CODE_MESSAGING_SOCKET = sockPath;
    process.env.CLAUDE_CODE_MESSAGING_TOKEN = 'ntok';
    hostState.host = HOST;
    await announce(stack, 'remote-a');
    const up = await launch(stack);
    try {
      const alice = await connectRaw(stack, 'alice');
      const sent = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['remote-a'], body: 'over the tunnel' });
      await settle(() => frames.length === 1, 'local native frame');
      expect(frames[0]).toContain('"token":"ntok"');
      expect(frames[0]).toContain('over the tunnel');
      await settle(() => stack.store.getDeliveries(sent.messageId)[0]?.state === 'delivered', 'delivered');
    } finally {
      await up.stop();
      native.close();
    }
  });
});

// A session of another machine looks dead to the broker, so a second session
// gets its handle. Naming one must not move the MCP connection of the other.
describe('a rename of one session sharing a handle', () => {
  const REMOTE: HostInfo = { hostPid: 9002, hostStart: 'S2', ancestors: [9002] };
  const hostRef = (h: HostInfo) => ({ hostPid: h.hostPid, hostStart: h.hostStart });

  async function setup(): Promise<{ stack: Stack; remote: Pair; local: Pair; shared: string }> {
    hostState.host = HOST;
    const stack = await buildStack();
    const open = (cli: string, host: HostInfo) =>
      stack.core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: cli, host: hostRef(host), cwd: '/w/guard', source: 'startup' });
    const first = open('remote', REMOTE);
    const second = open('local', HOST);
    expect(second.handle).toBe(first.handle);
    const connect = async (host: HostInfo): Promise<Pair> => {
      const made = await pair(stack, identityFor(host, { CLAUDECODE: '1' }, '/w/guard'));
      await req<RegisterResult>(made.murmur, { type: 'register', clientKind: 'mcp', host: hostRef(host), client: 'claude', cwd: '/w/guard' });
      return made;
    };
    const remote = await connect(REMOTE);
    const local = await connect(HOST);
    stack.core.register({ handle: 'target' });
    return { stack, remote, local, shared: first.handle };
  }

  it('leaves the other session sending under its own handle', async () => {
    const { stack, remote, local, shared } = await setup();
    const hook = await connectRaw(stack, 'hookish');
    await req(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 'local', host: hostRef(HOST), title: 'Proveground S4c: bait' });
    const fromRemote = await call<{ threadId: number }>(remote.sdk, 'murmur_send', { to: ['target'], body: 'from the remote session', mode: 'silent' });
    const fromLocal = await call<{ threadId: number }>(local.sdk, 'murmur_send', { to: ['target'], body: 'from the local session', mode: 'silent' });
    expect(stack.store.threadMessages(fromRemote.threadId, null)[0]?.sender).toBe(shared);
    expect(stack.store.threadMessages(fromLocal.threadId, null)[0]?.sender).toBe('proveground-s4c-bait');
  });

  it('leaves a connection that names no host with the session that keeps the handle', async () => {
    const { stack, shared } = await setup();
    const bystander = await connectRaw(stack, shared);
    const hook = await connectRaw(stack, 'hookish');
    await req(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 'local', host: hostRef(HOST), title: 'Proveground S4c: bait' });
    const sent = await req<{ threadId: number }>(bystander.client, { type: 'send', threadId: null, to: ['target'], body: 'from a connection with no host', mode: 'silent' });
    expect(stack.store.threadMessages(sent.threadId, null)[0]?.sender).toBe(shared);
  });

  it('moves a connection that names no host with the rename when only an ended session shared the handle', async () => {
    const { stack, shared } = await setup();
    stack.core.sessionEvent({ event: 'session-end', client: 'claude', cliSessionId: 'remote', host: hostRef(REMOTE) });
    const bystander = await connectRaw(stack, shared);
    const hook = await connectRaw(stack, 'hookish');
    await req(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 'local', host: hostRef(HOST), title: 'Proveground S4c: bait' });
    const sent = await req<{ threadId: number }>(bystander.client, { type: 'send', threadId: null, to: ['target'], body: 'follows the identity', mode: 'silent' });
    expect(stack.store.threadMessages(sent.threadId, null)[0]?.sender).toBe('proveground-s4c-bait');
  });

  it('moves a connection that names no host when the old handle keeps only an ended session', async () => {
    const { stack, shared } = await setup();
    stack.core.sessionEvent({ event: 'session-end', client: 'claude', cliSessionId: 'remote', host: hostRef(REMOTE) });
    // The new name is an existing peer, so the session is reassigned and the ended row stays on the old handle.
    stack.core.register({ handle: 'proveground-s4c-bait' });
    const bystander = await connectRaw(stack, shared);
    const hook = await connectRaw(stack, 'hookish');
    await req(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 'local', host: hostRef(HOST), title: 'Proveground S4c: bait' });
    expect(stack.store.sessionByCliId('claude', 'remote')?.handle).toBe(shared);
    const sent = await req<{ threadId: number }>(bystander.client, { type: 'send', threadId: null, to: ['target'], body: 'no live sibling holds the old name', mode: 'silent' });
    expect(stack.store.threadMessages(sent.threadId, null)[0]?.sender).toBe('proveground-s4c-bait');
  });

  it('matches the host by pid and start together when the sessions share a handle', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    const twin: HostInfo = { hostPid: HOST.hostPid, hostStart: 'S-other-boot', ancestors: [HOST.hostPid] };
    const open = (cli: string, host: HostInfo) =>
      stack.core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: cli, host: hostRef(host), cwd: '/w/guard', source: 'startup' });
    const first = open('twin', twin);
    const second = open('local', HOST);
    expect(second.handle).toBe(first.handle);
    const connect = async (host: HostInfo): Promise<Pair> => {
      const made = await pair(stack, identityFor(host, { CLAUDECODE: '1' }, '/w/guard'));
      await req<RegisterResult>(made.murmur, { type: 'register', clientKind: 'mcp', host: hostRef(host), client: 'claude', cwd: '/w/guard' });
      return made;
    };
    const stayer = await connect(twin);
    const mover = await connect(HOST);
    stack.core.register({ handle: 'target' });
    const hook = await connectRaw(stack, 'hookish');
    await req(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 'local', host: hostRef(HOST), title: 'Proveground S4c: bait' });
    const fromStayer = await call<{ threadId: number }>(stayer.sdk, 'murmur_send', { to: ['target'], body: 'same pid, another start', mode: 'silent' });
    const fromMover = await call<{ threadId: number }>(mover.sdk, 'murmur_send', { to: ['target'], body: 'the renamed one', mode: 'silent' });
    expect(stack.store.threadMessages(fromStayer.threadId, null)[0]?.sender).toBe(first.handle);
    expect(stack.store.threadMessages(fromMover.threadId, null)[0]?.sender).toBe('proveground-s4c-bait');
  });

  it('matches the host by machine too: a twin with the same pid and start on another machine keeps its connection', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    const twin: HostInfo = { hostPid: HOST.hostPid, hostStart: HOST.hostStart, ancestors: [HOST.hostPid] };
    const ref = (host: HostInfo, machine?: string) => ({ ...hostRef(host), ...(machine !== undefined ? { machine } : {}) });
    const first = stack.core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'twin', host: ref(twin, '00000000-0000-4000-8000-000000000009'), cwd: '/w/guard', source: 'startup' });
    const second = stack.core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'local', host: ref(HOST), cwd: '/w/guard', source: 'startup' });
    expect(first.id).not.toBe(second.id);
    // Both machines' sessions hold one handle (an older row), and both are alive.
    stack.store.reassignSession(first.id, first.handle, second.handle, false);
    const connect = async (host: HostInfo, machine?: string): Promise<Pair> => {
      const made = await pair(stack, identityFor(host, { CLAUDECODE: '1' }, '/w/guard'));
      await req<RegisterResult>(made.murmur, { type: 'register', clientKind: 'mcp', host: ref(host, machine), client: 'claude', cwd: '/w/guard' });
      return made;
    };
    const stayer = await connect(twin, '00000000-0000-4000-8000-000000000009');
    const mover = await connect(HOST);
    stack.core.register({ handle: 'target' });
    const hook = await connectRaw(stack, 'hookish');
    await req(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 'local', host: ref(HOST), title: 'Proveground S4c: bait' });
    const fromStayer = await call<{ threadId: number }>(stayer.sdk, 'murmur_send', { to: ['target'], body: 'same pid and start, another machine', mode: 'silent' });
    const fromMover = await call<{ threadId: number }>(mover.sdk, 'murmur_send', { to: ['target'], body: 'the renamed one', mode: 'silent' });
    expect(stack.store.threadMessages(fromStayer.threadId, null)[0]?.sender).toBe(second.handle);
    expect(stack.store.threadMessages(fromMover.threadId, null)[0]?.sender).toBe('proveground-s4c-bait');
  });

  it('leaves the other session on its handle when murmur_name renames one', async () => {
    const { stack, remote, local, shared } = await setup();
    const named = await call<RegisterResult>(local.sdk, 'murmur_name', { handle: 'chosen' });
    expect(named.handle).toBe('chosen');
    const fromLocal = await call<{ threadId: number }>(local.sdk, 'murmur_send', { to: ['target'], body: 'from the renamed session', mode: 'silent' });
    expect(stack.store.threadMessages(fromLocal.threadId, null)[0]?.sender).toBe('chosen');
    const fromRemote = await call<{ threadId: number }>(remote.sdk, 'murmur_send', { to: ['target'], body: 'still the remote one', mode: 'silent' });
    expect(stack.store.threadMessages(fromRemote.threadId, null)[0]?.sender).toBe(shared);
    expect(stack.store.sessionByCliId('claude', 'remote')?.handle).toBe(shared);
    expect(stack.store.sessionByCliId('claude', 'local')?.handle).toBe('chosen');
  });
});

// The status tool of an MCP server, as an agent sees it.
async function sdkStatus(stack: Stack): Promise<unknown> {
  const made = await pair(stack);
  const result = (await made.sdk.callTool({ name: 'murmur_status', arguments: {} })) as CallToolResult;
  return result.content;
}

// Machines at the broker: what a connection and a rename see.
describe('sessions of two machines at one broker', () => {
  const MAC = '00000000-0000-4000-8000-000000000009';
  const ref = (host: HostInfo, machine?: string) => ({ hostPid: host.hostPid, hostStart: host.hostStart, ...(machine !== undefined ? { machine } : {}) });
  const connect = async (stack: Stack, host: HostInfo, machine?: string): Promise<Pair> => {
    const made = await pair(stack, identityFor(host, { CLAUDECODE: '1' }, '/w/guard'));
    await req<RegisterResult>(made.murmur, { type: 'register', clientKind: 'mcp', host: ref(host, machine), client: 'claude', cwd: '/w/guard' });
    return made;
  };
  const start = (stack: Stack, cli: string, host: HostInfo, machine?: string, title?: string) =>
    stack.core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: cli, host: ref(host, machine), cwd: '/w/guard', source: 'startup', ...(title !== undefined ? { title } : {}) });

  it('reads a machine field that is too long or not printable as absent, where it enters the broker', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    const hook = await connectRaw(stack, 'hookish');
    const startAs = (cli: string, pid: number, fields: Record<string, unknown>) =>
      req(hook.client, { type: 'session', event: 'session-start', client: 'claude', cliSessionId: cli, host: { hostPid: pid, hostStart: 'S' + String(pid), ...fields }, cwd: '/w/' + cli });
    await req(hook.client, { type: 'session', event: 'session-start', client: 'claude', cliSessionId: 'huge', host: { hostPid: HOST.hostPid, hostStart: HOST.hostStart, machine: 'x'.repeat(1_000_000), machineName: 'mac' }, cwd: '/w/huge' });
    await startAs('lines', 1002, { machine: 'a\nb', machineName: 'mac' });
    await startAs('typed', 1003, { machine: 12, machineName: 'mac' });
    await startAs('good', 1004, { machine: '00000000-0000-4000-8000-000000000009', machineName: 'mac' });
    await startAs('badlabel', 1005, { machine: '00000000-0000-4000-8000-000000000009', machineName: 'x'.repeat(300) });
    await startAs('upper', 1006, { machine: 'AAAAAAAA-0000-4000-8000-00000000000A', machineName: 'mac' });
    await startAs('lower', 1007, { machine: 'aaaaaaaa-0000-4000-8000-00000000000a', machineName: 'mac' });
    await startAs('spaced', 1009, { machine: 'AAAAAAAA-0000-4000-8000-00000000000A:4026531836', machineName: 'box' });
    // A label and an uptime without a machine end nothing: they mean something only next to one.
    await startAs('oldboot', 1010, { machine: '00000000-0000-4000-8000-0000000000b1', machineName: 'mac' });
    await new Promise((resolve) => setTimeout(resolve, 20));
    await startAs('nomachine', 1011, { machineName: 'mac', machineUptime: 0 });
    expect(stack.store.sessionByCliId('claude', 'oldboot')?.state).not.toBe('ended');
    expect(stack.store.sessionByCliId('claude', 'nomachine')).toMatchObject({ hostMachine: null, hostLabel: null });
    await startAs('notuuid', 1008, { machine: 'mac-boot', machineName: 'mac' });
    const row = (cli: string) => stack.store.sessionByCliId('claude', cli);
    for (const cli of ['huge', 'lines', 'typed']) {
      expect(row(cli)).toMatchObject({ hostMachine: null, hostLabel: null });
    }
    // Upper and lower case are one machine, and something that is not a uuid is no machine.
    expect(row('upper')?.hostMachine).toBe('aaaaaaaa-0000-4000-8000-00000000000a');
    expect(row('lower')?.hostMachine).toBe('aaaaaaaa-0000-4000-8000-00000000000a');
    expect(row('spaced')?.hostMachine).toBe('aaaaaaaa-0000-4000-8000-00000000000a:4026531836');
    expect(row('notuuid')).toMatchObject({ hostMachine: null, hostLabel: null });
    expect(row('good')).toMatchObject({ hostMachine: '00000000-0000-4000-8000-000000000009', hostLabel: 'mac' });
    expect(row('badlabel')).toMatchObject({ hostMachine: '00000000-0000-4000-8000-000000000009', hostLabel: null });
    // A register that names ancestors with a bad machine is a client of the broker's own machine.
    const cli = await connectRaw(stack, 'shell');
    const registered = await req<RegisterResult>(cli.client, { type: 'register', clientKind: 'cli', ancestors: [HOST.hostPid], machine: 'x'.repeat(1_000_000) });
    expect(registered.handle).toBe(row('huge')?.handle);
  });

  it('reads a fingerprint that is not 32 lower case hex characters as absent, where it enters the broker', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    const hook = await connectRaw(stack, 'hookish');
    const startAs = (cli: string, pid: number, fingerprint: unknown) =>
      req(hook.client, { type: 'session', event: 'session-start', client: 'claude', cliSessionId: cli, host: { hostPid: pid, hostStart: 'S' + String(pid), machine: MAC, machineName: 'mac', machineFingerprint: fingerprint }, cwd: '/w/' + cli });
    await startAs('huge', 2001, 'f'.repeat(1_000_000));
    await startAs('typed', 2002, 12);
    await startAs('upper', 2003, 'F'.repeat(32));
    await startAs('short', 2004, 'f'.repeat(31));
    await startAs('good', 2005, 'f'.repeat(32));
    const fingerprint = (cli: string) => stack.store.sessionByCliId('claude', cli)?.hostFingerprint;
    expect(['huge', 'typed', 'upper', 'short'].map(fingerprint)).toEqual([null, null, null, null]);
    expect(fingerprint('good')).toBe('f'.repeat(32));
  });

  it('does not let the lookup of a session by its host adopt the row of another session', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    const hook = await connectRaw(stack, 'hookish');
    const event = (cli: string, host: Record<string, unknown>, cwd: string) =>
      req(hook.client, { type: 'session', event: 'session-start', client: 'claude', cliSessionId: cli, host: { hostPid: 3000, hostStart: 'S3000', ...host }, cwd });
    // An old client without a machine, then another session with a machine from the same pid and start.
    await event('old', {}, '/w/guard');
    await event('new', { machine: MAC, machineName: 'mac' }, '/w/guard');
    const rows = stack.store.listSessions().map((r) => [r.cliSessionId, r.hostMachine]).sort();
    expect(rows).toEqual([['new', MAC], ['old', null]]);
    // Nor one of another folder.
    await event('nocli-old', { hostPid: 3001, hostStart: 'S3001' }, '/w/first');
    const upgraded = await req<{ handle: string }>(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: null, host: { hostPid: 3001, hostStart: 'S3001', machine: MAC, machineName: 'mac' }, cwd: '/w/second' });
    expect(upgraded.handle).not.toBe(stack.store.sessionByCliId('claude', 'nocli-old')?.handle);
  });

  it('names the session of a shell on another machine by its ancestors and its machine', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    stack.store.upsertPeer('renn', 'renn (owner)', null, true, Date.now());
    const remote = start(stack, 'mac', { hostPid: 4000, hostStart: 'T4000', ancestors: [4000] }, MAC);
    const register = async (fields: Record<string, unknown>): Promise<{ client: MurmurClient; result: RegisterResult }> => {
      const conn = await connectRaw(stack, 'shell-' + String(Math.random()).slice(2, 8));
      return { client: conn.client, result: await req<RegisterResult>(conn.client, { type: 'register', clientKind: 'cli', ...fields }) };
    };
    // On its own machine the shell speaks as the session; without the machine it is the owner's.
    expect((await register({ ancestors: [4000, 1], machine: MAC })).result.handle).toBe(remote.handle);
    // The broker reads the boot id as one lower case value however it is written.
    expect((await register({ ancestors: [4000, 1], machine: MAC.toUpperCase() })).result.handle).toBe(remote.handle);
    expect((await register({ ancestors: [4000, 1], machine: 'not-a-uuid' })).result.handle).toBe('renn');
    expect((await register({ ancestors: [4000, 1] })).result.handle).toBe('renn');
    // The owner typing in that shell is recorded as typing in that session.
    stack.core.register({ handle: 'target' });
    const owner = await register({ owner: true, ancestors: [4000, 1], machine: MAC });
    const sent = await req<{ threadId: number }>(owner.client, { type: 'send', threadId: null, to: ['target'], body: 'typed in the remote session', mode: 'silent' });
    expect(stack.store.threadMessages(sent.threadId, null)[0]?.via).toBe(remote.handle);
  });

  it('moves the connection of a session of another machine along with its title', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    const remote = start(stack, 'mac', HOST, MAC);
    const made = await connect(stack, HOST, MAC);
    stack.core.register({ handle: 'target' });
    const hook = await connectRaw(stack, 'hookish');
    // No CLI session id: the session is found by its host, with its machine.
    await req(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: null, host: ref(HOST, MAC), title: 'Proveground S4c: bait' });
    const sent = await call<{ threadId: number }>(made.sdk, 'murmur_send', { to: ['target'], body: 'after the title', mode: 'silent' });
    expect(remote.handle).not.toBe('proveground-s4c-bait');
    expect(stack.store.threadMessages(sent.threadId, null)[0]?.sender).toBe('proveground-s4c-bait');
  });

  it('never shows a fingerprint: not in status, a thread, a reply to a send or the answer to a hook', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    const FP = '1234567890abcdef1234567890abcdef';
    const hook = await connectRaw(stack, 'hookish');
    const replies: unknown[] = [];
    replies.push(await req(hook.client, { type: 'session', event: 'session-start', client: 'claude', cliSessionId: 'mac', host: { hostPid: 5001, hostStart: 'T5001', machine: '00000000-0000-4000-8000-000000000009', machineName: 'mac', machineUptime: 90, machineFingerprint: FP }, cwd: '/w/mac' }));
    const other = await connectRaw(stack, 'peer');
    replies.push(await req(other.client, { type: 'status' }));
    const sent = await req<{ threadId: number }>(other.client, { type: 'send', threadId: null, to: [stack.store.sessionByCliId('claude', 'mac')?.handle as string], body: 'hello', mode: 'silent' });
    replies.push(sent, await req(other.client, { type: 'thread', threadId: sent.threadId, sinceSeq: null, full: true }), await req(other.client, { type: 'threads', limit: 10 }));
    expect(stack.store.sessionByCliId('claude', 'mac')?.hostFingerprint).toBe(FP);
    expect(JSON.stringify(replies)).not.toContain(FP);
    expect(JSON.stringify(stack.core.status(() => ({ online: true })))).not.toContain(FP);
    expect(JSON.stringify(await sdkStatus(stack))).not.toContain(FP);
  });

  it('does not bind a connection waiting for its session to a session of another machine with the same pid and start', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    const waiting = await connect(stack, HOST);
    stack.core.register({ handle: 'target' });
    const hook = await connectRaw(stack, 'hookish');
    await req(hook.client, { type: 'session', event: 'session-start', client: 'claude', cliSessionId: 'mac', host: ref(HOST, MAC), cwd: '/w/mac' });
    const sent = await call<{ threadId: number }>(waiting.sdk, 'murmur_send', { to: ['target'], body: 'from the local host', mode: 'silent' });
    const sender = stack.store.threadMessages(sent.threadId, null)[0]?.sender;
    expect(sender).not.toBe(stack.store.sessionByCliId('claude', 'mac')?.handle);
    expect(stack.store.sessionByHost(HOST.hostPid, HOST.hostStart, null)?.handle).toBe(sender);
  });

  it('keeps a session of another machine alive past the lease only for a connection of its own machine', async () => {
    hostState.host = HOST;
    let clock = 1_000_000;
    const stack = await buildStack(() => clock);
    const row = start(stack, 'mac', HOST, MAC);
    clock += 13 * 60 * 60 * 1000;
    const wrong = await connect(stack, HOST, '00000000-0000-4000-8000-000000000008');
    expect(stack.core.claimable(row.handle, null)).toBe(true);
    const right = await connect(stack, HOST, MAC);
    expect(stack.core.claimable(row.handle, null)).toBe(false);
    right.murmur.close();
    wrong.murmur.close();
  });

  it('moves the connections of the renamed session only, by machine as well as pid and start', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    const twin: HostInfo = { hostPid: HOST.hostPid, hostStart: HOST.hostStart, ancestors: [HOST.hostPid] };
    const remote = start(stack, 'twin', twin, MAC);
    const local = start(stack, 'local', HOST);
    expect(remote.handle).not.toBe(local.handle);
    const stayer = await connect(stack, twin, MAC);
    const mover = await connect(stack, HOST);
    stack.core.register({ handle: 'target' });
    const hook = await connectRaw(stack, 'hookish');
    await req(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 'local', host: ref(HOST), title: 'Proveground S4c: bait' });
    const fromStayer = await call<{ threadId: number }>(stayer.sdk, 'murmur_send', { to: ['target'], body: 'the other machine', mode: 'silent' });
    const fromMover = await call<{ threadId: number }>(mover.sdk, 'murmur_send', { to: ['target'], body: 'the renamed one', mode: 'silent' });
    expect(stack.store.threadMessages(fromStayer.threadId, null)[0]?.sender).toBe(remote.handle);
    expect(stack.store.threadMessages(fromMover.threadId, null)[0]?.sender).toBe('proveground-s4c-bait');
  });

  it('lets a connection that names no host follow a rename when the old handle keeps only a crashed session', async () => {
    hostState.host = HOST;
    const stack = await buildStack();
    // The crashed session of this machine: no process, never ended; the new one takes its handle.
    const crashed = start(stack, 'crashed', { hostPid: 7001, hostStart: 'S7001', ancestors: [7001] });
    const local = start(stack, 'local', HOST);
    expect(local.handle).toBe(crashed.handle);
    // Naming it onto an existing peer reassigns the session and leaves the crashed row on the old handle.
    stack.core.register({ handle: 'proveground-s4c-bait' });
    const bystander = await connectRaw(stack, crashed.handle);
    stack.core.register({ handle: 'target' });
    const hook = await connectRaw(stack, 'hookish');
    await req(hook.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 'local', host: ref(HOST), title: 'Proveground S4c: bait' });
    expect(stack.store.sessionByCliId('claude', 'crashed')?.handle).toBe(crashed.handle);
    const sent = await req<{ threadId: number }>(bystander.client, { type: 'send', threadId: null, to: ['target'], body: 'no live holder remains', mode: 'silent' });
    expect(stack.store.threadMessages(sent.threadId, null)[0]?.sender).toBe('proveground-s4c-bait');
  });
});
