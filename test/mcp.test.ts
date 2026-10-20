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
import { buildMcpServer, runMcpServer } from '../src/mcp.js';
import type {
  AckResult,
  ClientMessage,
  CollectResult,
  GroupsResult,
  InboxResult,
  ReadResult,
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

const TOOL_NAMES = [
  'murmur_register',
  'murmur_send',
  'murmur_reply',
  'murmur_ack',
  'murmur_read',
  'murmur_inbox',
  'murmur_thread',
  'murmur_collect',
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
  const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} });
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

async function pair(stack: Stack): Promise<Pair> {
  const events: ServerEvent[] = [];
  const murmur = await MurmurClient.connect({
    socketPath: stack.paths.socketPath,
    token: stack.token,
    onEvent: (ev) => {
      events.push(ev);
    },
  });
  const server = buildMcpServer(murmur);
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

describe('SDK conformance over InMemoryTransport', () => {
  it('completes initialize and lists the 11 murmur tools', async () => {
    const stack = await buildStack();
    const { sdk } = await pair(stack);
    const info = sdk.getServerVersion();
    expect(info).toMatchObject({ name: 'murmur', version: '0.1.0' });
    const listed = await sdk.listTools();
    const names = listed.tools.map((t) => t.name).sort();
    expect(names).toEqual([...TOOL_NAMES].sort());
    expect(listed.tools).toHaveLength(11);
    for (const tool of listed.tools) {
      expect(tool.description?.length ?? 0).toBeGreaterThan(0);
      expect(tool.inputSchema.type).toBe('object');
    }
    const send = listed.tools.find((t) => t.name === 'murmur_send');
    expect(send?.inputSchema.required).toEqual(expect.arrayContaining(['to', 'body']));
  });

  it('registers, sends, times out a wait after 1 second and collects', async () => {
    const stack = await buildStack();
    const { sdk } = await pair(stack);
    const peer = await connectRaw(stack, 'peerone');
    const registered = await call<RegisterResult>(sdk, 'murmur_register', {
      handle: 'mcpagent',
      display: 'MCP Agent',
      role: 'tester',
    });
    expect(registered).toEqual({ handle: 'mcpagent', owner: false });
    expect(stack.store.getPeer('mcpagent')).toMatchObject({ display: 'MCP Agent', role: 'tester' });
    const sent = await call<SendResult>(sdk, 'murmur_send', {
      to: ['peerone'],
      body: 'hello from mcp',
      references: ['renn'],
      mode: 'push',
    });
    expect(sent.seq).toBe(0);
    expect(sent.deliveries.map((d) => d.recipient)).toEqual(['peerone']);
    await settle(() => peer.events.some((ev) => ev.event === 'push'), 'push to peerone');
    const pushed = peer.events.find((ev) => ev.event === 'push');
    expect(pushed?.event === 'push' && pushed.payload.message.id === sent.messageId).toBe(true);
    // A follow-up in the same thread covers the explicit threadId path.
    const again = await call<SendResult>(sdk, 'murmur_send', { threadId: sent.threadId, to: ['peerone'], body: 'again' });
    expect(again.threadId).toBe(sent.threadId);
    expect(again.seq).toBe(1);
    const started = Date.now();
    const waited = await call<WaitResult>(sdk, 'murmur_wait', {
      threadId: sent.threadId,
      until: 'all_replied',
      timeoutMs: 1000,
    });
    expect(Date.now() - started).toBeGreaterThanOrEqual(900);
    expect(waited).toMatchObject({ threadId: sent.threadId, conditionMet: false, timedOut: true });
    expect(waited.matrix[0]).toMatchObject({ recipient: 'peerone', state: 'sent' });
    const collected = await call<CollectResult>(sdk, 'murmur_collect', { threadId: sent.threadId });
    expect(collected.threadId).toBe(sent.threadId);
    expect(collected.askMessageId).toBe(again.messageId);
    expect(collected.recipients).toHaveLength(1);
    expect(collected.recipients[0]).toMatchObject({ recipient: 'peerone', state: 'sent', verdict: null });
    expect(collected.verdictTally).toEqual({ agree: 0, disagree: 0, info: 0 });
  });

  it('exercises reply, ack, read, inbox, thread, groups, status and an immediate wait', async () => {
    const stack = await buildStack();
    const { sdk, events } = await pair(stack);
    const peer = await connectRaw(stack, 'peerone');
    await call<RegisterResult>(sdk, 'murmur_register', { handle: 'mcpagent' });
    const incoming = await req<SendResult>(peer.client, {
      type: 'send',
      threadId: null,
      to: ['mcpagent'],
      body: 'question for the agent\nwith a second line',
    });
    await settle(() => events.some((ev) => ev.event === 'push'), 'push to mcpagent');
    const acked = await call<AckResult>(sdk, 'murmur_ack', { ids: [incoming.messageId] });
    expect(acked).toEqual({ acknowledged: [incoming.messageId] });
    const read = await call<ReadResult>(sdk, 'murmur_read', { ids: [incoming.messageId] });
    expect(read.messages).toHaveLength(1);
    expect(read.messages[0]?.body).toBe('question for the agent\nwith a second line');
    const inbox = await call<InboxResult>(sdk, 'murmur_inbox', { unreadOnly: false });
    expect(inbox.handle).toBe('mcpagent');
    expect(inbox.threads[0]?.threadId).toBe(incoming.threadId);
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
    expect(digest.messages[1]?.verdict).toBeNull();
    expect(digest.messages[2]?.verdict).toBe('agree');
    expect(digest.messages[2]?.references).toEqual(['peerone']);
    const since = await call<ThreadResult>(sdk, 'murmur_thread', { threadId: incoming.threadId, sinceSeq: 1, full: true });
    expect(since.messages.map((m) => m.seq)).toEqual([2]);
    // The ask is fully replied, so a wait without timeoutMs returns at once;
    // the matrix carries the latest reply's verdict.
    const waited = await call<WaitResult>(sdk, 'murmur_wait', { threadId: incoming.threadId, until: 'any_replied' });
    expect(waited).toMatchObject({ conditionMet: true, timedOut: false });
    expect(waited.matrix[0]).toMatchObject({ recipient: 'mcpagent', state: 'replied', verdict: 'agree' });
    const created = await call<GroupsResult[]>(sdk, 'murmur_groups', { action: 'create', name: 'mcpgroup' });
    expect(created.map((g) => g.name)).toEqual(['mcpgroup']);
    const added = await call<GroupsResult[]>(sdk, 'murmur_groups', {
      action: 'add',
      name: 'mcpgroup',
      members: ['peerone', 'mcpagent'],
    });
    expect(added[0]?.members).toEqual(['mcpagent', 'peerone']);
    const listed = await call<GroupsResult[]>(sdk, 'murmur_groups', { action: 'list' });
    expect(listed).toEqual(added);
    const status = await call<StatusResult>(sdk, 'murmur_status');
    expect(status.peers.find((p) => p.handle === 'mcpagent')).toMatchObject({ presence: 'online' });
    expect(status.peers.find((p) => p.handle === 'peerone')).toMatchObject({ presence: 'online' });
  });

  it('surfaces broker errors as tool errors and rejects invalid arguments', async () => {
    const stack = await buildStack();
    const { sdk } = await pair(stack);
    await call<RegisterResult>(sdk, 'murmur_register', { handle: 'mcpagent' });
    const failed = (await sdk.callTool({
      name: 'murmur_send',
      arguments: { to: ['nobody-here'], body: 'hi' },
    })) as CallToolResult;
    expect(failed.isError).toBe(true);
    const text = (failed.content[0] as { type: 'text'; text: string }).text;
    expect(text).toContain('unknown handle: nobody-here');
    // Input validation failures come back as isError results from the SDK
    // before the tool body runs.
    const missingBody = (await sdk.callTool({ name: 'murmur_send', arguments: { to: ['x'] } })) as CallToolResult;
    expect(missingBody.isError).toBe(true);
    expect((missingBody.content[0] as { type: 'text'; text: string }).text).toContain('Input validation error');
    const badUntil = (await sdk.callTool({
      name: 'murmur_wait',
      arguments: { threadId: 1, until: 'never' },
    })) as CallToolResult;
    expect(badUntil.isError).toBe(true);
    expect((badUntil.content[0] as { type: 'text'; text: string }).text).toContain('Input validation error');
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

  it('exits 1 with a diagnostic when the broker is down', async () => {
    const home = freshHome();
    const paths = getPaths(home);
    ensureHome(paths);
    process.env.MURMUR_HOME = home;
    process.env.MURMUR_HANDLE = 'mcp-down';
    const { exits, stderr } = exitThrows();
    await expect(runMcpServer()).rejects.toThrow('process.exit(1)');
    expect(exits).toEqual([1]);
    expect(stderr.join('')).toContain('murmur mcp: cannot connect to broker at ' + paths.socketPath + ' (broker_down)');
    expect(fakeStdio.constructed).toBe(0);
  });

  it('exits 1 with a diagnostic when the broker rejects the handle', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HOME = stack.home;
    process.env.MURMUR_HANDLE = 'Not A Valid Handle';
    const { exits, stderr } = exitThrows();
    await expect(runMcpServer()).rejects.toThrow('process.exit(1)');
    expect(exits).toEqual([1]);
    expect(stderr.join('')).toContain(
      'murmur mcp: broker rejected registration of handle Not A Valid Handle (bad_request)',
    );
    expect(fakeStdio.constructed).toBe(0);
  });

  it('registers the anon handle, serves over stdio and stays up', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HOME = stack.home;
    delete process.env.MURMUR_HANDLE;
    const handle = 'anon-' + String(process.pid);
    const exitSpy = vi.spyOn(process, 'exit');
    // Never resolves by design; a rejection would surface here.
    let failure: unknown = null;
    void runMcpServer().catch((err: unknown) => {
      failure = err;
    });
    await settle(() => stack.store.getPeer(handle) !== null, 'anon registration');
    await settle(() => fakeStdio.started === 1, 'stdio transport started');
    expect(fakeStdio.constructed).toBe(1);
    expect(failure).toBeNull();
    expect(exitSpy).not.toHaveBeenCalled();
    const viewer = await MurmurClient.connect({ socketPath: stack.paths.socketPath, token: stack.token });
    clients.push(viewer);
    const status = await req<StatusResult>(viewer, { type: 'status' });
    expect(status.peers.find((p) => p.handle === handle)).toMatchObject({ presence: 'online' });
    // The exit teardown is registered for real process exit, which a test
    // worker never performs; fire it directly so the broker connection is
    // torn down the way production would. The only exit listeners in this
    // worker are the ones runMcpServer registered.
    expect(process.listenerCount('exit')).toBeGreaterThanOrEqual(1);
    process.emit('exit', 0);
    await settle(async () => {
      const next = await req<StatusResult>(viewer, { type: 'status' });
      return next.peers.find((p) => p.handle === handle)?.presence !== 'online';
    }, 'anon handle offline after exit teardown');
  });
});
