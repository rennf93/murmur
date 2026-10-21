import { appendFileSync, chmodSync, existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { createConnection } from 'node:net';
import type { Server, Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Broker } from '../src/broker.js';
import type { BrokerOptions } from '../src/broker.js';
import { MurmurClient } from '../src/client.js';
import { ensureHome, getPaths, readToken } from '../src/config.js';
import type { MurmurPaths } from '../src/config.js';
import { MurmurCore } from '../src/core.js';
import type { CoreEvents } from '../src/core.js';
import type { NativeSendResult } from '../src/native.js';
import type { ProcInfo } from '../src/proc.js';
import {
  LineReader,
  MAX_BODY_CHARS,
  NOTIFY_BATCH_MS,
  PROTOCOL_VERSION,
  WAIT_MAX_MS,
  deliveryTag,
  encodeLine,
  renderDelivery,
  replyFooter,
} from '../src/protocol.js';
import type {
  AckResult,
  ClientMessage,
  CollectResult,
  FetchPushResult,
  GroupsResult,
  InboxResult,
  NotificationEvent,
  NotificationPayload,
  PushEvent,
  ReadResult,
  RegisterResult,
  ReplyResult,
  SendResult,
  ServerEvent,
  SessionResult,
  StatusResult,
  ThreadResult,
  WaitResult,
} from '../src/protocol.js';
import { Store } from '../src/store.js';

// Broker tests boot a real Broker on a temp MURMUR_HOME with a real Store and
// Core, then drive it with real MurmurClient connections.

// Pass-through config layer with a fault switch: readToken only ever throws
// MurmurError for real, so the broker's non-murmur fallback in the auth path
// is exercised by making the token store blow up in a weirder way.
const configState = vi.hoisted(() => ({ tokenFailure: null as unknown }));

vi.mock('../src/config.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/config.js')>();
  return {
    ...actual,
    readToken: (paths: Parameters<typeof actual.readToken>[0]) => {
      if (configState.tokenFailure !== null) {
        const failure = configState.tokenFailure;
        configState.tokenFailure = null;
        throw failure;
      }
      return actual.readToken(paths);
    },
  };
});

type NativeSendFn = NonNullable<BrokerOptions['nativeSend']>;

interface Stack {
  home: string;
  paths: MurmurPaths;
  token: string;
  store: Store;
  core: MurmurCore;
  broker: Broker;
  // The fake process table the core reads liveness from (see hostRef).
  proc: Map<number, ProcInfo>;
  stateChanges: Array<{ threadId: number; recipient: string; from: string; to: string }>;
}

interface Peer {
  client: MurmurClient;
  events: ServerEvent[];
}

async function buildStack(nativeSend?: NativeSendFn, extra: Partial<BrokerOptions> = {}): Promise<Stack> {
  const home = mkdtempSync(join(tmpdir(), 'murmur-broker-'));
  const paths = getPaths(home);
  ensureHome(paths);
  const store = Store.open(paths.dbPath);
  const stateChanges: Stack['stateChanges'] = [];
  const events: CoreEvents = {
    onAppend: () => {},
    onStateChange: (arg) => {
      stateChanges.push(arg);
    },
  };
  const proc = new Map<number, ProcInfo>();
  const core = new MurmurCore(store, events, undefined, () => proc);
  const opts: BrokerOptions = { paths, store, core, ...extra };
  if (nativeSend !== undefined) {
    opts.nativeSend = nativeSend;
  }
  const broker = await Broker.start(opts);
  const stack: Stack = { home, paths, token: readToken(paths), store, core, broker, proc, stateChanges };
  stacks.push(stack);
  return stack;
}

const HOST_START = 'Mon Jan  1 00:00:00 2026';

// A live host process for a session: the fake table reports it as running.
function hostRef(stack: Stack, pid: number): { hostPid: number; hostStart: string } {
  stack.proc.set(pid, { ppid: 1, start: HOST_START, comm: 'claude' });
  return { hostPid: pid, hostStart: HOST_START };
}

// A connection that announces a session and listens for pushes, like an
// adapter running inside the session's CLI.
async function connectAdapter(
  stack: Stack,
  handle: string,
  pid: number,
  extra: Record<string, unknown> = {},
): Promise<Peer> {
  const peer = await connectPeer(stack);
  await req<SessionResult>(peer.client, {
    type: 'session',
    event: 'session-start',
    client: 'claude',
    cliSessionId: 'cli-' + handle,
    host: hostRef(stack, pid),
    handle,
    listen: true,
    ...extra,
  });
  return peer;
}

async function connectPeer(
  stack: Stack,
  register?: { handle: string; display?: string; role?: string },
): Promise<Peer> {
  const events: ServerEvent[] = [];
  const client = await MurmurClient.connect({
    socketPath: stack.paths.socketPath,
    token: stack.token,
    onEvent: (ev: ServerEvent) => {
      events.push(ev);
    },
  });
  clients.push(client);
  if (register !== undefined) {
    const msg: Record<string, unknown> = { type: 'register', handle: register.handle, clientKind: 'mcp' };
    if (register.display !== undefined) {
      msg.display = register.display;
    }
    if (register.role !== undefined) {
      msg.role = register.role;
    }
    await req<RegisterResult>(client, msg);
  }
  return { client, events };
}

// MurmurClient.request types its body as Omit over the union, which keeps
// only common keys; tests send both valid and deliberately malformed
// messages, so the wire body is kept loose here on purpose.
function req<T>(client: MurmurClient, msg: Record<string, unknown>): Promise<T> {
  return client.request<T>(msg as unknown as Omit<ClientMessage, 'v' | 'id'>);
}

function pushesOf(peer: Peer): PushEvent[] {
  return peer.events.filter((ev): ev is PushEvent => ev.event === 'push');
}

function notificationsOf(peer: Peer): NotificationEvent[] {
  return peer.events.filter((ev): ev is NotificationEvent => ev.event === 'notification');
}

async function statusOf(viewer: MurmurClient): Promise<StatusResult> {
  return req<StatusResult>(viewer, { type: 'status' });
}

// Event-loop yield that works under both real timers and fake timers limited
// to setTimeout/clearTimeout: setImmediate stays real, so one turn lets
// pending socket I/O flush.
async function yieldTurn(): Promise<void> {
  await new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
}

// Polls a predicate (sync or async) across event-loop turns. Works under real
// timers and under fake timers that only fake setTimeout/clearTimeout.
async function settle(pred: () => boolean | Promise<boolean>, label: string): Promise<void> {
  const deadline = Date.now() + 10000;
  let turns = 0;
  while (!(await pred())) {
    if (Date.now() > deadline || turns > 200000) {
      throw new Error('timed out waiting for ' + label);
    }
    turns += 1;
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
  }
}

// Advances the fake clock window by window until the predicate holds. Used
// for notification flushes so the test passes regardless of whether the
// enqueue happened before or after a particular advance.
async function advanceUntil(pred: () => boolean, label: string): Promise<void> {
  for (let i = 0; i < 100 && !pred(); i += 1) {
    await vi.advanceTimersByTimeAsync(NOTIFY_BATCH_MS);
  }
  await settle(pred, label);
}

// Fakes only setTimeout/clearTimeout: waiter and notify-batch timers become
// controllable while setImmediate, Date.now and socket I/O stay real.
function fakeWaitWindow(): void {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
}

// Raw pre-auth socket for guard tests that must bypass MurmurClient.
interface Raw {
  socket: Socket;
  nextLine: () => Promise<string>;
  closed: Promise<void>;
}

function rawConnect(socketPath: string): Promise<Raw> {
  return new Promise((resolve, reject) => {
    const socket = createConnection(socketPath);
    const reader = new LineReader();
    const lines: string[] = [];
    const waiters: Array<(line: string) => void> = [];
    let closedResolve: (() => void) | null = null;
    const closed = new Promise<void>((resolveClose) => {
      closedResolve = resolveClose;
    });
    socket.setEncoding('utf8');
    socket.on('data', (chunk: string) => {
      for (const line of reader.feed(chunk)) {
        const waiter = waiters.shift();
        if (waiter !== undefined) {
          waiter(line);
        } else {
          lines.push(line);
        }
      }
    });
    socket.on('close', () => {
      for (const waiter of waiters.splice(0)) {
        waiter('');
      }
      if (closedResolve !== null) {
        closedResolve();
      }
    });
    socket.once('connect', () => {
      resolve({
        socket,
        nextLine: () => {
          const line = lines.shift();
          if (line !== undefined) {
            return Promise.resolve(line);
          }
          return new Promise<string>((res, rej) => {
            waiters.push((value: string) => {
              if (value === '') {
                rej(new Error('raw connection closed before a line arrived'));
              } else {
                res(value);
              }
            });
          });
        },
        closed,
      });
    });
    socket.once('error', reject);
  });
}

interface FakeNative {
  calls: Array<{ socketPath: string; token: string; text: string }>;
  mode: 'ok' | 'fail' | 'throw';
  fn: NativeSendFn;
}

function fakeNative(): FakeNative {
  const fake: FakeNative = {
    calls: [],
    mode: 'ok',
    fn: async (socketPath: string, token: string, text: string) => {
      fake.calls.push({ socketPath, token, text });
      if (fake.mode === 'throw') {
        throw new Error('transport exploded');
      }
      if (fake.mode === 'fail') {
        return { ok: false, error: 'peer refused' };
      }
      return { ok: true, error: null };
    },
  };
  return fake;
}

const stacks: Stack[] = [];
const clients: MurmurClient[] = [];
const raws: Raw[] = [];

afterEach(async () => {
  configState.tokenFailure = null;
  for (const client of clients.splice(0)) {
    client.close();
  }
  for (const raw of raws.splice(0)) {
    raw.socket.destroy();
  }
  for (const stack of stacks.splice(0)) {
    await stack.broker.stop();
    stack.store.close();
    rmSync(stack.home, { recursive: true, force: true });
  }
  vi.useRealTimers();
});

describe('auth', () => {
  it('rejects a bad token and destroys the connection', async () => {
    const stack = await buildStack();
    await expect(
      MurmurClient.connect({ socketPath: stack.paths.socketPath, token: 'wrong-token' }),
    ).rejects.toMatchObject({ code: 'bad_token', name: 'MurmurError' });
    // Raw variant also proves the socket is destroyed after the error reply.
    const raw = await rawConnect(stack.paths.socketPath);
    raws.push(raw);
    raw.socket.write(encodeLine({ v: 1, id: 1, type: 'auth', token: 'wrong-token' }));
    const line = await raw.nextLine();
    expect(JSON.parse(line)).toEqual({ v: PROTOCOL_VERSION, id: 1, ok: false, error: { code: 'bad_token', message: 'invalid token' } });
    await raw.closed;
  });

  it('answers a non-auth first line with auth_required and destroys the connection', async () => {
    const stack = await buildStack();
    const raw = await rawConnect(stack.paths.socketPath);
    raws.push(raw);
    raw.socket.write(encodeLine({ v: 1, id: 7, type: 'status' }));
    const line = await raw.nextLine();
    const reply = JSON.parse(line) as { v: number; id: number; ok: boolean; error: { code: string; message: string } };
    expect(reply.ok).toBe(false);
    expect(reply.id).toBe(7);
    expect(reply.error.code).toBe('auth_required');
    await raw.closed;
  });

  it('rejects a non-string token and replies with id 0 when the line has no id', async () => {
    const stack = await buildStack();
    const raw = await rawConnect(stack.paths.socketPath);
    raws.push(raw);
    raw.socket.write(encodeLine({ v: 1, id: 2, type: 'auth', token: 42 }));
    const line = await raw.nextLine();
    expect(JSON.parse(line)).toMatchObject({ id: 2, ok: false, error: { code: 'bad_token' } });
    await raw.closed;
    // A request line without an id is answered under id 0.
    const raw2 = await rawConnect(stack.paths.socketPath);
    raws.push(raw2);
    raw2.socket.write(JSON.stringify({ v: 1, type: 'auth', token: 'nope' }) + '\n');
    const line2 = await raw2.nextLine();
    expect(JSON.parse(line2)).toMatchObject({ id: 0, ok: false, error: { code: 'bad_token' } });
    await raw2.closed;
  });

  it('tolerates a malformed line before auth and still accepts a good auth afterwards', async () => {
    const stack = await buildStack();
    const raw = await rawConnect(stack.paths.socketPath);
    raws.push(raw);
    raw.socket.write('this is not json\n');
    const first = await raw.nextLine();
    expect(JSON.parse(first)).toMatchObject({ id: 0, ok: false, error: { code: 'bad_request' } });
    raw.socket.write(encodeLine({ v: 1, id: 3, type: 'auth', token: stack.token }));
    const second = await raw.nextLine();
    expect(JSON.parse(second)).toEqual({ v: PROTOCOL_VERSION, id: 3, ok: true, result: null });
    // A JSON array is a valid line but not an object.
    raw.socket.write('[1,2,3]\n');
    const third = await raw.nextLine();
    expect(JSON.parse(third)).toMatchObject({
      id: 0,
      ok: false,
      error: { code: 'bad_request', message: 'line must be a JSON object' },
    });
  });

  it('rejects a second auth on an authenticated connection', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    await expect(req(alice.client, { type: 'auth', token: stack.token })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'connection is already authenticated',
    });
  });
});

describe('register and presence', () => {
  it('binds the handle so other connections see it online', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice', display: 'Alice', role: 'tester' });
    const result = await req<RegisterResult>(alice.client, {
      type: 'register',
      handle: 'alice',
      clientKind: 'mcp',
      display: 'Alice',
      role: 'tester',
    });
    expect(result).toEqual({ handle: 'alice', owner: false });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const status = await statusOf(bob.client);
    const aliceStatus = status.peers.find((p) => p.handle === 'alice');
    expect(aliceStatus).toBeDefined();
    expect(aliceStatus?.presence).toBe('online');
    expect(aliceStatus?.display).toBe('Alice');
    expect(aliceStatus?.role).toBe('tester');
    const row = stack.store.getPeer('alice');
    expect(row?.display).toBe('Alice');
    expect(row?.isOwner).toBe(false);
  });

  it('rebinds when one connection registers twice', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack, { handle: 'alice' });
    await req(conn.client, { type: 'register', handle: 'alice2', clientKind: 'mcp' });
    const watcher = await connectPeer(stack, { handle: 'watch' });
    const status = await statusOf(watcher.client);
    expect(status.peers.find((p) => p.handle === 'alice')?.presence).not.toBe('online');
    expect(status.peers.find((p) => p.handle === 'alice2')?.presence).toBe('online');
  });

  it('validates register fields', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack);
    await expect(req(conn.client, { type: 'register', handle: 5, clientKind: 'mcp' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'handle must be a string',
    });
    await expect(req(conn.client, { type: 'register', handle: 'x', clientKind: 'mcp', display: 9 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'display must be a string',
    });
    await expect(req(conn.client, { type: 'register', handle: 'x', clientKind: 'mcp', role: [] })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'role must be a string',
    });
    await expect(req(conn.client, { type: 'register', handle: 'x', clientKind: 'robot' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'clientKind must be one of: mcp, cli, hook, adapter',
    });
  });

  it('rejects handle-bound requests before any register and unknown types', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack);
    await expect(req(conn.client, { type: 'send', threadId: null, to: ['x'], body: 'hi' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'no handle is registered on this connection',
    });
    await expect(req(conn.client, { type: 'status' })).resolves.toBeDefined();
    await expect(req(conn.client, { type: 'bogus' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'unknown message type: bogus',
    });
    // A non-string type lands in the same default arm.
    await expect(req(conn.client, { type: 5 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'unknown message type: 5',
    });
  });
});

describe('send and push', () => {
  it('pushes to a listening adapter and marks the delivery sent', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectAdapter(stack, 'bob', 4001);
    const result = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'first line\nsecond line',
      references: ['carol'],
    });
    expect(result.seq).toBe(0);
    expect(result.deliveries).toHaveLength(1);
    // Deliveries are re-read after the pushes ran, so the reply already
    // shows the settled state.
    expect(result.deliveries[0]).toMatchObject({ recipient: 'bob', state: 'sent' });
    expect(result.deliveries[0]?.sentAt).not.toBeNull();
    await settle(() => pushesOf(bob).length > 0, 'push event');
    const push = pushesOf(bob)[0];
    expect(push.v).toBe(PROTOCOL_VERSION);
    expect(push.type).toBe('event');
    expect(push.event).toBe('push');
    expect(push.payload.mode).toBe('push');
    expect(push.payload.message.id).toBe(result.messageId);
    expect(push.payload.message.body).toBe('first line\nsecond line');
    expect(push.payload.message.references).toEqual(['carol']);
    expect(push.payload.footer).toBe(replyFooter({ id: result.messageId, threadId: result.threadId }));
    expect(push.payload.text).toBe(
      renderDelivery(push.payload.message, 'first line\nsecond line', ['bob']),
    );
    expect(push.payload.text).toContain(deliveryTag(result.messageId) + 't' + String(result.threadId) + ' s0 from alice to bob]');
    const row = stack.store.getDeliveries(result.messageId).find((d) => d.recipient === 'bob');
    expect(row?.state).toBe('sent');
    expect(row?.sentAt).not.toBeNull();
    expect(row?.deliveredAt).toBeNull();
  });

  it('leaves the delivery queued for a connected peer that is not listening', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const result = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'mcp only' });
    expect(result.deliveries[0]).toMatchObject({ recipient: 'bob', state: 'queued', sentAt: null });
    expect(pushesOf(bob)).toHaveLength(0);
  });

  it('leaves the delivery queued for a registered peer with no live connection', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bobConn = await connectPeer(stack, { handle: 'bob' });
    bobConn.client.close();
    const watcher = await connectPeer(stack, { handle: 'watch' });
    await settle(async () => {
      const status = await statusOf(watcher.client);
      return status.peers.find((p) => p.handle === 'bob')?.presence !== 'online';
    }, 'bob offline');
    const result = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'offline delivery',
    });
    expect(result.deliveries[0]).toMatchObject({ recipient: 'bob', state: 'queued', sentAt: null });
  });

  it('validates send fields', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    await expect(req(alice.client, { type: 'send', threadId: null, to: 'bob', body: 'hi' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'to must be an array of strings',
    });
    await expect(req(alice.client, { type: 'send', threadId: null, to: ['bob', 3], body: 'hi' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'to must be an array of strings',
    });
    await expect(req(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 7 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'body must be a string',
    });
    await expect(req(alice.client, { type: 'send', threadId: 'nine', to: ['bob'], body: 'hi' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'threadId must be a number',
    });
    await expect(
      req(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'hi', mode: 'telepathy' }),
    ).rejects.toMatchObject({
      code: 'bad_request',
      message: 'mode must be one of: push, notify, silent',
    });
    await expect(
      req(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'hi', references: 'bob' }),
    ).rejects.toMatchObject({
      code: 'bad_request',
      message: 'references must be an array of strings',
    });
  });

  it('rejects unknown recipients and unknown threads', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    await expect(req(alice.client, { type: 'send', threadId: null, to: ['nobody'], body: 'hi' })).rejects.toMatchObject({
      code: 'unknown_handle',
    });
    await expect(req(alice.client, { type: 'send', threadId: null, to: ['@nodev'], body: 'hi' })).rejects.toMatchObject({
      code: 'unknown_group',
    });
    await expect(req(alice.client, { type: 'send', threadId: 424242, to: ['alice'], body: 'hi' })).rejects.toMatchObject({
      code: 'unknown_thread',
    });
  });

  it('appends into an existing thread with the next seq', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const first = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['alice'], body: 'one' });
    const second = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: first.threadId,
      to: ['alice'],
      body: 'two',
    });
    expect(second.threadId).toBe(first.threadId);
    expect(second.seq).toBe(1);
    expect(stack.store.threadMessages(first.threadId, null)).toHaveLength(2);
  });
});

describe('native transport sends', () => {
  it('uses the injected nativeSend on the ok path and marks sent, not delivered', async () => {
    const fake = fakeNative();
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req(bob.client, {
      type: 'native-register',
      handle: 'bob',
      socketPath: '/tmp/fake-bob.sock',
      token: 'bob-native-token',
    });
    const result = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'via native',
    });
    expect(fake.calls).toHaveLength(1);
    expect(fake.calls[0]?.socketPath).toBe('/tmp/fake-bob.sock');
    expect(fake.calls[0]?.token).toBe('bob-native-token');
    expect(fake.calls[0]?.text).toContain('via native');
    expect(fake.calls[0]?.text).toContain(deliveryTag(result.messageId));
    expect(fake.calls[0]?.text).toContain('murmur_reply messageId ' + String(result.messageId));
    // Native wins over the live connection: no push event arrives.
    expect(pushesOf(bob)).toHaveLength(0);
    expect(result.deliveries[0]).toMatchObject({ recipient: 'bob', state: 'sent' });
    const row = stack.store.getDeliveries(result.messageId).find((d) => d.recipient === 'bob');
    expect(row?.state).toBe('sent');
    expect(row?.sentAt).not.toBeNull();
    expect(row?.deliveredAt).toBeNull();
    const transport = stack.store.getNativeTransport('bob');
    expect(transport).toEqual({ owner: 'bob', socketPath: '/tmp/fake-bob.sock', token: 'bob-native-token' });
  });

  it('keeps the delivery queued when the native peer reports failure', async () => {
    const fake = fakeNative();
    fake.mode = 'fail';
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req(bob.client, { type: 'native-register', handle: 'bob', socketPath: '/tmp/fake-bob.sock', token: 't' });
    const result = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'will fail' });
    expect(fake.calls).toHaveLength(1);
    expect(result.deliveries[0]).toMatchObject({ recipient: 'bob', state: 'queued' });
  });

  it('keeps the delivery queued when the native transport throws', async () => {
    const fake = fakeNative();
    fake.mode = 'throw';
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req(bob.client, { type: 'native-register', handle: 'bob', socketPath: '/tmp/fake-bob.sock', token: 't' });
    const result = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'will throw' });
    expect(fake.calls).toHaveLength(1);
    expect(result.deliveries[0]).toMatchObject({ recipient: 'bob', state: 'queued' });
  });

  it('hands the truncated delivered body to the native transport', async () => {
    const fake = fakeNative();
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req(bob.client, { type: 'native-register', handle: 'bob', socketPath: '/tmp/fake-bob.sock', token: 't' });
    const longBody = 'x'.repeat(MAX_BODY_CHARS + 50);
    const result = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: longBody });
    expect(stack.store.getDeliveries(result.messageId)[0]).toMatchObject({ recipient: 'bob', state: 'sent' });
    expect(stack.store.messageById(result.messageId)?.truncated).toBe(true);
    expect(fake.calls[0]?.text).toContain(
      'x'.repeat(MAX_BODY_CHARS) + '\n[truncated: full body stored, read it with murmur_thread full: true]',
    );
    expect(fake.calls[0]?.text).not.toContain('x'.repeat(MAX_BODY_CHARS + 1));
  });

  it('validates native-register fields', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack, { handle: 'alice' });
    await expect(req(conn.client, { type: 'native-register', socketPath: '/tmp/x.sock', token: 't' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'handle must be a string',
    });
    await expect(req(conn.client, { type: 'native-register', handle: 'alice', token: 't' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'socketPath must be a string',
    });
    await expect(req(conn.client, { type: 'native-register', handle: 'alice', socketPath: '/tmp/x.sock' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'token must be a string',
    });
  });
});

describe('reply, ack, read and inbox', () => {
  it('marks the ask replied with latency when a recipient replies', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'please decide' });
    const reply = await req<ReplyResult>(bob.client, {
      type: 'reply',
      messageId: ask.messageId,
      body: 'AGREE, ship it',
      verdict: 'agree',
    });
    expect(reply.messageId).not.toBe(ask.messageId);
    expect(reply.threadId).toBe(ask.threadId);
    expect(reply.seq).toBe(1);
    const delivery = stack.store.getDeliveries(ask.messageId).find((d) => d.recipient === 'bob');
    expect(delivery?.state).toBe('replied');
    expect(delivery?.latencyMs).not.toBeNull();
    expect(delivery?.latencyMs ?? -1).toBeGreaterThanOrEqual(0);
    expect(delivery?.repliedAt).not.toBeNull();
    expect(stack.stateChanges).toContainEqual({
      threadId: ask.threadId,
      recipient: 'bob',
      from: 'queued',
      to: 'replied',
    });
  });

  it('validates reply fields', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['alice'], body: 'ask' });
    await expect(req(alice.client, { type: 'reply', messageId: 'one', body: 'x' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'messageId must be a number',
    });
    await expect(req(alice.client, { type: 'reply', messageId: ask.messageId, body: 9 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'body must be a string',
    });
    await expect(req(alice.client, { type: 'reply', messageId: ask.messageId, body: 'x', verdict: 'maybe' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'verdict must be one of: agree, disagree, info, choice',
    });
    await expect(req(alice.client, { type: 'reply', messageId: ask.messageId, body: 'x', references: 'y' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'references must be an array of strings',
    });
    await expect(req(alice.client, { type: 'reply', messageId: 987654, body: 'x' })).rejects.toMatchObject({
      code: 'unknown_message',
    });
  });

  it('acks only ids the handle may acknowledge', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'alice'], body: 'ack me' });
    // A delivery that is still queued cannot be acknowledged.
    expect(await req<AckResult>(bob.client, { type: 'ack', ids: [ask.messageId] })).toEqual({ acknowledged: [] });
    await req<ReadResult>(bob.client, { type: 'read', ids: [ask.messageId] });
    const acked = await req<AckResult>(bob.client, { type: 'ack', ids: [ask.messageId, 987654] });
    expect(acked).toEqual({ acknowledged: [ask.messageId] });
    const bobRow = stack.store.getDeliveries(ask.messageId).find((d) => d.recipient === 'bob');
    expect(bobRow?.state).toBe('acknowledged');
    expect(stack.stateChanges).toContainEqual({
      threadId: ask.threadId,
      recipient: 'bob',
      from: 'delivered',
      to: 'acknowledged',
    });
    // A second ack of the same id is no longer eligible.
    const again = await req<AckResult>(bob.client, { type: 'ack', ids: [ask.messageId] });
    expect(again).toEqual({ acknowledged: [] });
    await expect(req(bob.client, { type: 'ack', ids: 'one' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'ids must be an array of numbers',
    });
    await expect(req(bob.client, { type: 'ack', ids: ['one'] })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'ids must be an array of numbers',
    });
  });

  it('reads full bodies addressed to the handle and marks them delivered', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'body for bob' });
    const other = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['alice'], body: 'private' });
    const read = await req<ReadResult>(bob.client, { type: 'read', ids: [ask.messageId, other.messageId] });
    expect(read.messages).toHaveLength(1);
    expect(read.messages[0]?.id).toBe(ask.messageId);
    expect(read.messages[0]?.body).toBe('body for bob');
    const row = stack.store.getDeliveries(ask.messageId).find((d) => d.recipient === 'bob');
    expect(row?.state).toBe('delivered');
    expect(stack.stateChanges).toContainEqual({
      threadId: ask.threadId,
      recipient: 'bob',
      from: 'queued',
      to: 'delivered',
    });
    await expect(req(bob.client, { type: 'read', ids: 'x' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'ids must be an array of numbers',
    });
  });

  it('lists the inbox with unread counts in both filter modes', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const first = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'one' });
    const second = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: first.threadId,
      to: ['bob'],
      body: 'two',
    });
    const inbox = await req<InboxResult>(bob.client, { type: 'inbox', unreadOnly: true });
    expect(inbox.handle).toBe('bob');
    expect(inbox.unreadTotal).toBe(2);
    expect(inbox.threads).toHaveLength(1);
    expect(inbox.threads[0]?.unread).toBe(2);
    expect(inbox.threads[0]?.latestSeq).toBe(1);
    expect(inbox.threads[0]?.messages).toHaveLength(2);
    // Reading keeps a delivery unread (delivered still counts); acking clears
    // it. unreadOnly false still lists the acked message, unreadOnly true
    // drops it.
    await req<ReadResult>(bob.client, { type: 'read', ids: [second.messageId] });
    const stillUnread = await req<InboxResult>(bob.client, { type: 'inbox', unreadOnly: true });
    expect(stillUnread.unreadTotal).toBe(2);
    await req<AckResult>(bob.client, { type: 'ack', ids: [second.messageId] });
    const all = await req<InboxResult>(bob.client, { type: 'inbox', unreadOnly: false });
    expect(all.unreadTotal).toBe(1);
    expect(all.threads[0]?.unread).toBe(1);
    expect(all.threads[0]?.latestSeq).toBe(1);
    expect(all.threads[0]?.messages).toHaveLength(2);
    const unread = await req<InboxResult>(bob.client, { type: 'inbox', unreadOnly: true });
    expect(unread.threads).toHaveLength(1);
    expect(unread.threads[0]?.messages).toHaveLength(1);
    expect(unread.threads[0]?.latestSeq).toBe(0);
    await expect(req(bob.client, { type: 'inbox', unreadOnly: 'yes' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'unreadOnly must be a boolean',
    });
  });
});

describe('thread and collect over the wire', () => {
  it('returns the full thread and honors sinceSeq', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const first = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['alice'], body: 'm0' });
    await req<SendResult>(alice.client, { type: 'send', threadId: first.threadId, to: ['alice'], body: 'm1' });
    const full = await req<ThreadResult>(alice.client, {
      type: 'thread',
      threadId: first.threadId,
      sinceSeq: null,
      full: true,
    });
    expect(full.threadId).toBe(first.threadId);
    expect(full.latestSeq).toBe(1);
    expect(full.messages.map((m) => m.body)).toEqual(['m0', 'm1']);
    const since = await req<ThreadResult>(alice.client, {
      type: 'thread',
      threadId: first.threadId,
      sinceSeq: 0,
      full: false,
    });
    expect(since.messages).toHaveLength(1);
    expect(since.messages[0]?.firstLine).toBe('m1');
    await expect(
      req(alice.client, { type: 'thread', threadId: first.threadId, sinceSeq: null, full: true, messageId: 'x' }),
    ).rejects.toMatchObject({ code: 'bad_request' });
    await expect(
      req(alice.client, { type: 'thread', threadId: first.threadId, sinceSeq: null, full: true, messageId: 98765 }),
    ).rejects.toMatchObject({ code: 'bad_request' });
    await expect(req(alice.client, { type: 'thread', threadId: 777, sinceSeq: null, full: true })).rejects.toMatchObject({
      code: 'unknown_thread',
    });
    await expect(
      req(alice.client, { type: 'thread', threadId: first.threadId, sinceSeq: null, full: 'yes' }),
    ).rejects.toMatchObject({
      code: 'bad_request',
      message: 'full must be a boolean',
    });
    await expect(
      req(alice.client, { type: 'thread', threadId: first.threadId, sinceSeq: 'x', full: true }),
    ).rejects.toMatchObject({
      code: 'bad_request',
      message: 'sinceSeq must be a number',
    });
  });

  it('collects the matrix and verdict tally for the latest ask', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'decide' });
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'AGREE', verdict: 'agree' });
    const collect = await req<CollectResult>(alice.client, { type: 'collect', threadId: ask.threadId });
    expect(collect.threadId).toBe(ask.threadId);
    expect(collect.askMessageId).toBe(ask.messageId);
    expect(collect.recipients).toHaveLength(1);
    expect(collect.recipients[0]).toMatchObject({ recipient: 'bob', state: 'replied', verdict: 'agree' });
    expect(collect.recipients[0]?.repliedLatencyMs).not.toBeNull();
    expect(collect.verdictTally).toEqual({ agree: 1, disagree: 0, info: 0, choice: 0 });
    await expect(req(alice.client, { type: 'collect', threadId: 555 })).rejects.toMatchObject({
      code: 'unknown_thread',
    });
  });
});

describe('wait', () => {
  it('answers any_replied immediately when a reply already exists', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'quick' });
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'done' });
    const result = await req<WaitResult>(alice.client, {
      type: 'wait',
      threadId: ask.threadId,
      until: 'any_replied',
      timeoutMs: 5000,
    });
    expect(result).toMatchObject({ threadId: ask.threadId, conditionMet: true, timedOut: false });
    expect(result.matrix[0]).toMatchObject({ recipient: 'bob', state: 'replied' });
    expect(result.replies.map((r) => [r.sender, r.body])).toEqual([['bob', 'done']]);
  });

  it('releases any_replied when a reply arrives from another connection', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'ping' });
    const pending = req<WaitResult>(alice.client, {
      type: 'wait',
      threadId: ask.threadId,
      until: 'any_replied',
      timeoutMs: 20000,
    });
    await yieldTurn();
    await yieldTurn();
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'pong' });
    const result = await pending;
    expect(result).toMatchObject({ threadId: ask.threadId, conditionMet: true, timedOut: false });
  });

  it('releases all_replied only after every recipient replied', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    const ask = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob', 'carol'],
      body: 'both please',
    });
    const pending = req<WaitResult>(alice.client, {
      type: 'wait',
      threadId: ask.threadId,
      until: 'all_replied',
      timeoutMs: 20000,
    });
    await yieldTurn();
    await yieldTurn();
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'first' });
    let settled = false;
    pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = false;
      },
    );
    await yieldTurn();
    await yieldTurn();
    expect(settled).toBe(false);
    await req<ReplyResult>(carol.client, { type: 'reply', messageId: ask.messageId, body: 'second' });
    const result = await pending;
    expect(result).toMatchObject({ threadId: ask.threadId, conditionMet: true, timedOut: false });
    expect(result.matrix.map((r) => r.recipient).sort()).toEqual(['bob', 'carol']);
  });

  it('accepts a choice verdict over the wire and hands it to the waiting asker', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5089);
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'Reply CHOICE ci or CHOICE now' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 20000 });
    await yieldTurn();
    await yieldTurn();
    const reply = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'CHOICE ci', verdict: 'choice' });
    const result = await pending;
    expect(result.replies.map((r) => [r.id, r.verdict, r.body])).toEqual([[reply.messageId, 'choice', 'CHOICE ci']]);
  });

  it('holds replies for an active wait, returns them in the result and marks them delivered (all_replied)', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5090);
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'both' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 20000 });
    await yieldTurn();
    await yieldTurn();
    const first = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'one', verdict: 'agree' });
    const second = await req<ReplyResult>(carol.client, { type: 'reply', messageId: ask.messageId, body: 'y'.repeat(MAX_BODY_CHARS + 5) });
    const result = await pending;
    expect(result.conditionMet).toBe(true);
    expect(result.replies.map((r) => [r.id, r.sender, r.verdict, r.seq])).toEqual([
      [first.messageId, 'bob', 'agree', 1],
      [second.messageId, 'carol', null, 2],
    ]);
    expect(result.replies[0]?.body).toBe('one');
    expect(result.replies[1]?.body).toContain('[truncated');
    expect(pushesOf(alice).filter((p) => p.payload.message.isReply)).toHaveLength(0);
    for (const id of [first.messageId, second.messageId]) {
      expect(stack.store.getDeliveries(id).map((d) => [d.recipient, d.state])).toEqual([['alice', 'delivered']]);
    }
  });

  it('sends the replier no queued_delivered notice for a reply handed over in the wait', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectAdapter(stack, 'bob', 5091);
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'quick one' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 20000 });
    await yieldTurn();
    await yieldTurn();
    const answer = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'done' });
    expect((await pending).replies.map((r) => r.id)).toEqual([answer.messageId]);
    await vi.advanceTimersByTimeAsync(NOTIFY_BATCH_MS * 2);
    await yieldTurn();
    expect(notificationsOf(bob)).toHaveLength(0);
  });

  it('still sends queued_delivered for a reply that waited on a closed session and a later wait hands over', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const asker = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectAdapter(stack, 'bob', 5092);
    const ask = await req<SendResult>(asker.client, { type: 'send', threadId: null, to: ['bob'], body: 'later' });
    asker.client.close();
    const answer = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'while you were away' });
    expect(stack.store.getDeliveries(answer.messageId).map((d) => [d.recipient, d.state])).toEqual([['alice', 'queued']]);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const result = await req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 20000 });
    expect(result.replies.map((r) => r.id)).toEqual([answer.messageId]);
    const delivered = (): NotificationPayload[] => notificationsOf(bob).flatMap((ev) => ev.payload).filter((p) => p.kind === 'queued_delivered');
    await advanceUntil(() => delivered().length > 0, 'queued_delivered for bob');
    expect(delivered()).toMatchObject([{ fromHandle: 'alice', threadId: ask.threadId }]);
  });

  it('hands over only replies to the latest ask, not other queued messages in the thread', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const first = await req<SendResult>(bob.client, { type: 'send', threadId: null, to: ['alice'], body: 'bob asks' });
    await req<ReplyResult>(alice.client, { type: 'reply', messageId: first.messageId, body: 'to bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: first.threadId, to: ['bob'], body: 'alice asks' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 20000 });
    await yieldTurn();
    await yieldTurn();
    const answer = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'to alice' });
    const result = await pending;
    expect(result.replies.map((r) => r.id)).toEqual([answer.messageId]);
    expect(stack.store.getDeliveries(first.messageId).map((d) => d.state)).not.toContain('delivered');
  });

  it('returns a held reply when the wait times out', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5091);
    const bob = await connectPeer(stack, { handle: 'bob' });
    await connectPeer(stack, { handle: 'carol' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'both' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 150 });
    await yieldTurn();
    await yieldTurn();
    const first = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'only me' });
    const result = await pending;
    expect(result).toMatchObject({ conditionMet: false, timedOut: true });
    expect(result.replies.map((r) => [r.id, r.sender, r.body])).toEqual([[first.messageId, 'bob', 'only me']]);
    expect(pushesOf(alice).filter((p) => p.payload.message.isReply)).toHaveLength(0);
    expect(stack.store.getDeliveries(first.messageId).map((d) => d.state)).toEqual(['delivered']);
  });

  it('pushes a reply that arrives after the wait resolved', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5092);
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'both' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 20000 });
    await yieldTurn();
    await yieldTurn();
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'first' });
    expect((await pending).replies).toHaveLength(1);
    const late = await req<ReplyResult>(carol.client, { type: 'reply', messageId: ask.messageId, body: 'late' });
    expect(pushesOf(alice).some((p) => p.payload.message.id === late.messageId)).toBe(true);
    expect(stack.store.getDeliveries(late.messageId).map((d) => d.state)).toEqual(['sent']);
  });

  it('keeps a held reply out of the queued push when its recipient reconnects mid-wait', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await connectPeer(stack, { handle: 'carol' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'q' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 400 });
    await yieldTurn();
    await yieldTurn();
    const first = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'held' });
    const again = await connectAdapter(stack, 'alice', 5094);
    expect(pushesOf(again).filter((p) => p.payload.message.isReply)).toHaveLength(0);
    expect((await pending).replies.map((r) => r.id)).toEqual([first.messageId]);
  });

  it('pushes a reply to an older ask during a wait instead of holding it', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5095);
    const bob = await connectPeer(stack, { handle: 'bob' });
    await connectPeer(stack, { handle: 'carol' });
    const m1 = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'q1' });
    await req<SendResult>(alice.client, { type: 'send', threadId: m1.threadId, to: ['carol'], body: 'q2' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: m1.threadId, until: 'all_replied', timeoutMs: 150 });
    await yieldTurn();
    await yieldTurn();
    const late = await req<ReplyResult>(bob.client, { type: 'reply', messageId: m1.messageId, body: 'late' });
    expect((await pending).replies).toEqual([]);
    expect(pushesOf(alice).some((p) => p.payload.message.id === late.messageId)).toBe(true);
    expect(stack.store.getDeliveries(late.messageId).map((d) => d.state)).toEqual(['sent']);
  });

  it('hands over a held reply when the latest ask changes during the wait (timeout)', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5098);
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    const q1 = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'q1' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: q1.threadId, until: 'all_replied', timeoutMs: 300 });
    await yieldTurn();
    await yieldTurn();
    const held = await req<ReplyResult>(bob.client, { type: 'reply', messageId: q1.messageId, body: 'held' });
    await req<SendResult>(carol.client, { type: 'send', threadId: q1.threadId, to: ['bob'], body: 'q2' });
    const result = await pending;
    expect(result.timedOut).toBe(true);
    expect(result.replies.map((r) => r.id)).toEqual([held.messageId]);
    expect(stack.store.getDeliveries(held.messageId).map((d) => d.state)).toEqual(['delivered']);
  });

  it('hands over a held reply when the latest ask changes during the wait (condition met)', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5099);
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    const q1 = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'q1' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: q1.threadId, until: 'all_replied', timeoutMs: 20000 });
    await yieldTurn();
    await yieldTurn();
    const held = await req<ReplyResult>(bob.client, { type: 'reply', messageId: q1.messageId, body: 'held' });
    const q2 = await req<SendResult>(carol.client, { type: 'send', threadId: q1.threadId, to: ['bob'], body: 'q2' });
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: q2.messageId, body: 'to carol' });
    const result = await pending;
    expect(result.conditionMet).toBe(true);
    expect(result.replies.map((r) => r.id)).toEqual([held.messageId]);
    expect(stack.store.getDeliveries(held.messageId).map((d) => d.state)).toEqual(['delivered']);
  });

  it('pushes a reply to a reply during a wait instead of holding it', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5096);
    const bob = await connectPeer(stack, { handle: 'bob' });
    await connectPeer(stack, { handle: 'carol' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'q' });
    const first = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'a' });
    const pending = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 150 });
    await yieldTurn();
    await yieldTurn();
    const nested = await req<ReplyResult>(bob.client, { type: 'reply', messageId: first.messageId, body: 'b' });
    await pending;
    expect(pushesOf(alice).some((p) => p.payload.message.id === nested.messageId)).toBe(true);
    expect(stack.store.getDeliveries(nested.messageId).map((d) => d.state)).toEqual(['sent']);
  });

  it('pushes a held reply when its waiting connection closes before the wait resolves', async () => {
    const stack = await buildStack();
    const waiter = await connectPeer(stack, { handle: 'alice' });
    const adapter = await connectAdapter(stack, 'alice', 5097);
    const bob = await connectPeer(stack, { handle: 'bob' });
    await connectPeer(stack, { handle: 'carol' });
    const ask = await req<SendResult>(waiter.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'q' });
    void req<WaitResult>(waiter.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 20000 }).catch(() => {});
    await yieldTurn();
    await yieldTurn();
    const held = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'held' });
    expect(pushesOf(adapter).some((p) => p.payload.message.id === held.messageId)).toBe(false);
    waiter.client.close();
    await settle(() => pushesOf(adapter).some((p) => p.payload.message.id === held.messageId), 'held reply push');
  });

  it('times out all_replied when nobody replied', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'slow' });
    const result = await req<WaitResult>(alice.client, {
      type: 'wait',
      threadId: ask.threadId,
      until: 'all_replied',
      timeoutMs: 150,
    });
    expect(result).toMatchObject({ threadId: ask.threadId, conditionMet: false, timedOut: true });
    expect(result.matrix[0]).toMatchObject({ recipient: 'bob', state: 'queued' });
  });

  it('tells a timed-out asker who has the message but is silent and whose receipt is unproven', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    await connectPeer(stack, { handle: 'dave' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol', 'dave'], body: 'slow' });
    for (const peer of [bob, carol]) {
      await req<ReadResult>(peer.client, { type: 'read', ids: [ask.messageId] });
      await req<AckResult>(peer.client, { type: 'ack', ids: [ask.messageId] });
    }
    const result = await req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 100 });
    expect(result.note).toBe(
      'timed out: bob, carol have the message but have not replied; dave has not confirmed receipt. Do not start work that could overlap theirs: ask the user, or murmur_wait again.',
    );
  });

  it('words the note for a single silent recipient', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'slow' });
    await req<ReadResult>(bob.client, { type: 'read', ids: [ask.messageId] });
    const result = await req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 100 });
    expect(result.note).toMatch(/^timed out: bob has the message but has not replied\. Do not start/);
  });

  it('omits the have-the-message clause when every silent recipient is still queued', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    await connectPeer(stack, { handle: 'bob' });
    await connectPeer(stack, { handle: 'carol' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'slow' });
    const result = await req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 100 });
    expect(result.note).toBe(
      'timed out: bob, carol have not confirmed receipt. Do not start work that could overlap theirs: ask the user, or murmur_wait again.',
    );
  });

  it('sets no note when the wait condition is met', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'quick' });
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'ok' });
    const result = await req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 100 });
    expect(result.conditionMet).toBe(true);
    expect('note' in result).toBe(false);
  });

  it('times out on a thread without an ask and returns an empty matrix', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const result = await req<WaitResult>(alice.client, {
      type: 'wait',
      threadId: 999,
      until: 'any_replied',
      timeoutMs: 100,
    });
    expect(result).toMatchObject({ threadId: 999, conditionMet: false, timedOut: true, matrix: [] });
  });

  it('drops a waiter whose connection closes before resolution', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'leaving' });
    const pending = req<WaitResult>(alice.client, {
      type: 'wait',
      threadId: ask.threadId,
      until: 'any_replied',
      timeoutMs: 30000,
    });
    // The request will be rejected by the close below; keep it handled so
    // no unhandled-rejection window opens before the final assertion.
    pending.catch(() => {});
    await yieldTurn();
    await yieldTurn();
    alice.client.close();
    await settle(() => alice.client.closed, 'alice closed');
    await yieldTurn();
    await yieldTurn();
    // The broker must not crash on the departed waiter, and the thread keeps
    // working: bob replies normally afterwards.
    const reply = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'after close' });
    expect(reply.seq).toBe(1);
    await expect(pending).rejects.toMatchObject({ code: 'broker_closed' });
    const watcher = await connectPeer(stack, { handle: 'watch' });
    await expect(statusOf(watcher.client)).resolves.toBeDefined();
  });

  it('abandons the remaining pushes when stopped mid-fanout', async () => {
    const fake = fakeNative();
    let calls = 0;
    const gates: Array<() => void> = [];
    fake.fn = async (socketPath: string, token: string, text: string) => {
      fake.calls.push({ socketPath, token, text });
      calls += 1;
      if (calls === 2) {
        await new Promise<void>((resolve) => {
          gates.push(resolve);
        });
      }
      return { ok: true, error: null };
    };
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    for (const name of ['nbob', 'ncarol', 'ndave']) {
      const conn = await connectPeer(stack, { handle: name });
      await req(conn.client, { type: 'native-register', handle: name, socketPath: '/tmp/' + name + '.sock', token: 't' });
    }
    const pending = req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['nbob', 'ncarol', 'ndave'],
      body: 'fanout stop',
    });
    pending.catch(() => {});
    await settle(() => calls >= 2, 'second native push started');
    // A second request queued behind the hanging one on the same connection
    // is dropped when its socket is destroyed mid-chain.
    const second = req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['nbob'],
      body: 'queued behind the hang',
    });
    second.catch(() => {});
    await yieldTurn();
    await yieldTurn();
    alice.client.close();
    const stopping = stack.broker.stop();
    for (const resolve of gates) {
      resolve();
    }
    await stopping;
    await expect(pending).rejects.toMatchObject({ code: 'broker_closed' });
    // The first recipient was pushed before the stop; the rest stay queued.
    expect(fake.calls.length).toBe(2);
  });

  it('turns a raw store failure into an internal error reply', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req<GroupsResult[]>(alice.client, { type: 'groups', action: 'create', name: 'dup' });
    // The same handle twice within one add on a fresh group trips the
    // membership primary key (the dedupe set is built before the loop).
    await expect(
      req<GroupsResult[]>(alice.client, { type: 'groups', action: 'add', name: 'dup', members: ['bob', 'bob'] }),
    ).rejects.toMatchObject({
      code: 'internal',
      message: expect.stringContaining('UNIQUE constraint failed'),
    });
    // The broker keeps serving afterwards.
    await expect(statusOf(alice.client)).resolves.toBeDefined();
    expect(bob.client.closed).toBe(false);
  });

  it('validates wait fields', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    await expect(req(alice.client, { type: 'wait', threadId: 1, until: 'sometimes', timeoutMs: 10 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'until must be one of: all_replied, any_replied',
    });
    await expect(req(alice.client, { type: 'wait', threadId: 1, until: 'any_replied', timeoutMs: 'soon' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'timeoutMs must be a number',
    });
    await expect(req(alice.client, { type: 'wait', threadId: 1, until: 'any_replied', timeoutMs: -1 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'timeoutMs must not be negative',
    });
    await expect(
      req(alice.client, { type: 'wait', threadId: 1, until: 'any_replied', timeoutMs: WAIT_MAX_MS + 1 }),
    ).rejects.toMatchObject({
      code: 'bad_request',
      message: 'timeoutMs exceeds the cap of ' + String(WAIT_MAX_MS) + 'ms',
    });
    await expect(req(alice.client, { type: 'wait', threadId: 'x', until: 'any_replied', timeoutMs: 10 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'threadId must be a number',
    });
  });

  it('clears waiters and notifications on stop without leaking errors', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'stop soon',
      mode: 'notify',
    });
    expect(ask.deliveries[0]).toMatchObject({ recipient: 'bob', state: 'queued' });
    const pending = req<WaitResult>(alice.client, {
      type: 'wait',
      threadId: ask.threadId,
      until: 'any_replied',
      timeoutMs: 60000,
    });
    pending.catch(() => {});
    await yieldTurn();
    await yieldTurn();
    await stack.broker.stop();
    await expect(pending).rejects.toMatchObject({ code: 'broker_closed' });
    await settle(() => bob.client.closed, 'bob connection closed');
    // A second stop resolves via the same promise.
    await expect(stack.broker.stop()).resolves.toBeUndefined();
  });
});

describe('ordering under concurrent replies', () => {
  it('assigns seq 1..10 with no gaps or duplicates across two crossed connections', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'ten replies',
      mode: 'silent',
    });
    const bodies = Array.from({ length: 10 }, (_, i) => 'reply ' + String(i));
    const results = await Promise.all([
      ...bodies.slice(0, 5).map((body) => req<ReplyResult>(alice.client, { type: 'reply', messageId: ask.messageId, body })),
      ...bodies.slice(5).map((body) => req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body })),
    ]);
    const seqs = results.map((r) => r.seq).sort((a, b) => a - b);
    expect(seqs).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(new Set(results.map((r) => r.messageId)).size).toBe(10);
    expect(new Set(results.map((r) => r.threadId)).size).toBe(1);
    const thread = await req<ThreadResult>(alice.client, {
      type: 'thread',
      threadId: ask.threadId,
      sinceSeq: null,
      full: true,
    });
    expect(thread.messages.map((m) => m.seq)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(thread.messages.filter((m) => m.isReply)).toHaveLength(10);
  });
});

describe('notifications', () => {
  it('batches notify-mode messages into one flushed notification event', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectAdapter(stack, 'bob', 4002);
    await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob', 'alice'],
      body: 'note one',
      mode: 'notify',
    });
    await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'note two', mode: 'notify' });
    expect(notificationsOf(bob)).toHaveLength(0);
    await advanceUntil(() => notificationsOf(bob).length > 0, 'batched notification');
    const batches = notificationsOf(bob);
    expect(batches).toHaveLength(1);
    const payloads = batches[0]?.payload ?? [];
    expect(payloads).toHaveLength(2);
    expect(payloads[0]).toMatchObject({ kind: 'reply_arrived', fromHandle: 'alice' });
    expect(payloads[1]).toMatchObject({ kind: 'reply_arrived', fromHandle: 'alice' });
    // The sender is excluded from its own notify fan-out.
    expect(notificationsOf(alice)).toHaveLength(0);
  });

  it('sends nothing for silent mode even after the notify window', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const bob = await connectPeer(stack, { handle: 'bob' });
    const alice = await connectPeer(stack, { handle: 'alice' });
    await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'quiet', mode: 'silent' });
    await vi.advanceTimersByTimeAsync(NOTIFY_BATCH_MS + 5000);
    await vi.advanceTimersByTimeAsync(NOTIFY_BATCH_MS);
    expect(bob.events).toHaveLength(0);
    const inbox = await req<InboxResult>(bob.client, { type: 'inbox', unreadOnly: true });
    expect(inbox.unreadTotal).toBe(1);
  });

  it('notifies the asker when a reply arrives, and no other thread recipient', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 4002);
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectAdapter(stack, 'carol', 4003);
    const ask = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob', 'carol'],
      body: 'who agrees',
      mode: 'notify',
    });
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'me', verdict: 'agree' });
    await advanceUntil(() => notificationsOf(alice).length > 0, 'reply_arrived for alice');
    const payloads: NotificationPayload[] = notificationsOf(alice).flatMap((ev) => ev.payload);
    expect(payloads.map((p) => p.detail)).toEqual(['reply from bob in thread ' + String(ask.threadId)]);
    expect(payloads[0]).toMatchObject({ kind: 'reply_arrived', threadId: ask.threadId, fromHandle: 'bob' });
    // carol got the notify-mode ask note only, not a note for a reply that is not for her.
    expect(notificationsOf(carol).flatMap((ev) => ev.payload).map((p) => p.detail)).toEqual([
      'new message from alice in thread ' + String(ask.threadId),
    ]);
    expect(pushesOf(carol)).toHaveLength(0);
    // bob, as the replier, gets nothing for his own reply.
    expect(notificationsOf(bob)).toHaveLength(0);
  });

  it('notifies the sender when a peer goes offline with pending deliveries, deduped per thread and sender', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 4004);
    const bob = await connectPeer(stack, { handle: 'bob' });
    // Thread A: two pending messages from alice to bob (dedup to one notice).
    const p1 = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'p1', mode: 'silent' });
    await req<SendResult>(alice.client, { type: 'send', threadId: p1.threadId, to: ['bob'], body: 'p2', mode: 'silent' });
    // Thread B: a copy bob already consumed (delivered) is not pending.
    const consumed = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'consumed',
      mode: 'silent',
    });
    await req<ReadResult>(bob.client, { type: 'read', ids: [consumed.messageId] });
    // Thread C: a message bob sent himself never notifies on his own departure.
    await req<SendResult>(bob.client, { type: 'send', threadId: null, to: ['bob'], body: 'self note', mode: 'silent' });
    bob.client.close();
    await advanceUntil(() => notificationsOf(alice).length > 0, 'peer_offline for alice');
    const payloads = notificationsOf(alice).flatMap((ev) => ev.payload);
    expect(payloads).toHaveLength(1);
    expect(payloads[0]).toMatchObject({
      kind: 'peer_offline',
      threadId: p1.threadId,
      fromHandle: 'bob',
      detail: 'peer bob went offline with undelivered messages in thread ' + String(p1.threadId),
    });
    expect(payloads[0]?.threadId).not.toBe(consumed.threadId);
  });

  it('falls back to plain text over the native transport when the peer is not listening', async () => {
    const fake = fakeNative();
    fakeWaitWindow();
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    const dave = await connectPeer(stack, { handle: 'dave' });
    await req(carol.client, { type: 'native-register', handle: 'carol', socketPath: '/tmp/carol.sock', token: 'ct' });
    const ask = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['carol', 'dave'],
      body: 'notice me',
      mode: 'notify',
    });
    await req<ReplyResult>(alice.client, { type: 'reply', messageId: ask.messageId, body: 'second thought' });
    expect(fake.calls).toHaveLength(0);
    await advanceUntil(() => fake.calls.length > 0, 'native notification fallback');
    // carol has a native transport: one plain text with both notices joined.
    // dave has none: his batch is dropped silently.
    expect(fake.calls).toHaveLength(1);
    expect(fake.calls[0]?.socketPath).toBe('/tmp/carol.sock');
    expect(fake.calls[0]?.token).toBe('ct');
    expect(fake.calls[0]?.text).toBe(
      '[murmur notice] new message from alice in thread ' + String(ask.threadId) +
        '; reply from alice in thread ' + String(ask.threadId),
    );
    expect(pushesOf(carol)).toHaveLength(0);
    expect(notificationsOf(dave)).toHaveLength(0);
  });

  it('drops a notification batch when the recipient has no listener and no native transport', async () => {
    const fake = fakeNative();
    fakeWaitWindow();
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'unheard', mode: 'notify' });
    await vi.advanceTimersByTimeAsync(NOTIFY_BATCH_MS);
    await yieldTurn();
    expect(bob.events).toHaveLength(0);
    expect(fake.calls).toHaveLength(0);
    const inbox = await req<InboxResult>(bob.client, { type: 'inbox', unreadOnly: true });
    expect(inbox.unreadTotal).toBe(1);
  });

  it('swallows a rejecting native transport when flushing notifications', async () => {
    const fake = fakeNative();
    fake.mode = 'throw';
    fakeWaitWindow();
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req(bob.client, { type: 'native-register', handle: 'bob', socketPath: '/tmp/bob.sock', token: 't' });
    await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'then vanish',
      mode: 'notify',
    });
    await vi.advanceTimersByTimeAsync(NOTIFY_BATCH_MS);
    expect(fake.calls).toHaveLength(1);
    // No unhandled rejection: the broker still serves.
    await expect(statusOf(alice.client)).resolves.toBeDefined();
  });

  it('only a live connection going away triggers peer_offline', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 4005);
    const carol = await connectPeer(stack);
    await req<RegisterResult>(carol.client, { type: 'register', handle: 'carol', clientKind: 'cli' });
    const dave = await connectPeer(stack);
    await req(dave.client, { type: 'hook', event: 'prompt', handle: 'dave' });
    await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['carol', 'dave'], body: 'pending', mode: 'silent' });
    carol.client.close();
    dave.client.close();
    await settle(() => carol.client.closed && dave.client.closed, 'short connections closed');
    await yieldTurn();
    await yieldTurn();
    await vi.advanceTimersByTimeAsync(NOTIFY_BATCH_MS * 2);
    expect(notificationsOf(alice)).toHaveLength(0);
    // An mcp connection with the same pending delivery does notify.
    const erin = await connectPeer(stack, { handle: 'erin' });
    await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['erin'], body: 'pending', mode: 'silent' });
    erin.client.close();
    await advanceUntil(() => notificationsOf(alice).length > 0, 'peer_offline for alice');
    expect(notificationsOf(alice).flatMap((ev) => ev.payload)).toHaveLength(1);
  });
});

describe('dead peer and queued delivery', () => {
  it('keeps the offline peer queued, reports it via collect, then delivers with a queued_delivered notice', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 4006);
    const bobSetup = await connectPeer(stack, { handle: 'bob' });
    bobSetup.client.close();
    const watcher = await connectPeer(stack, { handle: 'watcher' });
    await settle(async () => {
      const status = await statusOf(watcher.client);
      return status.peers.find((p) => p.handle === 'bob')?.presence !== 'online';
    }, 'bob offline');
    const ask = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['alice', 'bob'],
      body: 'dead peer check',
    });
    // alice's listening copy was pushed (sent), bob's stays queued.
    expect(ask.deliveries.map((d) => d.state).sort()).toEqual(['queued', 'sent']);
    const rows = stack.store.getDeliveries(ask.messageId);
    expect(rows.find((d) => d.recipient === 'alice')).toMatchObject({ state: 'sent' });
    expect(rows.find((d) => d.recipient === 'bob')).toMatchObject({ state: 'queued' });
    const collect = await req<CollectResult>(alice.client, { type: 'collect', threadId: ask.threadId });
    expect(collect.recipients.find((r) => r.recipient === 'bob')).toMatchObject({ state: 'queued' });
    expect(collect.recipients.find((r) => r.recipient === 'alice')).toMatchObject({ state: 'sent' });
    // The second peer comes online and pulls its queue.
    const bob = await connectPeer(stack, { handle: 'bob' });
    const fetched = await req<FetchPushResult>(bob.client, { type: 'fetch-push', handle: 'bob', limit: 10 });
    expect(fetched.messages).toHaveLength(1);
    expect(fetched.messages[0]?.id).toBe(ask.messageId);
    expect(stack.store.getDeliveries(ask.messageId).find((d) => d.recipient === 'bob')).toMatchObject({
      state: 'delivered',
    });
    await advanceUntil(() => notificationsOf(alice).length > 0, 'queued_delivered for alice');
    const payloads = notificationsOf(alice).flatMap((ev) => ev.payload);
    expect(payloads).toHaveLength(1);
    expect(payloads[0]).toMatchObject({
      kind: 'queued_delivered',
      threadId: ask.threadId,
      fromHandle: 'bob',
      detail: 'queued message ' + String(ask.messageId) + ' in thread ' + String(ask.threadId) + ' was delivered to bob',
    });
  });

  it('returns nothing from a second fetch-push and validates its fields', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    // Only push-mode messages are served by fetch-push; a silent one is not.
    await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'silent one', mode: 'silent' });
    const silent = await req<FetchPushResult>(bob.client, { type: 'fetch-push', handle: 'bob', limit: 10 });
    expect(silent.messages).toHaveLength(0);
    const sent = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'once' });
    const first = await req<FetchPushResult>(bob.client, { type: 'fetch-push', handle: 'bob', limit: 10 });
    expect(first.messages).toHaveLength(1);
    expect(first.messages[0]?.id).toBe(sent.messageId);
    expect(stack.store.getDeliveries(sent.messageId)[0]?.state).toBe('delivered');
    const second = await req<FetchPushResult>(bob.client, { type: 'fetch-push', handle: 'bob', limit: 10 });
    expect(second.messages).toHaveLength(0);
    const none = await req<FetchPushResult>(bob.client, { type: 'fetch-push', handle: 'carol', limit: 5 });
    expect(none.messages).toHaveLength(0);
    await expect(req(bob.client, { type: 'fetch-push', handle: 'bob', limit: -1 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'limit must be a non-negative integer',
    });
    await expect(req(bob.client, { type: 'fetch-push', handle: 'bob', limit: 1.5 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'limit must be a non-negative integer',
    });
    await expect(req(bob.client, { type: 'fetch-push', handle: 4, limit: 1 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'handle must be a string',
    });
    await expect(req(bob.client, { type: 'fetch-push', handle: 'bob' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'limit must be a number',
    });
  });
});

describe('groups and status over the wire', () => {
  it('creates, lists, adds to, removes from and deletes groups', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const created = await req<GroupsResult[]>(alice.client, { type: 'groups', action: 'create', name: '@dev' });
    expect(created).toHaveLength(1);
    expect(created[0]).toMatchObject({ name: 'dev', members: [] });
    await expect(req(alice.client, { type: 'groups', action: 'create', name: 'dev' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'group already exists: dev',
    });
    await expect(req(alice.client, { type: 'groups', action: 'create', name: 'Bad Name!' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'invalid group name: Bad Name!',
    });
    await expect(req(alice.client, { type: 'groups', action: 'create' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'invalid group name: undefined',
    });
    const afterAdd = await req<GroupsResult[]>(alice.client, {
      type: 'groups',
      action: 'add',
      name: 'dev',
      members: ['bob', 'alice'],
    });
    expect(afterAdd).toHaveLength(1);
    expect(afterAdd[0]).toMatchObject({ name: 'dev', members: ['alice', 'bob'] });
    await expect(
      req(alice.client, { type: 'groups', action: 'add', name: 'dev', members: ['ghost'] }),
    ).rejects.toMatchObject({ code: 'unknown_handle', message: 'unknown handle: ghost' });
    await expect(
      req(alice.client, { type: 'groups', action: 'add', name: 'nodev', members: ['bob'] }),
    ).rejects.toMatchObject({
      code: 'unknown_group',
    });
    await expect(req(alice.client, { type: 'groups', action: 'add', name: 'dev' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'members required',
    });
    // Adding an existing member stays a no-op.
    const stillTwo = await req<GroupsResult[]>(alice.client, {
      type: 'groups',
      action: 'add',
      name: 'dev',
      members: ['bob'],
    });
    expect(stillTwo[0]?.members).toEqual(['alice', 'bob']);
    const afterRemove = await req<GroupsResult[]>(alice.client, {
      type: 'groups',
      action: 'remove',
      name: 'dev',
      members: ['bob'],
    });
    expect(afterRemove[0]?.members).toEqual(['alice']);
    // Removing a non-member (bob again) is a no-op; an unknown handle is not.
    const removedAgain = await req<GroupsResult[]>(alice.client, {
      type: 'groups',
      action: 'remove',
      name: 'dev',
      members: ['bob'],
    });
    expect(removedAgain[0]?.members).toEqual(['alice']);
    await expect(
      req(alice.client, { type: 'groups', action: 'remove', name: 'dev', members: ['stranger'] }),
    ).rejects.toMatchObject({ code: 'unknown_handle', message: 'unknown handle: stranger' });
    const listed = await req<GroupsResult[]>(alice.client, { type: 'groups', action: 'list' });
    expect(listed).toEqual(afterRemove);
    const afterDelete = await req<GroupsResult[]>(alice.client, { type: 'groups', action: 'delete', name: '@dev' });
    expect(afterDelete).toEqual([]);
    await expect(req(alice.client, { type: 'groups', action: 'delete', name: 'dev' })).rejects.toMatchObject({
      code: 'unknown_group',
    });
    await expect(req(alice.client, { type: 'groups', action: 'rename' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'action must be one of: create, list, add, remove, delete',
    });
    await expect(req(alice.client, { type: 'groups', action: 'list', name: 4 })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'name must be a string',
    });
    await expect(req(alice.client, { type: 'groups', action: 'list', name: 'dev', members: 'alice' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'members must be an array of strings',
    });
    expect(bob.client.closed).toBe(false);
  });

  it('reports status presence including hook busy and idle states', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    // hooky registers via a hook event, then leaves: busy wins over recency.
    const hookConn = await connectPeer(stack);
    await req(hookConn.client, { type: 'hook', event: 'prompt', handle: 'hooky', sessionHint: 'sess-9' });
    hookConn.client.close();
    const gone = await connectPeer(stack, { handle: 'gone' });
    gone.client.close();
    const watcher = await connectPeer(stack, { handle: 'watch' });
    await settle(async () => {
      const status = await statusOf(watcher.client);
      return (
        status.peers.find((p) => p.handle === 'hooky')?.presence === 'busy' &&
        status.peers.find((p) => p.handle === 'gone')?.presence !== 'online'
      );
    }, 'hooky busy and gone offline');
    let status = await statusOf(watcher.client);
    let hooky = status.peers.find((p) => p.handle === 'hooky');
    expect(hooky?.presence).toBe('busy');
    expect(hooky?.busySince).not.toBeNull();
    expect(hooky?.idleSince).toBeNull();
    const gonePeer = status.peers.find((p) => p.handle === 'gone');
    expect(gonePeer?.presence).toBe('presumed-offline');
    expect(gonePeer?.busySince).toBeNull();
    expect(status.peers.find((p) => p.handle === 'alice')?.presence).toBe('online');
    expect(status.peers.find((p) => p.handle === 'alice')?.unread).toBe(0);
    expect(alice.client.closed).toBe(false);
    // A stop event flips hooky to idle after the hook connection leaves.
    const stopConn = await connectPeer(stack);
    await req(stopConn.client, { type: 'hook', event: 'stop', handle: 'hooky' });
    stopConn.client.close();
    await settle(async () => {
      const next = await statusOf(watcher.client);
      return next.peers.find((p) => p.handle === 'hooky')?.presence === 'idle';
    }, 'hooky idle');
    status = await statusOf(watcher.client);
    hooky = status.peers.find((p) => p.handle === 'hooky');
    expect(hooky?.presence).toBe('idle');
    expect(hooky?.idleSince).not.toBeNull();
    // Validation errors on a still-live connection.
    const validator = await connectPeer(stack);
    await expect(req(validator.client, { type: 'hook', event: 'crash', handle: 'hooky' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'event must be one of: session-start, prompt, stop, session-end',
    });
    await expect(req(validator.client, { type: 'hook', event: 'stop' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'handle must be a string',
    });
    await expect(
      req(validator.client, { type: 'hook', event: 'stop', handle: 'hooky', sessionHint: 5 }),
    ).rejects.toMatchObject({
      code: 'bad_request',
      message: 'sessionHint must be a string',
    });
  });

  it('sends to a group and dedups overlapping recipients', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req<GroupsResult[]>(alice.client, { type: 'groups', action: 'create', name: 'team' });
    await req<GroupsResult[]>(alice.client, {
      type: 'groups',
      action: 'add',
      name: 'team',
      members: ['bob', 'alice'],
    });
    const result = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['@team', 'bob'],
      body: 'group ping',
      mode: 'silent',
    });
    expect(result.deliveries.map((d) => d.recipient).sort()).toEqual(['alice', 'bob']);
    expect(result.deliveries).toHaveLength(2);
    expect(pushesOf(bob)).toHaveLength(0);
  });
});

describe('broker lifecycle', () => {
  it('refuses a second broker on the same socket path', async () => {
    const stack = await buildStack();
    await expect(Broker.start({ paths: stack.paths, store: stack.store, core: stack.core })).rejects.toThrow();
  });

  it('destroys live connections on stop', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    await stack.broker.stop();
    await settle(() => alice.client.closed, 'client closed after broker stop');
  });
});

// Real-time sleep for tests that must wait out wall-clock timers.
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

// Replaces core.collect with one that throws a non-error, the only way the
// broker's non-murmur fallbacks (which wrap every handler and both reply
// paths) can be driven. Restored by deleting the own property, which
// un-shadows the prototype method.
function breakCollect(core: MurmurCore): void {
  (core as unknown as { collect: () => never }).collect = () => {
    throw 'collect exploded';
  };
}

function restoreCollect(core: MurmurCore): void {
  delete (core as unknown as { collect?: unknown }).collect;
}

describe('auth fault tolerance', () => {
  it('answers a first line whose type is not a string with auth_required', async () => {
    const stack = await buildStack();
    const raw = await rawConnect(stack.paths.socketPath);
    raws.push(raw);
    raw.socket.write(encodeLine({ v: 1, id: 9 }));
    const line = await raw.nextLine();
    const reply = JSON.parse(line) as { v: number; id: number; ok: boolean; error: { code: string; message: string } };
    expect(reply.ok).toBe(false);
    expect(reply.id).toBe(9);
    expect(reply.error.code).toBe('auth_required');
    await raw.closed;
  });

  it('answers auth with an internal error when the token store throws a non-error', async () => {
    const stack = await buildStack();
    configState.tokenFailure = 'token vault sealed';
    const raw = await rawConnect(stack.paths.socketPath);
    raws.push(raw);
    raw.socket.write(encodeLine({ v: 1, id: 4, type: 'auth', token: 'whatever' }));
    const line = await raw.nextLine();
    const reply = JSON.parse(line) as { ok: boolean; error: { code: string; message: string } };
    expect(reply.ok).toBe(false);
    expect(reply.error.code).toBe('internal');
    expect(reply.error.message).toBe('token vault sealed');
    await raw.closed;
  });
});

describe('non-error faults from core.collect', () => {
  it('fails the releasing reply with internal and the string text', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'ping' });
    // The wait stays pending until the reply lands; only its planting matters.
    const pending = req<WaitResult>(bob.client, {
      type: 'wait',
      threadId: ask.threadId,
      until: 'any_replied',
      timeoutMs: 20000,
    });
    pending.catch(() => {});
    await yieldTurn();
    await yieldTurn();
    breakCollect(stack.core);
    try {
      await expect(
        req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'pong' }),
      ).rejects.toMatchObject({ code: 'internal', message: 'collect exploded' });
    } finally {
      restoreCollect(stack.core);
    }
  });

  it('leaves a timed-out wait unanswered instead of crashing the broker', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'silent' });
    breakCollect(stack.core);
    let settled = false;
    const pending = req<WaitResult>(bob.client, { type: 'wait', threadId: 1, until: 'any_replied', timeoutMs: 40 });
    pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    try {
      await sleep(300);
      expect(settled).toBe(false);
      restoreCollect(stack.core);
      const status = await req<StatusResult>(bob.client, { type: 'status' });
      expect(status.peers.length).toBeGreaterThan(0);
    } finally {
      restoreCollect(stack.core);
    }
  });
});

describe('native push fault tolerance', () => {
  it('skips a native push whose transport row vanished mid-flight', async () => {
    let release!: (result: NativeSendResult) => void;
    const gate = new Promise<NativeSendResult>((resolve) => {
      release = resolve;
    });
    const calls: Array<{ socketPath: string; token: string; text: string }> = [];
    const stub: NativeSendFn = async (socketPath: string, token: string, text: string) => {
      calls.push({ socketPath, token, text });
      return gate;
    };
    const stack = await buildStack(stub);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    await req(bob.client, { type: 'native-register', handle: 'bob', socketPath: '/tmp/fake-bob.sock', token: 'tb' });
    await req(carol.client, { type: 'native-register', handle: 'carol', socketPath: '/tmp/fake-carol.sock', token: 'tc' });
    const pending = req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob', 'carol'],
      body: 'vanish test',
    });
    await settle(() => calls.length === 1, 'first native push started');
    stack.store.deleteNativeTransport('carol');
    release({ ok: true, error: null });
    const result = await pending;
    expect(calls).toHaveLength(1);
    const rows = stack.store.getDeliveries(result.messageId);
    expect(rows.find((d) => d.recipient === 'bob')?.state).toBe('sent');
    expect(rows.find((d) => d.recipient === 'carol')?.state).toBe('queued');
  });
});

describe('wait condition edge', () => {
  it('never holds for an ask that has no delivery rows', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'seed thread' });
    const orphan = stack.store.appendMessage({
      threadId: ask.threadId,
      sender: 'alice',
      body: 'orphan ask',
      deliveredBody: 'orphan ask',
      truncated: false,
      refs: [],
      mode: 'push',
      isReply: false,
      inReplyTo: null,
      verdict: null,
      createdAt: Date.now(),
    });
    expect(stack.store.latestAsk(ask.threadId)?.id).toBe(orphan.id);
    expect(stack.store.getDeliveries(orphan.id)).toHaveLength(0);
    const inner = stack.broker as unknown as { conditionHolds(threadId: number, until: string): boolean };
    expect(inner.conditionHolds(ask.threadId, 'any_replied')).toBe(false);
    expect(inner.conditionHolds(ask.threadId, 'all_replied')).toBe(false);
  });
});

describe('write failures on a peer socket', () => {
  it('keeps the send alive and marks the delivery sent when the peer write throws', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectAdapter(stack, 'bob', 4007);
    const inner = stack.broker as unknown as { presence: Map<string, { socket: Socket }> };
    const bobSocket = inner.presence.get('bob')?.socket;
    expect(bobSocket).toBeDefined();
    const originalWrite = bobSocket!.write.bind(bobSocket);
    bobSocket!.write = (() => {
      throw new Error('kaboom');
    }) as Socket['write'];
    try {
      const result = await req<SendResult>(alice.client, {
        type: 'send',
        threadId: null,
        to: ['bob'],
        body: 'write explodes',
      });
      const row = stack.store.getDeliveries(result.messageId).find((d) => d.recipient === 'bob');
      expect(row?.state).toBe('sent');
    } finally {
      bobSocket!.write = originalWrite as Socket['write'];
    }
    expect(pushesOf(bob)).toHaveLength(0);
  });
});

describe('startup window guard', () => {
  it('destroys a connection that arrives before the broker instance exists', async () => {
    let observed = false;
    const homes: string[] = [];
    try {
      for (let attempt = 0; attempt < 40 && !observed; attempt += 1) {
        const home = mkdtempSync(join(tmpdir(), 'murmur-startup-'));
        homes.push(home);
        const paths = getPaths(home);
        ensureHome(paths);
        const store = Store.open(paths.dbPath);
        const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} });
        const starting = Broker.start({ paths, store, core });
        await settle(() => existsSync(paths.socketPath), 'startup socket');
        let sawData = '';
        const probe = createConnection(paths.socketPath);
        probe.setEncoding('utf8');
        probe.on('data', (chunk: string) => {
          sawData += chunk;
        });
        const gone = new Promise<void>((resolve) => {
          probe.once('close', () => resolve());
          probe.once('error', () => resolve());
        });
        const broker = await starting;
        let closedEarly = false;
        for (let turn = 0; turn < 5 && !closedEarly; turn += 1) {
          closedEarly = await Promise.race([gone.then(() => true), sleep(2).then(() => false)]);
        }
        probe.destroy();
        if (closedEarly && sawData.length === 0) {
          observed = true;
        }
        await broker.stop();
        store.close();
      }
    } finally {
      for (const home of homes) {
        rmSync(home, { recursive: true, force: true });
      }
    }
    expect(observed).toBe(true);
  });
});

// The remaining guards run on races between shutdown and in-flight work that
// a single-threaded test process cannot schedule; each test replays the exact
// state such a race would leave behind and checks the guard holds it.
describe('shutdown and straggler guards', () => {
  it('checkWaiters stops scanning once the broker has stopped', async () => {
    const stack = await buildStack();
    await stack.broker.stop();
    const inner = stack.broker as unknown as {
      waiters: Map<number, Set<Record<string, unknown>>>;
      checkWaiters(threadId: number): void;
    };
    const writes: string[] = [];
    const fakeConn = {
      socket: { destroyed: true, write: (line: string) => writes.push(line) },
      waiters: new Set<unknown>(),
    };
    const fakeWaiter = { conn: fakeConn, id: 13, threadId: 2, until: 'all_replied', timer: undefined, held: new Set<number>() };
    const planted = new Set([fakeWaiter]);
    inner.waiters.set(2, planted);
    inner.checkWaiters(2);
    expect(writes).toEqual([]);
    expect(planted.has(fakeWaiter)).toBe(true);
  });

  it('resolveTimedOut stays silent once the broker has stopped', async () => {
    const stack = await buildStack();
    await stack.broker.stop();
    const inner = stack.broker as unknown as {
      waiters: Map<number, Set<Record<string, unknown>>>;
      resolveTimedOut(waiter: Record<string, unknown>): void;
    };
    const writes: string[] = [];
    const fakeConn = {
      socket: { destroyed: true, write: (line: string) => writes.push(line) },
      waiters: new Set<unknown>(),
    };
    const fakeWaiter = { conn: fakeConn, id: 12, threadId: 1, until: 'any_replied', timer: undefined };
    fakeConn.waiters.add(fakeWaiter);
    const planted = new Set([fakeWaiter]);
    inner.waiters.set(1, planted);
    inner.resolveTimedOut(fakeWaiter);
    expect(writes).toEqual([]);
    expect(planted.has(fakeWaiter)).toBe(false);
    expect(inner.waiters.has(1)).toBe(false);
  });

  it('enqueueNotification after stop keeps the queue empty', async () => {
    const stack = await buildStack();
    await stack.broker.stop();
    const inner = stack.broker as unknown as {
      notifyQueue: Map<string, unknown>;
      enqueueNotification(recipient: string, payload: unknown): void;
    };
    inner.enqueueNotification('alice', { kind: 'peer_offline', threadId: 1, fromHandle: 'bob', detail: 'straggler' });
    expect(inner.notifyQueue.size).toBe(0);
  });

  it('flushNotifications ignores an unknown recipient', async () => {
    const stack = await buildStack();
    const inner = stack.broker as unknown as {
      notifyQueue: Map<string, unknown>;
      flushNotifications(recipient: string): void;
    };
    expect(() => inner.flushNotifications('ghost')).not.toThrow();
    expect(inner.notifyQueue.size).toBe(0);
  });

  it('flushNotifications drops an entry that holds no payloads', async () => {
    const stack = await buildStack();
    const inner = stack.broker as unknown as {
      notifyQueue: Map<string, { payloads: unknown[]; timer: NodeJS.Timeout }>;
      flushNotifications(recipient: string): void;
    };
    inner.notifyQueue.set('bob', { payloads: [], timer: setTimeout(() => {}, 60_000) });
    inner.flushNotifications('bob');
    expect(inner.notifyQueue.size).toBe(0);
  });
});

// v0.2: session identity, adapters, transports and delivery proof.

async function announce(
  stack: Stack,
  peer: Peer,
  pid: number,
  extra: Record<string, unknown> = {},
): Promise<SessionResult> {
  return req<SessionResult>(peer.client, {
    type: 'session',
    event: 'session-start',
    client: 'claude',
    cliSessionId: 'cli-' + String(pid),
    host: hostRef(stack, pid),
    ...extra,
  });
}

function senderOf(stack: Stack, threadId: number): string[] {
  return stack.store.threadMessages(threadId, null).map((m) => m.sender);
}

function addOwner(stack: Stack, handle = 'boss'): void {
  stack.store.upsertPeer(handle, null, null, true, Date.now());
}

describe('register modes', () => {
  it('owner binds the owner handle and errors when no owner exists', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack);
    await expect(req(conn.client, { type: 'register', clientKind: 'cli', owner: true })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'no owner peer; run murmur init',
    });
    addOwner(stack);
    await expect(req<RegisterResult>(conn.client, { type: 'register', clientKind: 'cli', owner: true })).resolves.toEqual({
      handle: 'boss',
      owner: true,
    });
    // The cli connection speaks as the owner without taking presence.
    const sent = await req<SendResult>(conn.client, { type: 'send', threadId: null, to: ['boss'], body: 'note' });
    expect(senderOf(stack, sent.threadId)).toEqual(['boss']);
    const other = await connectPeer(stack, { handle: 'watch' });
    expect((await statusOf(other.client)).peers.find((p) => p.handle === 'boss')?.presence).not.toBe('online');
  });

  it('applies display and role after resolving, each on its own', async () => {
    const stack = await buildStack();
    addOwner(stack);
    const conn = await connectPeer(stack);
    await req(conn.client, { type: 'register', clientKind: 'cli', owner: true, display: 'The Boss' });
    expect(stack.store.getPeer('boss')).toMatchObject({ display: 'The Boss', role: null, isOwner: true });
    await req(conn.client, { type: 'register', clientKind: 'cli', owner: true, role: 'lead' });
    expect(stack.store.getPeer('boss')).toMatchObject({ display: 'The Boss', role: 'lead', isOwner: true });
    // A handle that has no row yet gets one with the display and role.
    await req(conn.client, { type: 'register', clientKind: 'mcp', handle: 'fresh', display: 'Fresh', role: 'qa' });
    expect(stack.store.getPeer('fresh')).toMatchObject({ display: 'Fresh', role: 'qa', isOwner: false });
  });

  it('binds a host that already announced its session and renames it on request', async () => {
    const stack = await buildStack();
    const hook = await connectPeer(stack);
    await announce(stack, hook, 5001, { handle: 'sess-a' });
    const mcp = await connectPeer(stack);
    const same = await req<RegisterResult>(mcp.client, {
      type: 'register',
      clientKind: 'mcp',
      host: { hostPid: 5001, hostStart: HOST_START },
      client: 'claude',
      display: 'Sess A',
    });
    expect(same).toEqual({ handle: 'sess-a', owner: false });
    expect(stack.store.getPeer('sess-a')?.display).toBe('Sess A');
    const status = await statusOf(hook.client);
    expect(status.peers.find((p) => p.handle === 'sess-a')?.presence).toBe('idle');
    const renamed = await req<RegisterResult>(mcp.client, {
      type: 'register',
      clientKind: 'mcp',
      host: { hostPid: 5001, hostStart: HOST_START },
      handle: 'renamed-a',
    });
    expect(renamed.handle).toBe('renamed-a');
    expect(stack.store.getPeer('sess-a')).toBeNull();
    expect(stack.store.getPeer('renamed-a')).not.toBeNull();
    await expect(
      req(mcp.client, { type: 'register', clientKind: 'mcp', host: { hostPid: 5001, hostStart: HOST_START }, handle: 'Bad Handle' }),
    ).rejects.toMatchObject({ code: 'bad_request', message: 'invalid handle: Bad Handle' });
  });

  it('keeps a host pending until its session event arrives, then binds the waiting connection', async () => {
    const stack = await buildStack();
    const mcp = await connectPeer(stack);
    const host = hostRef(stack, 5002);
    await expect(
      req<RegisterResult>(mcp.client, { type: 'register', clientKind: 'mcp', host, client: 'claude' }),
    ).resolves.toEqual({ handle: '', owner: false });
    // A second pending connection on a different host stays pending.
    const other = await connectPeer(stack);
    await req(other.client, { type: 'register', clientKind: 'mcp', host: { hostPid: 5002, hostStart: 'a later start' }, client: 'claude' });
    const hook = await connectPeer(stack);
    await announce(stack, hook, 5002, { handle: 'late-bound' });
    const sent = await req<SendResult>(mcp.client, { type: 'send', threadId: null, to: ['late-bound'], body: 'hello' });
    expect(senderOf(stack, sent.threadId)).toEqual(['late-bound']);
    // The bound mcp connection is live: it holds the presence slot.
    const status = await statusOf(hook.client);
    expect(status.peers.find((p) => p.handle === 'late-bound')?.presence).toBe('idle');
    // The other host still has no identity; first use creates it.
    const created = await req<SendResult>(other.client, { type: 'send', threadId: null, to: ['late-bound'], body: 'hi' });
    expect(senderOf(stack, created.threadId)).toEqual(['claude']);
  });

  it('creates the session on first identity use when no hook ever announced the host', async () => {
    const stack = await buildStack();
    const mcp = await connectPeer(stack);
    await req(mcp.client, {
      type: 'register',
      clientKind: 'mcp',
      host: hostRef(stack, 5003),
      client: 'codex',
      cwd: '/work/My Proj',
    });
    const bare = await connectPeer(stack);
    await req(bare.client, { type: 'register', clientKind: 'adapter', host: hostRef(stack, 5004) });
    const sent = await req<SendResult>(mcp.client, { type: 'send', threadId: null, to: ['codex-my-proj'], body: 'self' });
    expect(senderOf(stack, sent.threadId)).toEqual(['codex-my-proj']);
    expect(stack.store.sessionsForHandle('codex-my-proj')[0]).toMatchObject({ client: 'codex', cwd: '/work/My Proj' });
    const sentBare = await req<SendResult>(bare.client, { type: 'send', threadId: null, to: ['other'], body: 'bare' });
    expect(senderOf(stack, sentBare.threadId)).toEqual(['other']);
    // Once resolved the connection keeps its handle.
    const again = await req<SendResult>(mcp.client, { type: 'send', threadId: null, to: ['other'], body: 'again' });
    expect(senderOf(stack, again.threadId)).toEqual(['codex-my-proj']);
  });

  it('resolves ancestors to the nearest live session, else the owner', async () => {
    const stack = await buildStack();
    const hook = await connectPeer(stack);
    await announce(stack, hook, 5010, { handle: 'anc-a' });
    const cli = await connectPeer(stack);
    await expect(
      req<RegisterResult>(cli.client, { type: 'register', clientKind: 'cli', ancestors: [1, 5010, 77] }),
    ).resolves.toEqual({ handle: 'anc-a', owner: false });
    // No owner and no matching session: nothing to speak as.
    await expect(req(cli.client, { type: 'register', clientKind: 'cli', ancestors: [99999] })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'no owner peer; run murmur init',
    });
    addOwner(stack);
    await expect(req<RegisterResult>(cli.client, { type: 'register', clientKind: 'cli', ancestors: [99999] })).resolves.toEqual({
      handle: 'boss',
      owner: true,
    });
    // A session whose host is gone from the process table does not match.
    stack.proc.delete(5010);
    await expect(req<RegisterResult>(cli.client, { type: 'register', clientKind: 'cli', ancestors: [5010] })).resolves.toEqual({
      handle: 'boss',
      owner: true,
    });
    // An explicit handle beats ancestors.
    await expect(
      req<RegisterResult>(cli.client, { type: 'register', clientKind: 'cli', ancestors: [5010], handle: 'explicit-x' }),
    ).resolves.toEqual({ handle: 'explicit-x', owner: false });
  });

  it('registers an explicit handle, and refuses a register that names nothing', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack);
    await expect(req<RegisterResult>(conn.client, { type: 'register', clientKind: 'adapter', handle: 'adp' })).resolves.toEqual({
      handle: 'adp',
      owner: false,
    });
    const watcher = await connectPeer(stack, { handle: 'watch' });
    // adapter connections are long lived, so they hold presence.
    expect((await statusOf(watcher.client)).peers.find((p) => p.handle === 'adp')?.presence).toBe('online');
    await expect(req(conn.client, { type: 'register', clientKind: 'mcp' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'register needs a handle, a host, ancestors or owner',
    });
  });

  it('validates the v0.2 register fields', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack);
    const bad = (extra: Record<string, unknown>, message: string): Promise<void> =>
      expect(req(conn.client, { type: 'register', clientKind: 'mcp', handle: 'x', ...extra })).rejects.toMatchObject({
        code: 'bad_request',
        message,
      });
    await bad({ host: 5 }, 'host must be an object');
    await bad({ host: null }, 'host must be an object');
    await bad({ host: { hostPid: 'x', hostStart: 's' } }, 'hostPid must be a number');
    await bad({ host: { hostPid: 1, hostStart: 2 } }, 'hostStart must be a string');
    await bad({ client: 'nope' }, 'client must be one of: claude, codex, gemini, zcode, opencode, hummin, other');
    await bad({ owner: 'yes' }, 'owner must be a boolean');
    await bad({ cwd: 4 }, 'cwd must be a string');
    await bad({ ancestors: 'a' }, 'ancestors must be an array of numbers');
    await bad({ ancestors: ['a'] }, 'ancestors must be an array of numbers');
  });
});

describe('session handler', () => {
  it('walks every event and keeps native transports in step', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack);
    const host = hostRef(stack, 5020);
    const native = { socketPath: '/tmp/s-native.sock', token: 'nt' };
    const base = { type: 'session', client: 'gemini', cliSessionId: 'g-1', host };
    const start = await req<SessionResult>(conn.client, {
      ...base,
      event: 'session-start',
      native,
      transcriptPath: '/tmp/none.jsonl',
      cwd: '/home/dev/api',
    });
    expect(start).toEqual({ handle: 'gemini-api' });
    expect(stack.store.getNativeTransport('gemini-api')).toMatchObject(native);
    const state = (): string | undefined => stack.store.sessionsForHandle('gemini-api')[0]?.state;
    expect(state()).toBe('idle');
    await req(conn.client, { ...base, event: 'prompt' });
    expect(state()).toBe('busy');
    await req(conn.client, { ...base, event: 'stop' });
    expect(state()).toBe('idle');
    // cliSessionId null is allowed: the host finds the session.
    await expect(req(conn.client, { ...base, cliSessionId: null, event: 'prompt' })).resolves.toEqual({ handle: 'gemini-api' });
    await expect(req(conn.client, { ...base, event: 'session-end' })).resolves.toEqual({ handle: 'gemini-api' });
    expect(state()).toBe('ended');
    expect(stack.store.getNativeTransport('gemini-api')).toBeNull();
  });

  it('validates session fields', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack);
    const host = hostRef(stack, 5021);
    const ok = { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 's', host };
    const bad = (extra: Record<string, unknown>, message: string): Promise<void> =>
      expect(req(conn.client, { ...ok, ...extra })).rejects.toMatchObject({ code: 'bad_request', message });
    await bad({ event: 'nap' }, 'event must be one of: session-start, prompt, stop, session-end');
    await bad({ client: 'nope' }, 'client must be one of: claude, codex, gemini, zcode, opencode, hummin, other');
    await bad({ cliSessionId: 4 }, 'cliSessionId must be a string');
    await bad({ cliSessionId: undefined }, 'cliSessionId must be a string');
    await bad({ host: 'h' }, 'host must be an object');
    await bad({ transcriptPath: 4 }, 'transcriptPath must be a string');
    await bad({ cwd: 4 }, 'cwd must be a string');
    await bad({ handle: 4 }, 'handle must be a string');
    await bad({ listen: 'yes' }, 'listen must be a boolean');
    await bad({ native: 'x' }, 'native must be an object');
    await bad({ native: null }, 'native must be an object');
    await bad({ native: { socketPath: 1, token: 't' } }, 'socketPath must be a string');
    await bad({ native: { socketPath: 's' } }, 'token must be a string');
  });

  it('pushes queued messages oldest first when a session starts listening', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    const bobSetup = await connectPeer(stack, { handle: 'bob' });
    bobSetup.client.close();
    await settle(() => bobSetup.client.closed, 'bob gone');
    const m1 = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'first' });
    const m2 = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'second' });
    await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'quiet', mode: 'silent' });
    expect(stack.store.getDeliveries(m1.messageId).find((d) => d.recipient === 'bob')?.state).toBe('queued');
    const bob = await connectAdapter(stack, 'bob', 5030);
    expect(pushesOf(bob).map((p) => p.payload.message.id)).toEqual([m1.messageId, m2.messageId]);
    expect(pushesOf(bob)[0]?.payload.text).toBe(renderDelivery(pushesOf(bob)[0]!.payload.message, 'first', ['bob', 'carol']));
    const states = (id: number): string | undefined => stack.store.getDeliveries(id).find((d) => d.recipient === 'bob')?.state;
    expect(states(m1.messageId)).toBe('sent');
    expect(states(m2.messageId)).toBe('sent');
    // carol's copy of the first message is untouched, silent mail is never pushed.
    expect(stack.store.getDeliveries(m1.messageId).find((d) => d.recipient === 'carol')?.state).toBe('queued');
    expect(pushesOf(bob)).toHaveLength(2);
    expect(carol.events).toHaveLength(0);
  });

  it('pushes queued messages on a listening prompt but not on a plain stop', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const first = await connectPeer(stack);
    await announce(stack, first, 5031, { handle: 'bob' });
    const quiet = await connectPeer(stack);
    const queued = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'waiting' });
    // Not listening anywhere: still queued after a stop event.
    await req(quiet.client, {
      type: 'session',
      event: 'stop',
      client: 'claude',
      cliSessionId: 'cli-5031',
      host: hostRef(stack, 5031),
    });
    const stateOf = (): string | undefined => stack.store.getDeliveries(queued.messageId)[0]?.state;
    expect(stateOf()).toBe('queued');
    const listener = await connectPeer(stack);
    await req(listener.client, {
      type: 'session',
      event: 'prompt',
      client: 'claude',
      cliSessionId: 'cli-5031',
      host: hostRef(stack, 5031),
      listen: true,
    });
    expect(pushesOf(listener).map((p) => p.payload.message.id)).toEqual([queued.messageId]);
    expect(stateOf()).toBe('sent');
  });

  it('a session start with a native transport pushes the queued mail through it', async () => {
    const fake = fakeNative();
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bobSetup = await connectPeer(stack, { handle: 'bob' });
    bobSetup.client.close();
    await settle(() => bobSetup.client.closed, 'bob gone');
    const queued = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'for native' });
    expect(fake.calls).toHaveLength(0);
    const hook = await connectPeer(stack);
    await announce(stack, hook, 5032, { handle: 'bob', native: { socketPath: '/tmp/bob-n.sock', token: 'bn' } });
    expect(fake.calls).toHaveLength(1);
    expect(fake.calls[0]).toMatchObject({ socketPath: '/tmp/bob-n.sock', token: 'bn' });
    expect(fake.calls[0]?.text).toContain(deliveryTag(queued.messageId));
    expect(stack.store.getDeliveries(queued.messageId)[0]?.state).toBe('sent');
  });

  it('tells the senders a session went offline when it ends with unconsumed messages', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5040);
    const bob = await connectAdapter(stack, 'bob', 5041);
    const sent = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'unread' });
    expect(pushesOf(bob)).toHaveLength(1);
    const end = await req<SessionResult>(bob.client, {
      type: 'session',
      event: 'session-end',
      client: 'claude',
      cliSessionId: 'cli-bob',
      host: hostRef(stack, 5041),
      listen: true,
    });
    expect(end).toEqual({ handle: 'bob' });
    await advanceUntil(() => notificationsOf(alice).length > 0, 'peer_offline for alice');
    expect(notificationsOf(alice).flatMap((ev) => ev.payload)).toEqual([
      {
        kind: 'peer_offline',
        threadId: sent.threadId,
        fromHandle: 'bob',
        detail: 'peer bob went offline with undelivered messages in thread ' + String(sent.threadId),
      },
    ]);
  });
});

describe('delivered acks', () => {
  it('moves queued and sent deliveries to delivered and ignores everything else', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5050);
    const bob = await connectAdapter(stack, 'bob', 5051);
    const sentMsg = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'pushed' });
    const queuedMsg = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'queued',
      mode: 'silent',
    });
    const notBob = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['alice'], body: 'mine' });
    const result = await req<{ delivered: number[] }>(bob.client, {
      type: 'delivered',
      ids: [sentMsg.messageId, queuedMsg.messageId, sentMsg.messageId, notBob.messageId, 987654],
    });
    expect(result.delivered).toEqual([sentMsg.messageId, queuedMsg.messageId]);
    const stateOf = (id: number): string | undefined => stack.store.getDeliveries(id).find((d) => d.recipient === 'bob')?.state;
    expect(stateOf(sentMsg.messageId)).toBe('delivered');
    expect(stateOf(queuedMsg.messageId)).toBe('delivered');
    // Only the delivery that had been waiting tells the sender.
    await advanceUntil(() => notificationsOf(alice).length > 0, 'queued_delivered for alice');
    const payloads = notificationsOf(alice).flatMap((ev) => ev.payload);
    expect(payloads).toHaveLength(1);
    expect(payloads[0]).toMatchObject({ kind: 'queued_delivered', fromHandle: 'bob', threadId: queuedMsg.threadId });
    await expect(req(bob.client, { type: 'delivered', ids: 'x' })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'ids must be an array of numbers',
    });
    const anon = await connectPeer(stack);
    await expect(req(anon.client, { type: 'delivered', ids: [] })).rejects.toMatchObject({
      code: 'bad_request',
      message: 'no handle is registered on this connection',
    });
  });
});

describe('push routing', () => {
  it('native ok marks sent, then transcript proof marks delivered', async () => {
    const dir = mkdtempSync(join('/tmp', 'mm-'));
    const transcript = join(dir, 'bob.jsonl');
    writeFileSync(transcript, 'earlier line\n');
    try {
      const fake = fakeNative();
      const stack = await buildStack(fake.fn, { proofPollMs: 20 });
      const alice = await connectPeer(stack, { handle: 'alice' });
      const hook = await connectPeer(stack);
      await announce(stack, hook, 5060, {
        handle: 'bob',
        transcriptPath: transcript,
        native: { socketPath: '/tmp/bob-p.sock', token: 'bp' },
      });
      const sent = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'prove me' });
      expect(fake.calls).toHaveLength(1);
      expect(sent.deliveries[0]?.state).toBe('sent');
      await sleep(80);
      expect(stack.store.getDeliveries(sent.messageId)[0]?.state).toBe('sent');
      // The session writes the message into its transcript: delivery proven.
      appendFileSync(transcript, (fake.calls[0]?.text ?? '') + '\n');
      await settle(() => stack.store.getDeliveries(sent.messageId)[0]?.state === 'delivered', 'proof delivered');
      expect(stack.store.getDeliveries(sent.messageId)[0]?.deliveredAt).not.toBeNull();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('a transcript written during the push is still found (offset taken before the push)', async () => {
    const dir = mkdtempSync(join('/tmp', 'mm-'));
    const transcript = join(dir, 'bob.jsonl');
    writeFileSync(transcript, 'earlier line\n');
    try {
      const stack = await buildStack(
        async (_socketPath, _token, text) => {
          appendFileSync(transcript, text + '\n');
          return { ok: true, error: null };
        },
        { proofPollMs: 20 },
      );
      const alice = await connectPeer(stack, { handle: 'alice' });
      const hook = await connectPeer(stack);
      await announce(stack, hook, 5061, {
        handle: 'bob',
        transcriptPath: transcript,
        native: { socketPath: '/tmp/bob-q.sock', token: 'bq' },
      });
      const sent = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'fast session' });
      await settle(() => stack.store.getDeliveries(sent.messageId)[0]?.state === 'delivered', 'proof delivered');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('a session event rechecks the transcript past the polling window', async () => {
    const dir = mkdtempSync(join('/tmp', 'mm-'));
    const transcript = join(dir, 'bob.jsonl');
    writeFileSync(transcript, '');
    try {
      const fake = fakeNative();
      // The poll interval is longer than the test: only recheck can find it.
      const stack = await buildStack(fake.fn, { proofPollMs: 3_600_000 });
      const alice = await connectPeer(stack, { handle: 'alice' });
      const hook = await connectPeer(stack);
      await announce(stack, hook, 5062, {
        handle: 'bob',
        transcriptPath: transcript,
        native: { socketPath: '/tmp/bob-r.sock', token: 'br' },
      });
      const sent = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'later' });
      appendFileSync(transcript, (fake.calls[0]?.text ?? '') + '\n');
      expect(stack.store.getDeliveries(sent.messageId)[0]?.state).toBe('sent');
      await req(hook.client, {
        type: 'session',
        event: 'prompt',
        client: 'claude',
        cliSessionId: 'cli-5062',
        host: hostRef(stack, 5062),
      });
      expect(stack.store.getDeliveries(sent.messageId)[0]?.state).toBe('delivered');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('wake accepts: marks sent for the live session, with proof when it has a transcript', async () => {
    const dir = mkdtempSync(join('/tmp', 'mm-'));
    const transcript = join(dir, 'cx.jsonl');
    writeFileSync(transcript, '');
    try {
      const wake = vi.fn(async () => true);
      const fake = fakeNative();
      const stack = await buildStack(fake.fn, { wake, proofPollMs: 20 });
      const alice = await connectPeer(stack, { handle: 'alice' });
      const hook = await connectPeer(stack);
      await announce(stack, hook, 5070, { handle: 'cx', client: 'codex', transcriptPath: transcript });
      const sent = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['cx'], body: 'wake up' });
      expect(wake).toHaveBeenCalledTimes(1);
      const [session, text] = wake.mock.calls[0] as unknown as [{ handle: string; client: string }, string];
      expect(session).toMatchObject({ handle: 'cx', client: 'codex' });
      expect(text).toContain(deliveryTag(sent.messageId));
      expect(fake.calls).toHaveLength(0);
      expect(stack.store.getDeliveries(sent.messageId)[0]?.state).toBe('sent');
      appendFileSync(transcript, text);
      await settle(() => stack.store.getDeliveries(sent.messageId)[0]?.state === 'delivered', 'wake proof delivered');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('wake returning false or rejecting leaves the delivery queued', async () => {
    let answer: 'false' | 'reject' = 'false';
    const wake = vi.fn(async () => {
      if (answer === 'reject') {
        throw new Error('codex is gone');
      }
      return false;
    });
    const stack = await buildStack(undefined, { wake });
    const alice = await connectPeer(stack, { handle: 'alice' });
    const hook = await connectPeer(stack);
    await announce(stack, hook, 5071, { handle: 'cx2', client: 'codex' });
    const first = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['cx2'], body: 'no' });
    answer = 'reject';
    const second = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['cx2'], body: 'nope' });
    expect(wake).toHaveBeenCalledTimes(2);
    expect(first.deliveries[0]?.state).toBe('queued');
    expect(second.deliveries[0]?.state).toBe('queued');
  });

  it('does not wake an ended session or a peer without a session, and native beats wake', async () => {
    const wake = vi.fn(async () => true);
    const fake = fakeNative();
    const stack = await buildStack(fake.fn, { wake });
    const alice = await connectPeer(stack, { handle: 'alice' });
    await connectPeer(stack, { handle: 'plain' });
    const hook = await connectPeer(stack);
    await announce(stack, hook, 5072, { handle: 'gone-cx', client: 'codex' });
    await req(hook.client, {
      type: 'session',
      event: 'session-end',
      client: 'codex',
      cliSessionId: 'cli-5072',
      host: hostRef(stack, 5072),
    });
    const both = await connectPeer(stack);
    await announce(stack, both, 5073, { handle: 'both', client: 'codex', native: { socketPath: '/tmp/both.sock', token: 'bt' } });
    const sent = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['plain', 'gone-cx', 'both'],
      body: 'who wakes',
    });
    expect(wake).not.toHaveBeenCalled();
    expect(fake.calls).toHaveLength(1);
    const states = Object.fromEntries(sent.deliveries.map((d) => [d.recipient, d.state]));
    expect(states).toEqual({ plain: 'queued', 'gone-cx': 'queued', both: 'sent' });
  });

  it('a native failure or throw on a live session does not fall through to wake', async () => {
    const wake = vi.fn(async () => true);
    const fake = fakeNative();
    fake.mode = 'fail';
    const stack = await buildStack(fake.fn, { wake });
    const alice = await connectPeer(stack, { handle: 'alice' });
    const hook = await connectPeer(stack);
    await announce(stack, hook, 5074, { handle: 'nat', client: 'codex', native: { socketPath: '/tmp/nat.sock', token: 'nt' } });
    const failed = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['nat'], body: 'x' });
    fake.mode = 'throw';
    const threw = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['nat'], body: 'y' });
    expect(failed.deliveries[0]?.state).toBe('queued');
    expect(threw.deliveries[0]?.state).toBe('queued');
    expect(wake).not.toHaveBeenCalled();
  });

  it('keeps a native push queued when the broker stops while it is in flight', async () => {
    let release!: (result: NativeSendResult) => void;
    const gate = new Promise<NativeSendResult>((resolve) => {
      release = resolve;
    });
    const stack = await buildStack(() => gate);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req(bob.client, { type: 'native-register', handle: 'bob', socketPath: '/tmp/bob-s.sock', token: 't' });
    const pending = req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'in flight' });
    pending.catch(() => {});
    await settle(() => stack.store.listPeers().length > 0 && stack.store.pendingPushFor('bob', 5).length > 0, 'message stored');
    const stopping = stack.broker.stop();
    release({ ok: true, error: null });
    await stopping;
    await expect(pending).rejects.toMatchObject({ code: 'broker_closed' });
    const messageId = stack.store.pendingPushFor('bob', 5)[0]?.message.id as number;
    expect(stack.store.getDeliveries(messageId)[0]?.state).toBe('queued');
    // A proof that arrives after stop is ignored.
    const inner = stack.broker as unknown as { onProof(messageId: number, recipient: string): void };
    inner.onProof(messageId, 'bob');
    expect(stack.store.getDeliveries(messageId)[0]?.state).toBe('queued');
  });

  it('leaves a delivery to its in-flight push instead of letting the recipient pull it', async () => {
    let release!: (result: NativeSendResult) => void;
    const gate = new Promise<NativeSendResult>((resolve) => {
      release = resolve;
    });
    let started = false;
    const stack = await buildStack(() => {
      started = true;
      return gate;
    });
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req(bob.client, { type: 'native-register', handle: 'bob', socketPath: '/tmp/bob-d.sock', token: 't' });
    const pending = req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'raced' });
    await settle(() => started, 'native push started');
    const fetched = await req<FetchPushResult>(bob.client, { type: 'fetch-push', handle: 'bob', limit: 5 });
    expect(fetched.messages).toEqual([]);
    // A read still lands during the flight and the finishing push must not downgrade it.
    await req<ReadResult>(bob.client, { type: 'read', ids: [1] });
    release({ ok: true, error: null });
    const sent = await pending;
    expect(stack.store.getDeliveries(sent.messageId)[0]?.state).toBe('delivered');
  });

  it('pushes a reply only to the asker as an event', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5080);
    const bob = await connectAdapter(stack, 'bob', 5081);
    const carol = await connectAdapter(stack, 'carol', 5082);
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'vote' });
    const reply = await req<ReplyResult>(bob.client, {
      type: 'reply',
      messageId: ask.messageId,
      body: 'ship it',
      verdict: 'agree',
      references: ['spec.md'],
    });
    const carolReply = pushesOf(carol).find((p) => p.payload.message.id === reply.messageId);
    const aliceReply = pushesOf(alice).find((p) => p.payload.message.id === reply.messageId);
    const recipients = stack.store.getDeliveries(reply.messageId).map((d) => d.recipient);
    expect(recipients).toEqual(['alice']);
    expect(carolReply).toBeUndefined();
    expect(aliceReply?.payload.text).toMatch(new RegExp(' re m' + String(ask.messageId) + ' AGREE to alice\\]'));
    expect(aliceReply?.payload.text).toContain('refs: spec.md');
    expect(pushesOf(bob).some((p) => p.payload.message.id === reply.messageId)).toBe(false);
    const rows = stack.store.getDeliveries(reply.messageId);
    expect(rows.map((d) => [d.recipient, d.state])).toEqual([['alice', 'sent']]);
  });

  it('notify-mode replies leave a note, not a push', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5083);
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'fyi', mode: 'notify' });
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'noted' });
    await advanceUntil(() => notificationsOf(alice).length > 0, 'reply note for alice');
    expect(notificationsOf(alice).flatMap((ev) => ev.payload)).toEqual([
      {
        kind: 'reply_arrived',
        threadId: ask.threadId,
        fromHandle: 'bob',
        detail: 'reply from bob in thread ' + String(ask.threadId),
      },
    ]);
    expect(pushesOf(alice)).toHaveLength(0);
  });
});

describe('fetch-push texts', () => {
  it('returns the rendered text of each message with every recipient listed', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await connectPeer(stack, { handle: 'carol' });
    const sent = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob', 'carol'],
      body: 'long ' + 'z'.repeat(MAX_BODY_CHARS),
      references: ['x', 'y'],
    });
    const fetched = await req<FetchPushResult>(bob.client, { type: 'fetch-push', handle: 'bob', limit: 5 });
    expect(fetched.messages).toHaveLength(1);
    expect(fetched.texts).toHaveLength(1);
    const text = fetched.texts?.[0] ?? '';
    expect(text.startsWith(deliveryTag(sent.messageId) + 't' + String(sent.threadId) + ' s0 from alice to bob, carol]\n')).toBe(true);
    expect(text).toContain('[truncated: full body stored, read it with murmur_thread full: true]');
    expect(text).toContain('\nrefs: x, y\n');
    expect(text.endsWith('murmur_thread ' + String(sent.threadId) + ' messageId ' + String(sent.messageId) + ')')).toBe(true);
  });

  it('tolerates a delivery that vanishes between listing and snapshot', async () => {
    const home = mkdtempSync(join(tmpdir(), 'murmur-broker-'));
    const paths = getPaths(home);
    ensureHome(paths);
    const store = Store.open(paths.dbPath);
    const proc = new Map<number, ProcInfo>();
    const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, undefined, () => proc);
    let blindOnce = false;
    const proxied = new Proxy(store, {
      get(target, prop) {
        if (prop === 'getDeliveries') {
          return (id: number) => {
            if (blindOnce) {
              blindOnce = false;
              return [];
            }
            return target.getDeliveries(id);
          };
        }
        const value = Reflect.get(target, prop) as unknown;
        return typeof value === 'function' ? (value as (...args: unknown[]) => unknown).bind(target) : value;
      },
    });
    const broker = await Broker.start({ paths, store: proxied as unknown as Store, core });
    const stack: Stack = { home, paths, token: readToken(paths), store, core, broker, proc, stateChanges: [] };
    stacks.push(stack);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const sent = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'once' });
    blindOnce = true;
    const fetched = await req<FetchPushResult>(bob.client, { type: 'fetch-push', handle: 'bob', limit: 5 });
    expect(fetched.messages.map((m) => m.id)).toEqual([sent.messageId]);
    expect(stack.store.getDeliveries(sent.messageId)[0]?.state).toBe('delivered');
  });
});

describe('socket startup', () => {
  it('removes a stale socket file nobody answers on', async () => {
    const home = mkdtempSync(join('/tmp', 'mm-'));
    const paths = getPaths(home);
    ensureHome(paths);
    writeFileSync(paths.socketPath, '');
    const store = Store.open(paths.dbPath);
    const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} });
    const broker = await Broker.start({ paths, store, core });
    const stack: Stack = { home, paths, token: readToken(paths), store, core, broker, proc: new Map(), stateChanges: [] };
    stacks.push(stack);
    const peer = await connectPeer(stack, { handle: 'alice' });
    expect(peer.client.closed).toBe(false);
  });

  it('refuses to start when a broker answers, and leaves that broker serving', async () => {
    const stack = await buildStack();
    await expect(Broker.start({ paths: stack.paths, store: stack.store, core: stack.core })).rejects.toThrow(
      'murmur: a broker is already listening on ' + stack.paths.socketPath,
    );
    const peer = await connectPeer(stack, { handle: 'alice' });
    await expect(statusOf(peer.client)).resolves.toBeDefined();
  });

  it('refuses to start and keeps the socket when the probe is denied, not refused (a CLI sandbox)', async () => {
    const stack = await buildStack();
    chmodSync(stack.paths.socketPath, 0);
    try {
      await expect(Broker.start({ paths: stack.paths, store: stack.store, core: stack.core })).rejects.toThrow(
        'murmur: cannot probe ' + stack.paths.socketPath + ' (EACCES); not removing it',
      );
      expect(existsSync(stack.paths.socketPath)).toBe(true);
    } finally {
      chmodSync(stack.paths.socketPath, 0o600);
    }
    const peer = await connectPeer(stack, { handle: 'alice' });
    await expect(statusOf(peer.client)).resolves.toBeDefined();
  });
});

describe('via, titles and prune', () => {
  it('stamps via on owner sends and replies, and pushes replies into the typed-in session', async () => {
    const stack = await buildStack();
    addOwner(stack, 'renn');
    const typedIn = await connectAdapter(stack, 'typed-in', 6001);
    const target = await connectAdapter(stack, 'target-b', 6002);
    const cli = await connectPeer(stack);
    await req<RegisterResult>(cli.client, { type: 'register', clientKind: 'cli', owner: true, ancestors: [1, 6001] });
    const sent = await req<SendResult>(cli.client, { type: 'send', threadId: null, to: ['target-b'], body: 'ask' });
    expect(sent.via).toBe('typed-in');
    expect(pushesOf(target)[0]?.payload.text).toContain('from renn via typed-in to target-b]');
    const reply = await req<ReplyResult>(target.client, { type: 'reply', messageId: sent.messageId, body: 'answer' });
    expect(stack.store.getDeliveries(reply.messageId).map((d) => d.recipient)).toEqual(['renn', 'typed-in']);
    await settle(() => pushesOf(typedIn).length > 0, 'reply pushed into the typed-in session');
    // The owner replying on the same connection is stamped too.
    const own = await req<ReplyResult>(cli.client, { type: 'reply', messageId: sent.messageId, body: 'thanks' });
    expect(stack.store.messageById(own.messageId)?.via).toBe('typed-in');
    const view = await req<ThreadResult>(cli.client, { type: 'thread', threadId: sent.threadId, sinceSeq: null, full: false });
    expect(view.messages.map((m) => m.via)).toEqual(['typed-in', null, 'typed-in']);
    // No session among the ancestors, or no ancestors at all: no via.
    const plain = await connectPeer(stack);
    await req(plain.client, { type: 'register', clientKind: 'cli', owner: true, ancestors: [424242] });
    expect((await req<SendResult>(plain.client, { type: 'send', threadId: null, to: ['target-b'], body: 'plain' })).via).toBeNull();
    // A re-register clears the via of the connection.
    await req(cli.client, { type: 'register', clientKind: 'cli', owner: true });
    expect((await req<SendResult>(cli.client, { type: 'send', threadId: null, to: ['target-b'], body: 'bare' })).via).toBeNull();
  });

  it('a title renames the session and its MCP connection follows to send and hold presence', async () => {
    const stack = await buildStack();
    const mcp = await connectPeer(stack);
    const host = hostRef(stack, 6010);
    await req(mcp.client, { type: 'register', clientKind: 'mcp', host, client: 'claude' });
    const hook = await connectPeer(stack);
    const base = { type: 'session', client: 'claude', cliSessionId: 'c-6010', host };
    const first = await req<SessionResult>(hook.client, { ...base, event: 'session-start', cwd: '/w/proj' });
    expect(first.handle).toBe('claude-proj');
    const peer = await connectPeer(stack, { handle: 'someone' });
    const sendAs = async (body: string): Promise<string> => {
      const sent = await req<SendResult>(mcp.client, { type: 'send', threadId: null, to: ['someone'], body });
      return senderOf(stack, sent.threadId)[0] as string;
    };
    expect(await sendAs('before')).toBe('claude-proj');
    const renamed = await req<SessionResult>(hook.client, { ...base, event: 'prompt', title: 'Live Peer A' });
    expect(renamed.handle).toBe('live-peer-a');
    expect(await sendAs('after')).toBe('live-peer-a');
    const presence = (stack.broker as unknown as { presence: Map<string, unknown> }).presence;
    expect(presence.has('live-peer-a')).toBe(true);
    expect(presence.has('claude-proj')).toBe(false);
    const status = await statusOf(peer.client);
    expect(status.peers.find((p) => p.handle === 'live-peer-a')).toMatchObject({ display: 'Live Peer A', presence: 'busy' });
    // An explicit name pins; later titles only change the display.
    await expect(req<RegisterResult>(mcp.client, { type: 'register', clientKind: 'mcp', host, handle: 'chosen' })).resolves.toEqual({
      handle: 'chosen',
      owner: false,
    });
    expect(await sendAs('chosen')).toBe('chosen');
    await expect(req<RegisterResult>(mcp.client, { type: 'register', clientKind: 'mcp', host, handle: 'chosen' })).resolves.toEqual({
      handle: 'chosen',
      owner: false,
    });
    const pinned = await req<SessionResult>(hook.client, { ...base, event: 'stop', title: 'Live Peer B' });
    expect(pinned.handle).toBe('chosen');
    expect((await statusOf(peer.client)).peers.find((p) => p.handle === 'chosen')?.display).toBe('Live Peer B');
  });

  it('a title joining an existing peer still moves the MCP connection when only an ended session keeps the old name', async () => {
    const stack = await buildStack();
    const hook = await connectPeer(stack);
    const old = { type: 'session', client: 'claude', cliSessionId: 'c-6020', host: hostRef(stack, 6020), cwd: '/w/proj' };
    await req(hook.client, { ...old, event: 'session-start' });
    await req(hook.client, { ...old, event: 'session-end' });
    stack.store.upsertPeer('live-peer-a', null, null, false, 1);
    await connectPeer(stack, { handle: 'someone' });
    const host = hostRef(stack, 6021);
    const mcp = await connectPeer(stack);
    await req(mcp.client, { type: 'register', clientKind: 'mcp', host, client: 'claude' });
    const base = { type: 'session', client: 'claude', cliSessionId: 'c-6021', host, cwd: '/w/proj' };
    expect((await req<SessionResult>(hook.client, { ...base, event: 'session-start' })).handle).toBe('claude-proj');
    expect((await req<SessionResult>(hook.client, { ...base, event: 'prompt', title: 'Live Peer A' })).handle).toBe('live-peer-a');
    const sent = await req<SendResult>(mcp.client, { type: 'send', threadId: null, to: ['someone'], body: 'hi' });
    expect(senderOf(stack, sent.threadId)[0]).toBe('live-peer-a');
  });

  it('a title rename carries pending delivery proof to the new handle', async () => {
    const dir = mkdtempSync(join('/tmp', 'mm-'));
    const transcript = join(dir, 'bob.jsonl');
    writeFileSync(transcript, 'earlier line\n');
    try {
      const fake = fakeNative();
      const stack = await buildStack(fake.fn, { proofPollMs: 20 });
      const alice = await connectPeer(stack, { handle: 'alice' });
      const hook = await connectPeer(stack);
      const base = { type: 'session', client: 'claude', cliSessionId: 'c-6030', host: hostRef(stack, 6030) };
      const first = await req<SessionResult>(hook.client, {
        ...base,
        event: 'session-start',
        cwd: '/w/proof',
        transcriptPath: transcript,
        native: { socketPath: '/tmp/rn.sock', token: 'rn' },
      });
      const sent = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: [first.handle], body: 'prove me' });
      expect(stack.store.getDeliveries(sent.messageId)[0]?.state).toBe('sent');
      const renamed = await req<SessionResult>(hook.client, { ...base, event: 'prompt', title: 'Proof Renamed' });
      expect(renamed.handle).toBe('proof-renamed');
      appendFileSync(transcript, (fake.calls[0]?.text ?? '') + '\n');
      await settle(() => stack.store.getDeliveries(sent.messageId)[0]?.state === 'delivered', 'proof delivered after rename');
      expect(stack.store.getDeliveries(sent.messageId)[0]?.recipient).toBe('proof-renamed');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('a title rename moves a queued notification batch to the new handle', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const adapter = await connectPeer(stack);
    const base = { type: 'session', client: 'claude', cliSessionId: 'c-6031', host: hostRef(stack, 6031) };
    const first = await req<SessionResult>(adapter.client, { ...base, event: 'session-start', cwd: '/w/note', listen: true });
    await req<SendResult>(alice.client, { type: 'send', threadId: null, to: [first.handle], body: 'fyi', mode: 'notify' });
    const renamed = await req<SessionResult>(adapter.client, { ...base, event: 'prompt', title: 'Note Renamed', listen: true });
    expect(renamed.handle).toBe('note-renamed');
    await advanceUntil(() => notificationsOf(adapter).length > 0, 'notification after rename');
    expect(notificationsOf(adapter)[0]?.payload).toHaveLength(1);
  });

  it('rejects a title that is not a string', async () => {
    const stack = await buildStack();
    const conn = await connectPeer(stack);
    await expect(
      req(conn.client, { type: 'session', event: 'prompt', client: 'claude', cliSessionId: 's', host: hostRef(stack, 6011), title: 5 }),
    ).rejects.toMatchObject({ code: 'bad_request', message: 'title must be a string' });
  });

  it('prune removes unreferenced peers and spares the connected, the owner and the used', async () => {
    const stack = await buildStack();
    addOwner(stack, 'renn');
    for (const handle of ['claude-0a1b2c3d', 'claude-4e5f6a7b']) {
      stack.store.upsertPeer(handle, null, null, false, 1);
    }
    await connectPeer(stack, { handle: 'still-here' });
    const cli = await connectPeer(stack);
    await req(cli.client, { type: 'register', clientKind: 'cli', owner: true });
    await expect(req(cli.client, { type: 'prune' })).resolves.toEqual({ removed: ['claude-0a1b2c3d', 'claude-4e5f6a7b'] });
    await expect(req(cli.client, { type: 'prune' })).resolves.toEqual({ removed: [] });
    expect(stack.store.getPeer('still-here')).not.toBeNull();
    expect(stack.store.getPeer('renn')).not.toBeNull();
  });
});

describe('an in-flight push', () => {
  // A held reply whose push starts when the waiter's connection closes, with
  // the native send left pending until release().
  async function inFlightHeld() {
    let release!: (r: NativeSendResult) => void;
    const calls: string[] = [];
    const stack = await buildStack((_s, _t, text) => {
      calls.push(text);
      return new Promise<NativeSendResult>((res) => {
        release = res;
      });
    });
    const a1 = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await connectPeer(stack, { handle: 'carol' });
    await req(a1.client, { type: 'native-register', handle: 'alice', socketPath: '/tmp/a.sock', token: 't' });
    const ask = await req<SendResult>(a1.client, { type: 'send', threadId: null, to: ['bob', 'carol'], body: 'q' });
    void req<WaitResult>(a1.client, { type: 'wait', threadId: ask.threadId, until: 'all_replied', timeoutMs: 20000 }).catch(() => {});
    await yieldTurn();
    await yieldTurn();
    const held = await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'HELDBODY' });
    a1.client.close();
    await settle(() => calls.length > 0, 'push started');
    return { stack, ask, held, calls, release: (r: NativeSendResult) => release(r) };
  }

  it('is not handed over by a new wait', async () => {
    const { stack, ask, held, calls, release } = await inFlightHeld();
    const a2 = await connectPeer(stack, { handle: 'alice' });
    const w = await req<WaitResult>(a2.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 1000 });
    expect(w.replies).toEqual([]);
    release({ ok: true, error: null });
    await settle(() => stack.store.getDeliveries(held.messageId).every((d) => d.state !== 'queued'), 'push done');
    expect(calls.filter((c) => c.includes('HELDBODY'))).toHaveLength(1);
  });

  it('is not handed over by the hook pull, and a failed push stays pullable', async () => {
    const { stack, held, release } = await inFlightHeld();
    const a2 = await connectPeer(stack, { handle: 'alice' });
    const during = await req<FetchPushResult>(a2.client, { type: 'fetch-push', handle: 'alice', limit: 10 });
    expect(during.messages).toEqual([]);
    release({ ok: false, error: 'refused' });
    await yieldTurn();
    await yieldTurn();
    const after = await req<FetchPushResult>(a2.client, { type: 'fetch-push', handle: 'alice', limit: 10 });
    expect(after.messages.map((m) => m.id)).toEqual([held.messageId]);
  });
});

describe('quiet error paths and shared waits', () => {
  it('swallows server and socket errors instead of crashing', async () => {
    const stack = await buildStack();
    await connectPeer(stack, { handle: 'alice' });
    const inner = stack.broker as unknown as { server: Server; conns: Set<{ socket: Socket }> };
    expect(inner.conns.size).toBe(1);
    expect(() => inner.server.emit('error', new Error('accept failed'))).not.toThrow();
    for (const conn of inner.conns) {
      expect(() => conn.socket.emit('error', new Error('reset'))).not.toThrow();
    }
    expect(inner.conns.size).toBe(1);
  });

  it('keeps a second waiter on the thread when the first connection closes', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5201);
    const second = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'shared' });
    const first = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 20000 });
    const dropped = req<WaitResult>(second.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 20000 });
    dropped.catch(() => undefined);
    await yieldTurn();
    await yieldTurn();
    second.client.close();
    await expect(dropped).rejects.toBeDefined();
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'still here' });
    expect(await first).toMatchObject({ conditionMet: true, timedOut: false });
  });

  it('survives a failed push of replies held for a wait whose connection closed', async () => {
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5202);
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'drop me' });
    const pushQueued = vi
      .spyOn(stack.broker as unknown as { pushQueued(handle: string): Promise<void> }, 'pushQueued')
      .mockRejectedValue(new Error('push failed'));
    try {
      const waiting = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 20000 });
      waiting.catch(() => undefined);
      await yieldTurn();
      await yieldTurn();
      alice.client.close();
      await settle(() => pushQueued.mock.calls.length > 0, 'pushQueued on close');
      await yieldTurn();
      expect(pushQueued).toHaveBeenCalledWith('alice');
    } finally {
      pushQueued.mockRestore();
    }
    await expect(req<StatusResult>(bob.client, { type: 'status' })).resolves.toBeDefined();
  });
});

describe('one reply resolving two waits', () => {
  it('answers each wait exactly once', async () => {
    // The reply resolves one wait, whose handover re-enters checkWaiters and resolves the other.
    const stack = await buildStack();
    const alice = await connectAdapter(stack, 'alice', 5204);
    const second = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const ask = await req<SendResult>(alice.client, { type: 'send', threadId: null, to: ['bob'], body: 'both wait' });
    const replyOk = vi.spyOn(stack.broker as unknown as { replyOk(socket: Socket, id: number, result: unknown): void }, 'replyOk');
    try {
      const one = req<WaitResult>(alice.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 20000 });
      const two = req<WaitResult>(second.client, { type: 'wait', threadId: ask.threadId, until: 'any_replied', timeoutMs: 20000 });
      await yieldTurn();
      await yieldTurn();
      await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'for both' });
      expect(await one).toMatchObject({ conditionMet: true });
      expect(await two).toMatchObject({ conditionMet: true });
      expect((stack.broker as unknown as { waiters: Map<number, unknown> }).waiters.size).toBe(0);
      const perSocket = new Map<Socket, number>();
      for (const [socket, , result] of replyOk.mock.calls) {
        if ((result as { conditionMet?: boolean }).conditionMet === true) {
          perSocket.set(socket, (perSocket.get(socket) ?? 0) + 1);
        }
      }
      expect([...perSocket.values()]).toEqual([1, 1]);
    } finally {
      replyOk.mockRestore();
    }
  });
});
