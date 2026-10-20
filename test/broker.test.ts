import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { createConnection } from 'node:net';
import type { Socket } from 'node:net';
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
import {
  LineReader,
  MAX_BODY_CHARS,
  NOTIFY_BATCH_MS,
  PROTOCOL_VERSION,
  WAIT_MAX_MS,
  encodeLine,
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
  stateChanges: Array<{ threadId: number; recipient: string; from: string; to: string }>;
}

interface Peer {
  client: MurmurClient;
  events: ServerEvent[];
}

async function buildStack(nativeSend?: NativeSendFn): Promise<Stack> {
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
  const core = new MurmurCore(store, events);
  const opts: BrokerOptions = { paths, store, core };
  if (nativeSend !== undefined) {
    opts.nativeSend = nativeSend;
  }
  const broker = await Broker.start(opts);
  const stack: Stack = { home, paths, token: readToken(paths), store, core, broker, stateChanges };
  stacks.push(stack);
  return stack;
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
    expect(JSON.parse(line)).toEqual({ v: 1, id: 1, ok: false, error: { code: 'bad_token', message: 'invalid token' } });
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
      clientKind: 'cli',
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
    await req(conn.client, { type: 'register', handle: 'alice2', clientKind: 'hook' });
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
      message: 'clientKind must be one of: mcp, cli, hook',
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
  it('pushes over a live connection and marks the delivery sent', async () => {
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const result = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'first line\nsecond line',
      references: ['carol'],
    });
    expect(result.seq).toBe(0);
    expect(result.deliveries).toHaveLength(1);
    // The reply carries the delivery snapshot taken when the message was
    // appended (before the push ran), so it reads queued; the store holds
    // the settled state.
    expect(result.deliveries[0]).toMatchObject({ recipient: 'bob', state: 'queued', sentAt: null });
    await settle(() => pushesOf(bob).length > 0, 'push event');
    const push = pushesOf(bob)[0];
    expect(push.v).toBe(PROTOCOL_VERSION);
    expect(push.type).toBe('event');
    expect(push.event).toBe('push');
    expect(push.payload.mode).toBe('push');
    expect(push.payload.message.id).toBe(result.messageId);
    expect(push.payload.message.body).toBe('first line\nsecond line');
    expect(push.payload.message.references).toEqual(['carol']);
    expect(push.payload.footer).toBe(
      'thread ' + String(result.threadId) + ' seq 0: reply with murmur_reply in_reply_to ' +
        String(result.messageId) + '. The full thread is readable with murmur_thread ' + String(result.threadId) + '.',
    );
    const row = stack.store.getDeliveries(result.messageId).find((d) => d.recipient === 'bob');
    expect(row?.state).toBe('sent');
    expect(row?.sentAt).not.toBeNull();
    expect(row?.deliveredAt).toBeNull();
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
  it('uses the injected nativeSend on the ok path and marks delivered', async () => {
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
    expect(fake.calls[0]?.text).toContain('reply with murmur_reply in_reply_to ' + String(result.messageId));
    // Native wins over the live connection: no push event arrives.
    expect(pushesOf(bob)).toHaveLength(0);
    expect(result.deliveries[0]).toMatchObject({ recipient: 'bob', state: 'queued' });
    const row = stack.store.getDeliveries(result.messageId).find((d) => d.recipient === 'bob');
    expect(row?.state).toBe('delivered');
    expect(row?.sentAt).not.toBeNull();
    expect(row?.deliveredAt).not.toBeNull();
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
    expect(stack.store.getDeliveries(result.messageId)[0]).toMatchObject({ recipient: 'bob', state: 'delivered' });
    expect(stack.store.messageById(result.messageId)?.truncated).toBe(true);
    expect(
      fake.calls[0]?.text.startsWith('x'.repeat(MAX_BODY_CHARS) + '\n[truncated: full body stored, read with murmur read]'),
    ).toBe(true);
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
      from: 'sent',
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
      message: 'verdict must be one of: agree, disagree, info',
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
    const acked = await req<AckResult>(bob.client, { type: 'ack', ids: [ask.messageId, 987654] });
    expect(acked).toEqual({ acknowledged: [ask.messageId] });
    const bobRow = stack.store.getDeliveries(ask.messageId).find((d) => d.recipient === 'bob');
    expect(bobRow?.state).toBe('acknowledged');
    expect(stack.stateChanges).toContainEqual({
      threadId: ask.threadId,
      recipient: 'bob',
      from: 'sent',
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
      from: 'sent',
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
    expect(collect.verdictTally).toEqual({ agree: 1, disagree: 0, info: 0 });
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
    expect(result.matrix[0]).toMatchObject({ recipient: 'bob', state: 'sent' });
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
    const bob = await connectPeer(stack, { handle: 'bob' });
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

  it('notifies other thread recipients when a reply arrives', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    const ask = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob', 'carol'],
      body: 'who agrees',
      mode: 'silent',
    });
    await req<ReplyResult>(bob.client, { type: 'reply', messageId: ask.messageId, body: 'me', verdict: 'agree' });
    await advanceUntil(() => notificationsOf(carol).length > 0, 'reply_arrived for carol');
    const payloads: NotificationPayload[] = notificationsOf(carol).flatMap((ev) => ev.payload);
    expect(payloads).toHaveLength(1);
    expect(payloads[0]).toMatchObject({
      kind: 'reply_arrived',
      threadId: ask.threadId,
      fromHandle: 'bob',
      detail: 'reply from bob in thread ' + String(ask.threadId),
    });
    // bob, as the sender, gets nothing; alice has no delivery row in the thread.
    expect(notificationsOf(bob)).toHaveLength(0);
    expect(notificationsOf(alice)).toHaveLength(0);
  });

  it('notifies the sender when a peer goes offline with pending deliveries, deduped per thread and sender', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
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

  it('falls back to the native transport for notifications when the peer has no live connection', async () => {
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
      body: 'offline ping',
      mode: 'silent',
    });
    // carol and dave drop their connections; both have pending deliveries.
    carol.client.close();
    dave.client.close();
    const watcher = await connectPeer(stack, { handle: 'watcher' });
    await settle(async () => {
      const status = await statusOf(watcher.client);
      const carolPeer = status.peers.find((p) => p.handle === 'carol');
      const davePeer = status.peers.find((p) => p.handle === 'dave');
      return carolPeer?.presence !== 'online' && davePeer?.presence !== 'online';
    }, 'carol and dave offline');
    fake.calls.length = 0;
    // A reply in the thread notifies the departed recipients.
    await req<ReplyResult>(alice.client, { type: 'reply', messageId: ask.messageId, body: 'update' });
    await advanceUntil(() => fake.calls.length > 0, 'native notification fallback');
    expect(fake.calls).toHaveLength(1);
    expect(fake.calls[0]?.socketPath).toBe('/tmp/carol.sock');
    const event = JSON.parse(fake.calls[0]?.text ?? '{}') as NotificationEvent;
    expect(event.event).toBe('notification');
    expect(event.payload[0]).toMatchObject({ kind: 'reply_arrived', fromHandle: 'alice' });
  });

  it('swallows a rejecting native transport when flushing notifications', async () => {
    const fake = fakeNative();
    fake.mode = 'throw';
    fakeWaitWindow();
    const stack = await buildStack(fake.fn);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    await req(bob.client, { type: 'native-register', handle: 'bob', socketPath: '/tmp/bob.sock', token: 't' });
    const ask = await req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob'],
      body: 'then vanish',
      mode: 'silent',
    });
    bob.client.close();
    const watcher = await connectPeer(stack, { handle: 'watcher' });
    await settle(async () => {
      const status = await statusOf(watcher.client);
      return status.peers.find((p) => p.handle === 'bob')?.presence !== 'online';
    }, 'bob offline');
    fake.calls.length = 0;
    await req<ReplyResult>(alice.client, { type: 'reply', messageId: ask.messageId, body: 'late update' });
    await vi.advanceTimersByTimeAsync(NOTIFY_BATCH_MS);
    expect(fake.calls).toHaveLength(1);
    // No unhandled rejection: the broker still serves.
    await expect(statusOf(watcher.client)).resolves.toBeDefined();
  });
});

describe('dead peer and queued delivery', () => {
  it('keeps the offline peer queued, reports it via collect, then delivers with a queued_delivered notice', async () => {
    fakeWaitWindow();
    const stack = await buildStack();
    const alice = await connectPeer(stack, { handle: 'alice' });
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
    // The reply snapshot predates the push; the store carries the outcome:
    // alice's live copy was pushed (sent), bob's stays queued.
    expect(ask.deliveries.map((d) => d.state)).toEqual(['queued', 'queued']);
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
      message: 'event must be one of: session-start, prompt, stop',
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
    expect(rows.find((d) => d.recipient === 'bob')?.state).toBe('delivered');
    expect(rows.find((d) => d.recipient === 'carol')?.state).toBe('queued');
  });

  it('falls back to the stored full body when the message row is gone at push time', async () => {
    const home = mkdtempSync(join(tmpdir(), 'murmur-broker-'));
    const paths = getPaths(home);
    ensureHome(paths);
    const store = Store.open(paths.dbPath);
    const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} });
    let hideMessageId: number | null = null;
    const proxiedStore = new Proxy(store, {
      get(target, prop) {
        if (prop === 'messageById') {
          return (id: number) => (hideMessageId === id ? null : target.messageById(id));
        }
        const value = Reflect.get(target, prop) as unknown;
        return typeof value === 'function' ? (value as (...args: unknown[]) => unknown).bind(target) : value;
      },
    });
    let release!: (result: NativeSendResult) => void;
    const gate = new Promise<NativeSendResult>((resolve) => {
      release = resolve;
    });
    const calls: string[] = [];
    const stub: NativeSendFn = async (socketPath: string, token: string, text: string) => {
      calls.push(text);
      return gate;
    };
    const broker = await Broker.start({ paths, store: proxiedStore as unknown as Store, core, nativeSend: stub });
    const stack: Stack = { home, paths, token: readToken(paths), store, core, broker, stateChanges: [] };
    stacks.push(stack);
    const alice = await connectPeer(stack, { handle: 'alice' });
    const bob = await connectPeer(stack, { handle: 'bob' });
    const carol = await connectPeer(stack, { handle: 'carol' });
    await req(bob.client, { type: 'native-register', handle: 'bob', socketPath: '/tmp/fake-bob.sock', token: 'tb' });
    await req(carol.client, { type: 'native-register', handle: 'carol', socketPath: '/tmp/fake-carol.sock', token: 'tc' });
    const longBody = 'y'.repeat(MAX_BODY_CHARS + 50);
    const pending = req<SendResult>(alice.client, {
      type: 'send',
      threadId: null,
      to: ['bob', 'carol'],
      body: longBody,
    });
    await settle(() => calls.length === 1, 'first native push started');
    hideMessageId = stack.store.pendingPushFor('carol', 5)[0]?.message.id ?? null;
    expect(hideMessageId).not.toBeNull();
    release({ ok: true, error: null });
    await pending;
    const truncatedPrefix = longBody.slice(0, MAX_BODY_CHARS) + '\n[truncated: full body stored, read with murmur read]';
    expect(calls[0]?.startsWith(truncatedPrefix)).toBe(true);
    expect(calls[1]?.startsWith(longBody)).toBe(true);
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
    const bob = await connectPeer(stack, { handle: 'bob' });
    const inner = stack.broker as unknown as { presence: Map<string, { socket: Socket }> };
    const bobSocket = inner.presence.get('bob')?.socket;
    expect(bobSocket).toBeDefined();
    const originalWrite = bobSocket!.write.bind(bobSocket);
    bobSocket!.write = (() => {
      throw new Error('kaboom');
    }) as typeof bobSocket!.write;
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
      bobSocket!.write = originalWrite as typeof bobSocket!.write;
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
    const fakeWaiter = { conn: fakeConn, id: 13, threadId: 2, until: 'all_replied', timer: undefined };
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
