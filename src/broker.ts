import { chmodSync } from 'node:fs';
import { Server, Socket, createServer } from 'node:net';
import { StringDecoder } from 'node:string_decoder';
import { readToken } from './config.js';
import type { MurmurPaths } from './config.js';
import type { MurmurCore, PushTarget } from './core.js';
import { nativeSend } from './native.js';
import type { NativeSendResult } from './native.js';
import type { Store } from './store.js';
import {
  LineReader,
  MurmurError,
  NOTIFY_BATCH_MS,
  PROTOCOL_VERSION,
  WAIT_MAX_MS,
  encodeLine,
} from './protocol.js';
import type {
  AckResult,
  ClientKind,
  CollectResult,
  DeliveryMode,
  ErrorCode,
  FetchPushResult,
  GroupAction,
  GroupsResult,
  HookEvent,
  InboxResult,
  NotificationEvent,
  NotificationPayload,
  PushEvent,
  PushPayload,
  ReadResult,
  RecipientMatrix,
  RegisterResult,
  ReplyResult,
  SendResult,
  StatusResult,
  ThreadResult,
  Verdict,
  WaitUntil,
} from './protocol.js';

// The Unix socket server. Newline-delimited JSON, one shared token from the
// token file, per-connection auth, presence tracking, push execution after
// sends, batched notifications, and waiters. All policy lives in MurmurCore;
// the broker drives it and owns the sockets.

export interface BrokerOptions {
  paths: MurmurPaths;
  store: Store;
  core: MurmurCore;
  nativeSend?: (socketPath: string, token: string, text: string) => Promise<NativeSendResult>;
}

interface ConnState {
  socket: Socket;
  decoder: StringDecoder;
  reader: LineReader;
  authenticated: boolean;
  // Handle this connection is currently bound as in the presence map, or
  // null. A second register (or hook) on one connection rebinds.
  handle: string | null;
  // Bytes of the not yet complete trailing line, for the 1 MiB line guard.
  pendingBytes: number;
  // Waiters registered by this connection, so close can clear their timers.
  waiters: Set<Waiter>;
  // Lines are processed strictly in order; each link awaits the previous
  // handler, so one connection cannot interleave its own requests.
  chain: Promise<void>;
}

interface Waiter {
  conn: ConnState;
  id: number;
  threadId: number;
  until: WaitUntil;
  timer: NodeJS.Timeout;
}

interface NotifyEntry {
  payloads: NotificationPayload[];
  timer: NodeJS.Timeout;
}

const CLIENT_KINDS: readonly ClientKind[] = ['mcp', 'cli', 'hook'];
const HOOK_EVENTS: readonly HookEvent[] = ['session-start', 'prompt', 'stop'];
const DELIVERY_MODES: readonly DeliveryMode[] = ['push', 'notify', 'silent'];
const VERDICTS: readonly Verdict[] = ['agree', 'disagree', 'info'];
const GROUP_ACTIONS: readonly GroupAction[] = ['create', 'list', 'add', 'remove', 'delete'];
const WAIT_UNTILS: readonly WaitUntil[] = ['all_replied', 'any_replied'];

// Wire guard: no single request line may exceed 1 MiB.
const MAX_LINE_BYTES = 1024 * 1024;

function badRequest(message: string): MurmurError {
  return new MurmurError('bad_request', message);
}

function reqString(rec: Record<string, unknown>, key: string): string {
  const value = rec[key];
  if (typeof value !== 'string') {
    throw badRequest(key + ' must be a string');
  }
  return value;
}

function optString(rec: Record<string, unknown>, key: string): string | undefined {
  if (rec[key] === undefined) {
    return undefined;
  }
  return reqString(rec, key);
}

function reqNumber(rec: Record<string, unknown>, key: string): number {
  const value = rec[key];
  if (typeof value !== 'number') {
    throw badRequest(key + ' must be a number');
  }
  return value;
}

function reqNumberOrNull(rec: Record<string, unknown>, key: string): number | null {
  const value = rec[key];
  if (value === null) {
    return null;
  }
  return reqNumber(rec, key);
}

function reqBool(rec: Record<string, unknown>, key: string): boolean {
  const value = rec[key];
  if (typeof value !== 'boolean') {
    throw badRequest(key + ' must be a boolean');
  }
  return value;
}

function reqStringArray(rec: Record<string, unknown>, key: string): string[] {
  const value = rec[key];
  if (!Array.isArray(value)) {
    throw badRequest(key + ' must be an array of strings');
  }
  for (const item of value) {
    if (typeof item !== 'string') {
      throw badRequest(key + ' must be an array of strings');
    }
  }
  return value;
}

function optStringArray(rec: Record<string, unknown>, key: string): string[] | undefined {
  if (rec[key] === undefined) {
    return undefined;
  }
  return reqStringArray(rec, key);
}

function reqNumberArray(rec: Record<string, unknown>, key: string): number[] {
  const value = rec[key];
  if (!Array.isArray(value)) {
    throw badRequest(key + ' must be an array of numbers');
  }
  for (const item of value) {
    if (typeof item !== 'number') {
      throw badRequest(key + ' must be an array of numbers');
    }
  }
  return value;
}

function reqEnum<T extends string>(rec: Record<string, unknown>, key: string, allowed: readonly T[]): T {
  const value = rec[key];
  if (typeof value !== 'string' || !allowed.includes(value as T)) {
    throw badRequest(key + ' must be one of: ' + allowed.join(', '));
  }
  return value as T;
}

function optEnum<T extends string>(rec: Record<string, unknown>, key: string, allowed: readonly T[]): T | undefined {
  if (rec[key] === undefined) {
    return undefined;
  }
  return reqEnum(rec, key, allowed);
}

export class Broker {
  private readonly server: Server;
  private readonly paths: MurmurPaths;
  private readonly store: Store;
  private readonly core: MurmurCore;
  private readonly nativeSendFn: (socketPath: string, token: string, text: string) => Promise<NativeSendResult>;
  // Live bindings: handle to connection. register and hook both bind.
  private readonly presence = new Map<string, ConnState>();
  private readonly conns = new Set<ConnState>();
  // Recipient to pending notification payloads, one flush timer per recipient.
  private readonly notifyQueue = new Map<string, NotifyEntry>();
  // Thread id to waiters registered against that thread.
  private readonly waiters = new Map<number, Set<Waiter>>();
  private stopped = false;
  private stopPromise: Promise<void> | null = null;

  private constructor(opts: BrokerOptions, server: Server) {
    this.server = server;
    this.paths = opts.paths;
    this.store = opts.store;
    this.core = opts.core;
    this.nativeSendFn = opts.nativeSend ?? nativeSend;
  }

  static async start(opts: BrokerOptions): Promise<Broker> {
    const server = createServer();
    let broker: Broker | null = null;
    // Attached before listen so no connection can arrive unhandled.
    server.on('connection', (socket: Socket) => {
      if (broker === null) {
        socket.destroy();
        return;
      }
      broker.onConnection(socket);
    });
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(opts.paths.socketPath, () => resolve());
    });
    try {
      chmodSync(opts.paths.socketPath, 0o600);
    } catch (err) {
      server.close();
      throw new Error(
        'murmur: cannot set mode 0600 on socket ' + opts.paths.socketPath + ': ' + (err as Error).message,
      );
    }
    // Yield two macrotask turns before the instance exists: a client that
    // dials the socket in this window is accepted and then destroyed by the
    // guard above, so it retries against a fully started broker instead of a
    // half-built one.
    for (let i = 0; i < 2; i += 1) {
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
    }
    broker = new Broker(opts, server);
    // Server errors after a successful listen are rare; swallow them so an
    // idle accept failure cannot crash the process. Connection cleanup runs
    // through each socket's own close handler.
    server.on('error', () => {
      // no-op
    });
    return broker;
  }

  async stop(): Promise<void> {
    if (this.stopPromise !== null) {
      return this.stopPromise;
    }
    this.stopPromise = this.stopInner();
    return this.stopPromise;
  }

  private async stopInner(): Promise<void> {
    this.stopped = true;
    for (const set of this.waiters.values()) {
      for (const waiter of set) {
        clearTimeout(waiter.timer);
        waiter.conn.waiters.delete(waiter);
      }
    }
    this.waiters.clear();
    for (const entry of this.notifyQueue.values()) {
      clearTimeout(entry.timer);
    }
    this.notifyQueue.clear();
    for (const conn of [...this.conns]) {
      conn.socket.destroy();
    }
    this.conns.clear();
    this.presence.clear();
    await new Promise<void>((resolve) => {
      this.server.close(() => resolve());
    });
  }

  // Connection lifecycle.

  private onConnection(socket: Socket): void {
    const conn: ConnState = {
      socket,
      decoder: new StringDecoder('utf8'),
      reader: new LineReader(),
      authenticated: false,
      handle: null,
      pendingBytes: 0,
      waiters: new Set<Waiter>(),
      chain: Promise.resolve(),
    };
    this.conns.add(conn);
    socket.on('data', (chunk: Buffer) => {
      this.onSocketData(conn, chunk);
    });
    socket.on('error', () => {
      // Socket errors surface as a close; cleanup happens there.
    });
    socket.on('close', () => {
      this.onSocketClose(conn);
    });
  }

  private onSocketData(conn: ConnState, chunk: Buffer): void {
    // Byte exact line length guard: every complete line in this chunk, plus
    // the trailing partial line, must stay under 1 MiB.
    let offset = 0;
    for (;;) {
      const nl = chunk.indexOf(0x0a, offset);
      if (nl === -1) {
        break;
      }
      if (conn.pendingBytes + (nl - offset) > MAX_LINE_BYTES) {
        this.rejectOversizedLine(conn);
        return;
      }
      conn.pendingBytes = 0;
      offset = nl + 1;
    }
    conn.pendingBytes += chunk.byteLength - offset;
    if (conn.pendingBytes > MAX_LINE_BYTES) {
      this.rejectOversizedLine(conn);
      return;
    }
    const text = conn.decoder.write(chunk);
    for (const line of conn.reader.feed(text)) {
      conn.chain = conn.chain.then(() => this.dispatchLine(conn, line));
    }
  }

  private rejectOversizedLine(conn: ConnState): void {
    this.replyErr(conn.socket, 0, 'internal', 'line exceeds the 1 MiB limit');
    conn.socket.destroy();
  }

  private onSocketClose(conn: ConnState): void {
    this.conns.delete(conn);
    for (const waiter of [...conn.waiters]) {
      clearTimeout(waiter.timer);
      this.removeWaiter(waiter);
    }
    if (conn.handle !== null && this.presence.get(conn.handle) === conn) {
      this.presence.delete(conn.handle);
    }
    // Stamp nothing on close; only register refreshes last_seen_at.
    if (conn.handle !== null && !this.stopped) {
      this.notifyPeerOffline(conn.handle);
    }
  }

  private async dispatchLine(conn: ConnState, line: string): Promise<void> {
    if (this.stopped || conn.socket.destroyed) {
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch (err) {
      this.replyErr(conn.socket, 0, 'bad_request', 'line is not valid JSON: ' + (err as Error).message);
      return;
    }
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      this.replyErr(conn.socket, 0, 'bad_request', 'line must be a JSON object');
      return;
    }
    const rec = parsed as Record<string, unknown>;
    const id = typeof rec.id === 'number' ? rec.id : 0;
    if (!conn.authenticated) {
      this.handleAuthLine(conn, rec, id);
      return;
    }
    try {
      await this.handleMessage(conn, rec, id);
    } catch (err) {
      if (err instanceof MurmurError) {
        this.replyErr(conn.socket, id, err.code, err.message);
      } else {
        this.replyErr(conn.socket, id, 'internal', err instanceof Error ? err.message : String(err));
      }
    }
  }

  private handleAuthLine(conn: ConnState, rec: Record<string, unknown>, id: number): void {
    const type = typeof rec.type === 'string' ? rec.type : '';
    if (type !== 'auth') {
      this.replyErr(conn.socket, id, 'auth_required', 'authenticate with a token before anything else');
      conn.socket.destroy();
      return;
    }
    let expected: string;
    try {
      expected = readToken(this.paths);
    } catch (err) {
      const code: ErrorCode = err instanceof MurmurError ? err.code : 'internal';
      this.replyErr(conn.socket, id, code, err instanceof Error ? err.message : String(err));
      conn.socket.destroy();
      return;
    }
    const token = rec.token;
    if (typeof token !== 'string' || token !== expected) {
      this.replyErr(conn.socket, id, 'bad_token', 'invalid token');
      conn.socket.destroy();
      return;
    }
    conn.authenticated = true;
    this.replyOk(conn.socket, id, null);
  }

  // Dispatch by type. Every handler path is inside the caller's try, so an
  // unexpected throw lands in the wrap: MurmurError keeps its code, anything
  // else becomes internal.
  private async handleMessage(conn: ConnState, rec: Record<string, unknown>, id: number): Promise<void> {
    const type = typeof rec.type === 'string' ? rec.type : '';
    switch (type) {
      case 'auth':
        throw badRequest('connection is already authenticated');
      case 'register':
        this.replyOk(conn.socket, id, this.handleRegister(conn, rec));
        return;
      case 'hook':
        this.replyOk(conn.socket, id, this.handleHook(conn, rec));
        return;
      case 'native-register':
        this.replyOk(conn.socket, id, this.handleNativeRegister(rec));
        return;
      case 'send': {
        const result = await this.handleSend(conn, rec);
        this.replyOk(conn.socket, id, result);
        return;
      }
      case 'reply':
        this.replyOk(conn.socket, id, this.handleReply(conn, rec));
        return;
      case 'ack':
        this.replyOk(conn.socket, id, this.handleAck(conn, rec));
        return;
      case 'read':
        this.replyOk(conn.socket, id, this.handleRead(conn, rec));
        return;
      case 'inbox':
        this.replyOk(conn.socket, id, this.handleInbox(conn, rec));
        return;
      case 'thread':
        this.replyOk(conn.socket, id, this.handleThread(rec));
        return;
      case 'collect':
        this.replyOk(conn.socket, id, this.handleCollect(rec));
        return;
      case 'wait':
        // Replies itself: immediately when met, otherwise from a timer.
        this.handleWait(conn, rec, id);
        return;
      case 'groups':
        this.replyOk(conn.socket, id, this.handleGroups(rec));
        return;
      case 'status':
        this.replyOk(conn.socket, id, this.handleStatus());
        return;
      case 'fetch-push':
        this.replyOk(conn.socket, id, this.handleFetchPush(rec));
        return;
      default:
        throw badRequest('unknown message type: ' + String(rec.type));
    }
  }

  // Handlers.

  private handleRegister(conn: ConnState, rec: Record<string, unknown>): RegisterResult {
    const handle = reqString(rec, 'handle');
    const display = optString(rec, 'display');
    const role = optString(rec, 'role');
    // clientKind rides on the wire; core.register has no use for it.
    reqEnum<ClientKind>(rec, 'clientKind', CLIENT_KINDS);
    const arg: { handle: string; display?: string; role?: string } = { handle };
    if (display !== undefined) {
      arg.display = display;
    }
    if (role !== undefined) {
      arg.role = role;
    }
    const result = this.core.register(arg);
    this.bind(conn, handle);
    this.core.touchSeen(handle);
    return result;
  }

  private handleHook(conn: ConnState, rec: Record<string, unknown>): null {
    const event = reqEnum<HookEvent>(rec, 'event', HOOK_EVENTS);
    const handle = reqString(rec, 'handle');
    // sessionHint rides on the wire; core.hookState has no use for it.
    optString(rec, 'sessionHint');
    // hookState creates the peer row when the handle is new (ensurePeer).
    this.core.hookState(handle, event);
    this.bind(conn, handle);
    return null;
  }

  private handleNativeRegister(rec: Record<string, unknown>): null {
    const handle = reqString(rec, 'handle');
    const socketPath = reqString(rec, 'socketPath');
    const token = reqString(rec, 'token');
    this.core.registerNative({ handle, socketPath, token });
    return null;
  }

  private async handleSend(conn: ConnState, rec: Record<string, unknown>): Promise<SendResult> {
    const sender = this.requireBoundHandle(conn);
    const threadId = reqNumberOrNull(rec, 'threadId');
    const to = reqStringArray(rec, 'to');
    const body = reqString(rec, 'body');
    const references = optStringArray(rec, 'references');
    const mode = optEnum<DeliveryMode>(rec, 'mode', DELIVERY_MODES);
    const arg: { threadId: number | null; to: string[]; body: string; references?: string[]; mode?: DeliveryMode } = {
      threadId,
      to,
      body,
    };
    if (references !== undefined) {
      arg.references = references;
    }
    if (mode !== undefined) {
      arg.mode = mode;
    }
    const { result, pushes } = this.core.send(sender, arg);
    // Pushes run before the reply so delivery states are settled once the
    // sender sees the ok.
    await this.executePushes(pushes);
    if (mode === 'notify') {
      for (const delivery of result.deliveries) {
        if (delivery.recipient === sender) {
          continue;
        }
        this.enqueueNotification(delivery.recipient, {
          kind: 'reply_arrived',
          threadId: result.threadId,
          fromHandle: sender,
          detail: 'new message from ' + sender + ' in thread ' + String(result.threadId),
        });
      }
    }
    this.checkWaiters(result.threadId);
    return result;
  }

  private handleReply(conn: ConnState, rec: Record<string, unknown>): ReplyResult {
    const sender = this.requireBoundHandle(conn);
    const messageId = reqNumber(rec, 'messageId');
    const body = reqString(rec, 'body');
    const verdict = optEnum<Verdict>(rec, 'verdict', VERDICTS);
    const references = optStringArray(rec, 'references');
    const arg: { messageId: number; body: string; verdict?: Verdict; references?: string[] } = { messageId, body };
    if (verdict !== undefined) {
      arg.verdict = verdict;
    }
    if (references !== undefined) {
      arg.references = references;
    }
    const { result } = this.core.reply(sender, arg);
    // reply_arrived to every other thread recipient.
    for (const recipient of this.store.threadRecipientHandles(result.threadId)) {
      if (recipient === sender) {
        continue;
      }
      this.enqueueNotification(recipient, {
        kind: 'reply_arrived',
        threadId: result.threadId,
        fromHandle: sender,
        detail: 'reply from ' + sender + ' in thread ' + String(result.threadId),
      });
    }
    this.checkWaiters(result.threadId);
    return result;
  }

  private handleAck(conn: ConnState, rec: Record<string, unknown>): AckResult {
    const handle = this.requireBoundHandle(conn);
    const ids = reqNumberArray(rec, 'ids');
    // Mirror core.ack's eligibility filter (a delivery row for this handle in
    // delivered or sent) so the reply can carry the acknowledged ids, which
    // is the shape AckResult promises.
    const acknowledged: number[] = [];
    const threadIds = new Set<number>();
    for (const messageId of ids) {
      const message = this.store.messageById(messageId);
      if (message === null) {
        continue;
      }
      const delivery = this.store.getDeliveries(messageId).find((d) => d.recipient === handle);
      if (delivery === undefined || (delivery.state !== 'delivered' && delivery.state !== 'sent')) {
        continue;
      }
      acknowledged.push(messageId);
      threadIds.add(message.threadId);
    }
    this.core.ack(handle, ids);
    for (const threadId of threadIds) {
      this.checkWaiters(threadId);
    }
    return { acknowledged };
  }

  private handleRead(conn: ConnState, rec: Record<string, unknown>): ReadResult {
    const handle = this.requireBoundHandle(conn);
    const ids = reqNumberArray(rec, 'ids');
    const messages = this.core.read(handle, ids);
    for (const message of messages) {
      this.checkWaiters(message.threadId);
    }
    return { messages };
  }

  private handleInbox(conn: ConnState, rec: Record<string, unknown>): InboxResult {
    const handle = this.requireBoundHandle(conn);
    const unreadOnly = reqBool(rec, 'unreadOnly');
    return this.core.inbox(handle, unreadOnly);
  }

  private handleThread(rec: Record<string, unknown>): ThreadResult {
    const threadId = reqNumber(rec, 'threadId');
    const sinceSeq = reqNumberOrNull(rec, 'sinceSeq');
    // full is part of the wire shape; core.threadView always returns full
    // bodies, so it is validated and nothing more.
    reqBool(rec, 'full');
    return this.core.threadView(threadId, sinceSeq);
  }

  private handleCollect(rec: Record<string, unknown>): CollectResult {
    const threadId = reqNumber(rec, 'threadId');
    return this.core.collect(threadId);
  }

  private handleGroups(rec: Record<string, unknown>): GroupsResult[] {
    const action = reqEnum<GroupAction>(rec, 'action', GROUP_ACTIONS);
    const name = optString(rec, 'name');
    const members = optStringArray(rec, 'members');
    if (name === undefined) {
      return this.core.groups(action);
    }
    if (members === undefined) {
      return this.core.groups(action, name);
    }
    return this.core.groups(action, name, members);
  }

  private handleStatus(): StatusResult {
    return this.core.status((handle) => ({ online: this.presence.has(handle) }));
  }

  private handleFetchPush(rec: Record<string, unknown>): FetchPushResult {
    const handle = reqString(rec, 'handle');
    const limit = reqNumber(rec, 'limit');
    if (!Number.isInteger(limit) || limit < 0) {
      throw badRequest('limit must be a non-negative integer');
    }
    // Snapshot the pending delivery states so the moves from queued to
    // delivered can be reported after core.pendingPush hands them out.
    const before = new Map<number, string>();
    for (const entry of this.store.pendingPushFor(handle, limit)) {
      const delivery = this.store.getDeliveries(entry.message.id).find((d) => d.recipient === handle);
      if (delivery !== undefined) {
        before.set(entry.message.id, delivery.state);
      }
    }
    const result = this.core.pendingPush(handle, limit);
    for (const message of result.messages) {
      if (before.get(message.id) === 'queued') {
        this.enqueueNotification(message.sender, {
          kind: 'queued_delivered',
          threadId: message.threadId,
          fromHandle: handle,
          detail:
            'queued message ' + String(message.id) + ' in thread ' + String(message.threadId) +
            ' was delivered to ' + handle,
        });
      }
      this.checkWaiters(message.threadId);
    }
    return result;
  }

  // Push execution after core.send. core.pushKindFor only yields native or
  // connection for push mode messages, so the payload mode is always push
  // here; queued targets stay queued and surface through notifications or
  // fetch-push.
  private async executePushes(pushes: PushTarget[]): Promise<void> {
    for (const target of pushes) {
      if (this.stopped) {
        return;
      }
      if (target.kind === 'queued') {
        continue;
      }
      const payload: PushPayload = { message: target.message, mode: 'push', footer: target.footer };
      if (target.kind === 'native') {
        // A vanished transport row skips silently: the delivery stays queued.
        const transport = this.store.getNativeTransport(target.recipient);
        if (transport === null) {
          continue;
        }
        let outcome: NativeSendResult;
        try {
          outcome = await this.nativeSendFn(transport.socketPath, transport.token, this.nativeTextFor(target));
        } catch {
          // A throwing transport counts as a failed send: leave queued.
          continue;
        }
        if (!outcome.ok) {
          continue;
        }
        // queued -> sent -> delivered, waking thread waiters after each hop.
        const now = Date.now();
        this.store.updateDelivery(target.message.id, target.recipient, { state: 'sent', sentAt: now });
        this.checkWaiters(target.message.threadId);
        this.store.updateDelivery(target.message.id, target.recipient, { state: 'delivered', deliveredAt: now });
        this.checkWaiters(target.message.threadId);
      } else {
        const peer = this.presence.get(target.recipient);
        if (peer === undefined || peer.socket.destroyed) {
          continue;
        }
        const event: PushEvent = { v: PROTOCOL_VERSION, type: 'event', event: 'push', payload };
        this.writeLine(peer.socket, encodeLine(event));
        this.store.updateDelivery(target.message.id, target.recipient, { state: 'sent', sentAt: Date.now() });
        this.checkWaiters(target.message.threadId);
      }
    }
  }

  private nativeTextFor(target: PushTarget): string {
    // The delivered body carries the truncation notice when the stored body
    // was over MAX_BODY_CHARS.
    const row = this.store.messageById(target.message.id);
    const body = row !== null ? row.deliveredBody : target.message.body;
    return body + '\n\n' + target.footer;
  }

  // Notifications.

  private enqueueNotification(recipient: string, payload: NotificationPayload): void {
    if (this.stopped) {
      return;
    }
    let entry = this.notifyQueue.get(recipient);
    if (entry === undefined) {
      entry = {
        payloads: [],
        timer: setTimeout(() => this.flushNotifications(recipient), NOTIFY_BATCH_MS),
      };
      this.notifyQueue.set(recipient, entry);
    }
    entry.payloads.push(payload);
  }

  private flushNotifications(recipient: string): void {
    const entry = this.notifyQueue.get(recipient);
    if (entry === undefined) {
      return;
    }
    this.notifyQueue.delete(recipient);
    clearTimeout(entry.timer);
    if (this.stopped || entry.payloads.length === 0) {
      return;
    }
    const event: NotificationEvent = {
      v: PROTOCOL_VERSION,
      type: 'event',
      event: 'notification',
      payload: entry.payloads,
    };
    const peer = this.presence.get(recipient);
    if (peer !== undefined && !peer.socket.destroyed) {
      this.writeLine(peer.socket, encodeLine(event));
      return;
    }
    // No live connection: fall back to the native transport when one exists,
    // otherwise drop the batch (the data stays in the DB).
    const transport = this.store.getNativeTransport(recipient);
    if (transport === null) {
      return;
    }
    this.nativeSendFn(transport.socketPath, transport.token, JSON.stringify(event)).catch(() => {
      // Notifications are best effort; the data stays in the DB.
    });
  }

  // On disconnect: the peers still waiting on this handle learn it went
  // offline. A notified peer is the sender of a message the departed handle
  // has not yet consumed (queued or sent delivery), one per thread per sender.
  private notifyPeerOffline(handle: string): void {
    const seen = new Set<string>();
    for (const row of this.store.inboxFor(handle, false).messages) {
      const delivery = this.store.getDeliveries(row.id).find((d) => d.recipient === handle);
      if (delivery === undefined || (delivery.state !== 'queued' && delivery.state !== 'sent')) {
        continue;
      }
      if (row.sender === handle) {
        continue;
      }
      const key = String(row.threadId) + '\u0000' + row.sender;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      this.enqueueNotification(row.sender, {
        kind: 'peer_offline',
        threadId: row.threadId,
        fromHandle: handle,
        detail: 'peer ' + handle + ' went offline with undelivered messages in thread ' + String(row.threadId),
      });
    }
  }

  // Waiters.

  private handleWait(conn: ConnState, rec: Record<string, unknown>, id: number): void {
    const until = reqEnum<WaitUntil>(rec, 'until', WAIT_UNTILS);
    const timeoutMs = reqNumber(rec, 'timeoutMs');
    if (timeoutMs < 0) {
      throw badRequest('timeoutMs must not be negative');
    }
    if (timeoutMs > WAIT_MAX_MS) {
      throw badRequest('timeoutMs exceeds the cap of ' + String(WAIT_MAX_MS) + 'ms');
    }
    const threadId = reqNumber(rec, 'threadId');
    if (this.conditionHolds(threadId, until)) {
      this.replyOk(conn.socket, id, {
        threadId,
        conditionMet: true,
        timedOut: false,
        matrix: this.matrixFor(threadId),
      });
      return;
    }
    const waiter: Waiter = {
      conn,
      id,
      threadId,
      until,
      timer: setTimeout(() => this.resolveTimedOut(waiter), timeoutMs),
    };
    conn.waiters.add(waiter);
    let set = this.waiters.get(threadId);
    if (set === undefined) {
      set = new Set<Waiter>();
      this.waiters.set(threadId, set);
    }
    set.add(waiter);
  }

  // The condition is evaluated against the thread's latest ask: all_replied
  // wants every recipient delivery replied, any_replied wants at least one.
  // An ask with no recipients (or no ask at all) never holds.
  private conditionHolds(threadId: number, until: WaitUntil): boolean {
    const ask = this.store.latestAsk(threadId);
    if (ask === null) {
      return false;
    }
    const deliveries = this.store.getDeliveries(ask.id);
    if (deliveries.length === 0) {
      return false;
    }
    if (until === 'any_replied') {
      return deliveries.some((d) => d.state === 'replied');
    }
    return deliveries.every((d) => d.state === 'replied');
  }

  // The collect matrix for a WaitResult. A thread without messages or without
  // an ask collects nothing, which degrades to an empty matrix.
  private matrixFor(threadId: number): RecipientMatrix[] {
    try {
      return this.core.collect(threadId).recipients;
    } catch (err) {
      if (err instanceof MurmurError) {
        return [];
      }
      throw err;
    }
  }

  // Called after every mutation the broker drives (sends, replies, acks,
  // reads, fetch-push, push state hops), which together cover every append
  // and state change in the process, so a waiter resolves as soon as the
  // condition starts to hold.
  private checkWaiters(threadId: number): void {
    const set = this.waiters.get(threadId);
    if (set === undefined) {
      return;
    }
    for (const waiter of [...set]) {
      if (this.stopped) {
        return;
      }
      if (!this.conditionHolds(threadId, waiter.until)) {
        continue;
      }
      this.removeWaiter(waiter);
      clearTimeout(waiter.timer);
      this.replyOk(waiter.conn.socket, waiter.id, {
        threadId,
        conditionMet: true,
        timedOut: false,
        matrix: this.matrixFor(threadId),
      });
    }
  }

  private resolveTimedOut(waiter: Waiter): void {
    this.removeWaiter(waiter);
    if (this.stopped) {
      return;
    }
    try {
      this.replyOk(waiter.conn.socket, waiter.id, {
        threadId: waiter.threadId,
        conditionMet: false,
        timedOut: true,
        matrix: this.matrixFor(waiter.threadId),
      });
    } catch {
      // Never let a timer callback crash the process.
    }
  }

  private removeWaiter(waiter: Waiter): void {
    const set = this.waiters.get(waiter.threadId);
    if (set !== undefined) {
      set.delete(waiter);
      if (set.size === 0) {
        this.waiters.delete(waiter.threadId);
      }
    }
    waiter.conn.waiters.delete(waiter);
  }

  // Presence and replies.

  private bind(conn: ConnState, handle: string): void {
    if (conn.handle !== null && this.presence.get(conn.handle) === conn) {
      this.presence.delete(conn.handle);
    }
    conn.handle = handle;
    this.presence.set(handle, conn);
  }

  private requireBoundHandle(conn: ConnState): string {
    if (conn.handle === null) {
      throw badRequest('no handle is registered on this connection');
    }
    return conn.handle;
  }

  private replyOk(socket: Socket, id: number, result: unknown): void {
    this.writeLine(socket, encodeLine({ v: PROTOCOL_VERSION, id, ok: true, result }));
  }

  private replyErr(socket: Socket, id: number, code: ErrorCode, message: string): void {
    this.writeLine(socket, encodeLine({ v: PROTOCOL_VERSION, id, ok: false, error: { code, message } }));
  }

  private writeLine(socket: Socket, line: string): void {
    if (socket.destroyed) {
      return;
    }
    try {
      socket.write(line);
    } catch {
      // The socket died mid-write; the close handler finishes cleanup.
    }
  }
}
