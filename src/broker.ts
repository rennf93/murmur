import { chmodSync, existsSync, unlinkSync } from 'node:fs';
import { Server, Socket, createConnection, createServer } from 'node:net';
import { StringDecoder } from 'node:string_decoder';
import { readToken } from './config.js';
import type { MurmurPaths } from './config.js';
import { fullOf } from './core.js';
import type { MurmurCore, PushTarget } from './core.js';
import { nativeSend } from './native.js';
import type { NativeSendResult } from './native.js';
import { ProofWatcher } from './proof.js';
import type { SessionRow, Store } from './store.js';
import { defaultWake } from './wake.js';
import type { WakeFn } from './wake.js';
import {
  LineReader,
  MurmurError,
  NOTIFY_BATCH_MS,
  PROTOCOL_VERSION,
  WAIT_MAX_MS,
  encodeLine,
  renderDelivery,
  replyFooter,
} from './protocol.js';
import type {
  AckResult,
  CliName,
  ClientKind,
  CollectResult,
  DeliveredResult,
  DeliveryMode,
  ErrorCode,
  FetchPushResult,
  GroupAction,
  GroupsResult,
  HookEvent,
  HostRef,
  InboxResult,
  NotificationEvent,
  NotificationPayload,
  PushEvent,
  PruneResult,
  PushPayload,
  ReadResult,
  RecipientMatrix,
  RegisterResult,
  ReplyResult,
  SendResult,
  SessionResult,
  StatusResult,
  ThreadResult,
  Verdict,
  WaitReply,
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
  wake?: WakeFn;
  proofPollMs?: number;
}

interface ConnState {
  socket: Socket;
  decoder: StringDecoder;
  reader: LineReader;
  authenticated: boolean;
  // Handle this connection speaks as, or null. A second register (or
  // session) on one connection rebinds.
  handle: string | null;
  // The session the owner typed in, for an owner register that named its
  // ancestors; stamped on every message this connection sends.
  via: string | null;
  // Long-lived connections (mcp, adapter) count as presence; cli and hook
  // connections only carry an identity for their own requests.
  live: boolean;
  // An adapter that turns push events into user messages in its session.
  listening: boolean;
  // An MCP server whose session has not announced itself yet: bound as soon
  // as a session event arrives for this host, or created on first use.
  pendingHost: { host: HostRef; client: CliName; cwd?: string } | null;
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
  // Ids of replies held back for this wait; the handover returns them even if the latest ask moved on.
  held: Set<number>;
  // timeoutMs 0 asks for the state now; it never waited, so its result carries no timeout note.
  snapshot: boolean;
}

interface NotifyEntry {
  payloads: NotificationPayload[];
  timer: NodeJS.Timeout;
}

const CLIENT_KINDS: readonly ClientKind[] = ['mcp', 'cli', 'hook', 'adapter'];
const HOOK_EVENTS: readonly HookEvent[] = ['session-start', 'prompt', 'stop', 'session-end'];
const CLI_NAMES: readonly CliName[] = ['claude', 'codex', 'gemini', 'zcode', 'opencode', 'hummin', 'other'];
const DELIVERY_MODES: readonly DeliveryMode[] = ['push', 'notify', 'silent'];
const VERDICTS: readonly Verdict[] = ['agree', 'disagree', 'info', 'choice'];
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

function optNumberArray(rec: Record<string, unknown>, key: string): number[] | undefined {
  if (rec[key] === undefined) {
    return undefined;
  }
  return reqNumberArray(rec, key);
}

function optBool(rec: Record<string, unknown>, key: string): boolean | undefined {
  if (rec[key] === undefined) {
    return undefined;
  }
  return reqBool(rec, key);
}

function reqHost(value: unknown): HostRef {
  if (typeof value !== 'object' || value === null) {
    throw badRequest('host must be an object');
  }
  const rec = value as Record<string, unknown>;
  return { hostPid: reqNumber(rec, 'hostPid'), hostStart: reqString(rec, 'hostStart') };
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
  private readonly wakeFn: WakeFn;
  private readonly proof: ProofWatcher;
  // Live bindings: handle to its long-lived connection (mcp or adapter).
  private readonly presence = new Map<string, ConnState>();
  private readonly conns = new Set<ConnState>();
  // Recipient to pending notification payloads, one flush timer per recipient.
  private readonly notifyQueue = new Map<string, NotifyEntry>();
  // Thread id to waiters registered against that thread.
  private readonly waiters = new Map<number, Set<Waiter>>();
  // Deliveries a push is sending right now (messageId:recipient): no other path hands them over.
  private readonly inFlight = new Set<string>();
  private stopped = false;
  private stopPromise: Promise<void> | null = null;

  private constructor(opts: BrokerOptions, server: Server) {
    this.server = server;
    this.paths = opts.paths;
    this.store = opts.store;
    this.core = opts.core;
    this.nativeSendFn = opts.nativeSend ?? nativeSend;
    this.wakeFn = opts.wake ?? defaultWake;
    this.core.hasTools = (handle) => this.presence.has(handle);
    this.proof = new ProofWatcher((messageId, recipient) => this.onProof(messageId, recipient), opts.proofPollMs);
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
    await clearStaleSocket(opts.paths.socketPath);
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
    this.proof.stop();
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
      via: null,
      live: false,
      listening: false,
      pendingHost: null,
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
    const unresolved = conn.waiters.size > 0;
    for (const waiter of [...conn.waiters]) {
      clearTimeout(waiter.timer);
      this.removeWaiter(waiter);
    }
    if (conn.handle !== null && this.presence.get(conn.handle) === conn) {
      this.presence.delete(conn.handle);
    }
    // Replies held for the closed wait have no result to ride on: push them now.
    if (unresolved && conn.handle !== null && !this.stopped) {
      void this.pushQueued(conn.handle).catch(() => {});
    }
    // Stamp nothing on close; only register refreshes last_seen_at. Only a
    // long-lived connection going away means the peer went away.
    if (conn.handle !== null && conn.live && !this.stopped) {
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
      case 'session': {
        const result = await this.handleSession(conn, rec);
        this.replyOk(conn.socket, id, result);
        return;
      }
      case 'delivered':
        this.replyOk(conn.socket, id, this.handleDelivered(conn, rec));
        return;
      case 'native-register':
        this.replyOk(conn.socket, id, this.handleNativeRegister(rec));
        return;
      case 'send': {
        const result = await this.handleSend(conn, rec);
        this.replyOk(conn.socket, id, result);
        return;
      }
      case 'reply': {
        const result = await this.handleReply(conn, rec);
        this.replyOk(conn.socket, id, result);
        return;
      }
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
      case 'prune':
        this.replyOk(conn.socket, id, this.handlePrune());
        return;
      case 'fetch-push':
        this.replyOk(conn.socket, id, this.handleFetchPush(rec));
        return;
      default:
        throw badRequest('unknown message type: ' + String(rec.type));
    }
  }

  // Handlers.

  // See RegisterMessage for the identity rules.
  private handleRegister(conn: ConnState, rec: Record<string, unknown>): RegisterResult {
    const kind = reqEnum<ClientKind>(rec, 'clientKind', CLIENT_KINDS);
    const handle = optString(rec, 'handle');
    const display = optString(rec, 'display');
    const role = optString(rec, 'role');
    const host = rec.host === undefined ? undefined : reqHost(rec.host);
    const ancestors = optNumberArray(rec, 'ancestors');
    const client = optEnum<CliName>(rec, 'client', CLI_NAMES) ?? 'other';
    const cwd = optString(rec, 'cwd');
    const owner = optBool(rec, 'owner') === true;
    const live = kind === 'mcp' || kind === 'adapter';
    let resolved: string;
    conn.via = null;
    if (owner) {
      resolved = this.ownerHandle();
      if (ancestors !== undefined) {
        conn.via = this.core.sessionForAncestors(ancestors)?.handle ?? null;
      }
    } else if (host !== undefined) {
      const session = this.core.sessionForHost(host);
      if (session === null) {
        if (handle !== undefined) {
          throw badRequest('this session has not announced itself yet (no hook has run); name it after its first prompt');
        }
        // No hook announced this host yet: bind when one does, or on first use.
        conn.live = live;
        conn.pendingHost = { host, client, ...(cwd !== undefined ? { cwd } : {}) };
        return { handle: '', owner: false };
      }
      resolved = handle === undefined ? session.handle : this.core.renameSession(session, handle);
      this.rebind(session.handle, resolved);
    } else if (ancestors !== undefined && handle === undefined) {
      resolved = this.core.sessionForAncestors(ancestors)?.handle ?? this.ownerHandle();
    } else if (handle !== undefined) {
      resolved = this.core.register({ handle }).handle;
    } else {
      throw badRequest('register needs a handle, a host, ancestors or owner');
    }
    if (display !== undefined || role !== undefined) {
      const peer = this.store.getPeer(resolved);
      this.store.upsertPeer(resolved, display ?? null, role ?? null, peer?.isOwner === true, Date.now());
    }
    this.bind(conn, resolved, live);
    this.core.touchSeen(resolved);
    return { handle: resolved, owner: this.store.getPeer(resolved)?.isOwner === true };
  }

  private ownerHandle(): string {
    const owner = this.store.listPeers().find((peer) => peer.isOwner);
    if (owner === undefined) {
      throw badRequest('no owner peer; run murmur init');
    }
    return owner.handle;
  }

  private handleHook(conn: ConnState, rec: Record<string, unknown>): null {
    const event = reqEnum<HookEvent>(rec, 'event', HOOK_EVENTS);
    const handle = reqString(rec, 'handle');
    // sessionHint rides on the wire; core.hookState has no use for it.
    optString(rec, 'sessionHint');
    // hookState creates the peer row when the handle is new (ensurePeer).
    this.core.hookState(handle, event);
    this.bind(conn, handle, false);
    return null;
  }

  private async handleSession(conn: ConnState, rec: Record<string, unknown>): Promise<SessionResult> {
    const event = reqEnum<HookEvent>(rec, 'event', HOOK_EVENTS);
    const client = reqEnum<CliName>(rec, 'client', CLI_NAMES);
    const cliSessionId = rec.cliSessionId === null ? null : reqString(rec, 'cliSessionId');
    const host = reqHost(rec.host);
    const transcriptPath = optString(rec, 'transcriptPath');
    const cwd = optString(rec, 'cwd');
    const handle = optString(rec, 'handle');
    const title = optString(rec, 'title');
    const listen = optBool(rec, 'listen') === true;
    let native: { socketPath: string; token: string } | undefined;
    if (rec.native !== undefined) {
      if (typeof rec.native !== 'object' || rec.native === null) {
        throw badRequest('native must be an object');
      }
      const nat = rec.native as Record<string, unknown>;
      native = { socketPath: reqString(nat, 'socketPath'), token: reqString(nat, 'token') };
    }
    // A title can rename the session, so find the name it had before.
    const before = (cliSessionId === null ? null : this.store.sessionByCliId(client, cliSessionId)) ?? this.store.sessionByHost(host.hostPid, host.hostStart);
    const session = this.core.sessionEvent({
      event,
      client,
      cliSessionId,
      host,
      ...(transcriptPath !== undefined ? { transcriptPath } : {}),
      ...(cwd !== undefined ? { cwd } : {}),
      ...(title !== undefined ? { title } : {}),
      ...(handle !== undefined ? { handle } : {}),
      ...(native !== undefined ? { native } : {}),
    });
    if (before !== null) {
      this.rebind(before.handle, session.handle);
    }
    conn.listening = listen;
    this.bind(conn, session.handle, listen);
    // MCP servers of this session that registered before it announced itself.
    for (const other of this.conns) {
      const pending = other.pendingHost;
      if (pending !== null && pending.host.hostPid === host.hostPid && pending.host.hostStart === host.hostStart) {
        this.bind(other, session.handle, other.live);
      }
    }
    if (event === 'session-end') {
      this.notifyPeerOffline(session.handle);
      return { handle: session.handle };
    }
    this.proof.recheck(session.handle);
    if (event === 'session-start' || listen) {
      // A session that comes (back) up gets what waited for it.
      await this.pushQueued(session.handle);
    }
    return { handle: session.handle };
  }

  // An adapter reports that its session accepted these pushed messages.
  private handleDelivered(conn: ConnState, rec: Record<string, unknown>): DeliveredResult {
    const handle = this.requireBoundHandle(conn);
    const ids = reqNumberArray(rec, 'ids');
    const delivered: number[] = [];
    for (const messageId of ids) {
      if (this.markDeliveredHop(messageId, handle)) {
        delivered.push(messageId);
      }
    }
    return { delivered };
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
    const arg: { threadId: number | null; to: string[]; body: string; references?: string[]; mode?: DeliveryMode; via: string | null } = {
      threadId,
      to,
      body,
      via: conn.via,
    };
    if (references !== undefined) {
      arg.references = references;
    }
    if (mode !== undefined) {
      arg.mode = mode;
    }
    const { result, pushes } = this.core.send(sender, arg);
    // Pushes run before the reply so the sender sees the states they left.
    await this.executePushes(pushes);
    result.deliveries = this.core.deliveriesOf(result.messageId);
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

  private async handleReply(conn: ConnState, rec: Record<string, unknown>): Promise<ReplyResult> {
    const sender = this.requireBoundHandle(conn);
    const messageId = reqNumber(rec, 'messageId');
    const body = reqString(rec, 'body');
    const verdict = optEnum<Verdict>(rec, 'verdict', VERDICTS);
    const references = optStringArray(rec, 'references');
    const arg: { messageId: number; body: string; verdict?: Verdict; references?: string[]; via: string | null } = {
      messageId,
      body,
      via: conn.via,
    };
    if (verdict !== undefined) {
      arg.verdict = verdict;
    }
    if (references !== undefined) {
      arg.references = references;
    }
    const { result, pushes } = this.core.reply(sender, arg);
    // The reply itself travels to every participant; notify mode leaves
    // them a small note instead.
    await this.executePushes(pushes);
    const notify = this.store.messageById(result.messageId)?.mode === 'notify';
    for (const target of notify ? pushes : []) {
      this.enqueueNotification(target.recipient, {
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
    const messageId = rec.messageId === undefined ? undefined : reqNumber(rec, 'messageId');
    return this.core.threadView(threadId, sinceSeq, reqBool(rec, 'full'), messageId);
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

  // Every handle with a connection, so a peer someone is talking through is
  // never pruned.
  private handlePrune(): PruneResult {
    const connected = new Set<string>();
    for (const conn of this.conns) {
      if (conn.handle !== null) {
        connected.add(conn.handle);
      }
    }
    return { removed: this.core.prune(connected) };
  }

  private handleFetchPush(rec: Record<string, unknown>): FetchPushResult {
    const handle = reqString(rec, 'handle');
    const limit = reqNumber(rec, 'limit');
    if (!Number.isInteger(limit) || limit < 0) {
      throw badRequest('limit must be a non-negative integer');
    }
    // Everything the pull hands out was queued, so each one tells its
    // sender it finally arrived.
    const result = this.core.pendingPush(handle, limit, (id) => this.inFlight.has(String(id) + ':' + handle));
    for (const message of result.messages) {
      this.enqueueNotification(message.sender, {
        kind: 'queued_delivered',
        threadId: message.threadId,
        fromHandle: handle,
        detail:
          'queued message ' + String(message.id) + ' in thread ' + String(message.threadId) +
          ' was delivered to ' + handle,
      });
      this.checkWaiters(message.threadId);
    }
    return result;
  }

  // Push execution after core.send and core.reply, best transport first:
  // an adapter listening in the recipient's session, then the session's
  // native messaging socket, then the CLI's own command transport. A target
  // no transport takes stays queued and surfaces through the hook pull.
  // sent means a transport accepted it; delivered needs evidence from the
  // receiving side (adapter ack, transcript proof, hook pull, read).
  private async executePushes(pushes: PushTarget[]): Promise<void> {
    for (const target of pushes) {
      if (this.stopped) {
        return;
      }
      if (target.kind === 'queued' || this.heldForWait(target.message, target.recipient)) {
        continue;
      }
      await this.pushOne(target.message.id, target.message.threadId, target.recipient, target.text, {
        message: target.message,
        mode: 'push',
        footer: target.footer,
        text: target.text,
      });
    }
  }

  // A reply answering the thread's latest ask, whose recipient has a
  // murmur_wait open on that thread, stays queued; the wait result carries it
  // (takeReplies applies the same rule). Any other reply is pushed as normal.
  private heldForWait(message: { id: number; isReply: boolean; threadId: number; inReplyTo: number | null }, recipient: string): boolean {
    if (!message.isReply || message.inReplyTo === null || this.store.latestAsk(message.threadId)?.id !== message.inReplyTo) {
      return false;
    }
    let held = false;
    for (const w of this.waiters.get(message.threadId) ?? []) {
      if (w.conn.handle === recipient) {
        w.held.add(message.id);
        held = true;
      }
    }
    return held;
  }

  private async pushOne(
    messageId: number,
    threadId: number,
    recipient: string,
    text: string,
    payload: PushPayload,
  ): Promise<void> {
    const listener = this.presence.get(recipient);
    if (listener !== undefined && listener.listening && !listener.socket.destroyed) {
      const event: PushEvent = { v: PROTOCOL_VERSION, type: 'event', event: 'push', payload };
      this.writeLine(listener.socket, encodeLine(event));
      this.markSent(messageId, threadId, recipient);
      return;
    }
    const key = String(messageId) + ':' + recipient;
    this.inFlight.add(key);
    try {
      await this.pushVia(messageId, threadId, recipient, text);
    } finally {
      this.inFlight.delete(key);
    }
  }

  private async pushVia(messageId: number, threadId: number, recipient: string, text: string): Promise<void> {
    const session = this.liveSessionOf(recipient);
    const transcript = session?.transcriptPath ?? null;
    const offset = transcript === null ? 0 : ProofWatcher.offsetOf(transcript);
    let accepted = false;
    const transport = this.store.getNativeTransport(recipient);
    if (transport !== null) {
      try {
        accepted = (await this.nativeSendFn(transport.socketPath, transport.token, text)).ok;
      } catch {
        // A throwing transport counts as a failed send: leave queued.
      }
    } else if (session !== null) {
      accepted = await this.wakeFn(session, text).catch(() => false);
    }
    if (!accepted || this.stopped) {
      return;
    }
    this.markSent(messageId, threadId, recipient);
    if (transcript !== null) {
      this.proof.watch(messageId, recipient, transcript, offset);
    }
  }

  // Pushes every queued push-mode message waiting for this handle, oldest
  // first, through whatever transport it has now.
  private async pushQueued(handle: string): Promise<void> {
    const waiting = this.store.pendingPushFor(handle, 50).reverse();
    for (const { message } of waiting) {
      if (this.heldForWait(message, handle)) {
        continue;
      }
      const recipients = this.store.getDeliveries(message.id).map((d) => d.recipient);
      const full = fullOf(message);
      const shell = !this.presence.has(handle);
      const text = renderDelivery(full, message.deliveredBody, recipients, shell);
      await this.pushOne(message.id, message.threadId, handle, text, { message: full, mode: 'push', footer: replyFooter(full, shell), text });
    }
  }

  private liveSessionOf(handle: string): SessionRow | null {
    return this.store.sessionsForHandle(handle).find((row) => row.state !== 'ended') ?? null;
  }

  private markSent(messageId: number, threadId: number, recipient: string): void {
    const delivery = this.store.getDeliveries(messageId).find((d) => d.recipient === recipient);
    if (delivery === undefined || delivery.state !== 'queued') {
      return;
    }
    this.store.updateDelivery(messageId, recipient, { state: 'sent', sentAt: Date.now() });
    this.checkWaiters(threadId);
  }

  private onProof(messageId: number, recipient: string): void {
    if (!this.stopped) {
      this.markDeliveredHop(messageId, recipient);
    }
  }

  // queued or sent to delivered, with the queued_delivered notice to the
  // sender when the message had been waiting. False when not applicable.
  private markDeliveredHop(messageId: number, recipient: string, notifySender = true): boolean {
    const message = this.store.messageById(messageId);
    const delivery = this.store.getDeliveries(messageId).find((d) => d.recipient === recipient);
    if (message === null || delivery === undefined || (delivery.state !== 'queued' && delivery.state !== 'sent')) {
      return false;
    }
    this.store.markDelivered(messageId, recipient, Date.now());
    if (delivery.state === 'queued' && notifySender) {
      this.enqueueNotification(message.sender, {
        kind: 'queued_delivered',
        threadId: message.threadId,
        fromHandle: recipient,
        detail: 'queued message ' + String(messageId) + ' in thread ' + String(message.threadId) + ' was delivered to ' + recipient,
      });
    }
    this.checkWaiters(message.threadId);
    return true;
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
    if (peer !== undefined && peer.listening && !peer.socket.destroyed) {
      this.writeLine(peer.socket, encodeLine(event));
      return;
    }
    // No listening adapter: fall back to the native transport as plain text
    // when one exists, otherwise drop the batch (the data stays in the DB).
    const transport = this.store.getNativeTransport(recipient);
    if (transport === null) {
      return;
    }
    const text = '[murmur notice] ' + entry.payloads.map((p) => p.detail).join('; ');
    this.nativeSendFn(transport.socketPath, transport.token, text).catch(() => {
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
        replies: this.takeReplies(threadId, conn.handle),
      });
      return;
    }
    const waiter: Waiter = {
      conn,
      id,
      threadId,
      until,
      held: new Set<number>(),
      snapshot: timeoutMs === 0,
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

  // The replies still queued for this handle in the thread, handed over in the
  // wait result and marked delivered (the wait result is the receiving-side proof).
  private takeReplies(threadId: number, handle: string | null, held?: Set<number>): WaitReply[] {
    const replies: WaitReply[] = [];
    const ask = this.store.latestAsk(threadId);
    for (const row of this.store.threadMessages(threadId, null)) {
      const delivery = this.store.getDeliveries(row.id).find((d) => d.recipient === handle);
      if (((ask !== null && row.inReplyTo === ask.id) || held?.has(row.id) === true) && delivery?.state === 'queued' && !this.inFlight.has(String(row.id) + ':' + delivery.recipient)) {
        // A reply held for this wait never waited on a session that was down:
        // no queued_delivered notice, which would wake the replier for nothing.
        this.markDeliveredHop(row.id, delivery.recipient, held?.has(row.id) !== true);
        // The wait result is the asker's reading: seen, not unread.
        this.store.markAcked(row.id, delivery.recipient, Date.now());
        replies.push({ id: row.id, seq: row.seq, sender: row.sender, verdict: row.verdict as Verdict | null, body: row.deliveredBody });
      }
    }
    return replies;
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
      // A handover below re-enters checkWaiters and may already have answered this waiter.
      if (!set.has(waiter) || !this.conditionHolds(threadId, waiter.until)) {
        continue;
      }
      this.removeWaiter(waiter);
      clearTimeout(waiter.timer);
      this.replyOk(waiter.conn.socket, waiter.id, {
        threadId,
        conditionMet: true,
        timedOut: false,
        matrix: this.matrixFor(threadId),
        replies: this.takeReplies(threadId, waiter.conn.handle, waiter.held),
      });
    }
  }

  private resolveTimedOut(waiter: Waiter): void {
    this.removeWaiter(waiter);
    if (this.stopped) {
      return;
    }
    try {
      const matrix = this.matrixFor(waiter.threadId);
      const note = waiter.snapshot ? null : timeoutNote(matrix);
      this.replyOk(waiter.conn.socket, waiter.id, {
        threadId: waiter.threadId,
        conditionMet: false,
        timedOut: true,
        matrix,
        ...(note === null ? {} : { note }),
        replies: this.takeReplies(waiter.threadId, waiter.conn.handle, waiter.held),
      });
    } catch {
      // Never let a timer callback crash the process.
    }
  }

  // Only called for a registered waiter: checkWaiters checks set.has, a close
  // walks conn.waiters, and a timeout fires only while the timer was never cleared.
  private removeWaiter(waiter: Waiter): void {
    const set = this.waiters.get(waiter.threadId) as Set<Waiter>;
    set.delete(waiter);
    if (set.size === 0) {
      this.waiters.delete(waiter.threadId);
    }
    waiter.conn.waiters.delete(waiter);
  }

  // Presence and replies.

  // Sets the connection's identity. Only long-lived connections take the
  // presence slot, so a short CLI or hook connection never unseats a live
  // session's MCP server or adapter.
  private bind(conn: ConnState, handle: string, live: boolean): void {
    if (conn.handle !== null && this.presence.get(conn.handle) === conn) {
      this.presence.delete(conn.handle);
    }
    conn.handle = handle;
    conn.live = live;
    conn.pendingHost = null;
    if (live) {
      this.presence.set(handle, conn);
    }
  }

  // A session's name changed: connections still bound to the old one (its MCP
  // server, an adapter) follow it, unless another live session still holds
  // it. Ended sessions left on the old name do not count.
  private rebind(from: string, to: string): void {
    if (from === to || !this.core.claimable(from, null)) {
      return;
    }
    for (const conn of this.conns) {
      if (conn.handle === from) {
        this.bind(conn, to, conn.live);
      }
    }
    // A whole-identity rename moved the deliveries; pending proof and
    // batched notifications follow them.
    if (this.store.getPeer(from) === null) {
      this.proof.rename(from, to);
      const entry = this.notifyQueue.get(from);
      if (entry !== undefined) {
        clearTimeout(entry.timer);
        this.notifyQueue.delete(from);
        for (const payload of entry.payloads) {
          this.enqueueNotification(to, payload);
        }
      }
    }
  }

  private requireBoundHandle(conn: ConnState): string {
    if (conn.handle === null && conn.pendingHost !== null) {
      const { host, client, cwd } = conn.pendingHost;
      const session = this.core.sessionForHost(host, { client, ...(cwd !== undefined ? { cwd } : {}) }) as SessionRow;
      this.bind(conn, session.handle, conn.live);
    }
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

// A socket file left by a broker that died without cleanup blocks listen
// with EADDRINUSE. Remove it only when the probe proves nothing listens there
// (refused, or not a socket at all: ENOTSOCK on macOS, ECONNREFUSED on Linux).
// Refuse when a live broker answers or when the probe says nothing, such as a
// CLI sandbox denying Unix sockets: removing the file would cut a live broker
// off from every new client.
const STALE_PROBE_CODES = new Set(['ECONNREFUSED', 'ENOTSOCK']);

async function clearStaleSocket(socketPath: string): Promise<void> {
  if (!existsSync(socketPath)) {
    return;
  }
  const probeError = await new Promise<NodeJS.ErrnoException | null>((resolve) => {
    const probe = createConnection(socketPath);
    probe.once('connect', () => {
      probe.destroy();
      resolve(null);
    });
    probe.once('error', (err: NodeJS.ErrnoException) => resolve(err));
  });
  if (probeError === null) {
    throw new Error('murmur: a broker is already listening on ' + socketPath);
  }
  if (!STALE_PROBE_CODES.has(String(probeError.code))) {
    throw new Error('murmur: cannot probe ' + socketPath + ' (' + String(probeError.code) + '); not removing it');
  }
  unlinkSync(socketPath);
}

// What a timed-out asker is told: who has the message but is silent, whose receipt
// is unproven (queued or sent), and not to start work that could overlap theirs.
// Null when everyone replied.
function timeoutNote(matrix: RecipientMatrix[]): string | null {
  const names = (states: string[]): string[] => matrix.filter((m) => states.includes(m.state)).map((m) => m.recipient);
  const silent = names(['delivered', 'acknowledged']);
  const unreached = names(['queued', 'sent']);
  const clauses: string[] = [];
  if (silent.length > 0) {
    const v = silent.length > 1 ? 'have' : 'has';
    clauses.push(`${silent.join(', ')} ${v} the message but ${v} not replied`);
  }
  if (unreached.length > 0) {
    clauses.push(`${unreached.join(', ')} ${unreached.length > 1 ? 'have' : 'has'} not confirmed receipt`);
  }
  if (clauses.length === 0) {
    return null;
  }
  return `timed out: ${clauses.join('; ')}. Do not start work that could overlap theirs: ask the user, or murmur_wait again.`;
}
