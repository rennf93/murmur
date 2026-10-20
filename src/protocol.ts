import { randomBytes } from 'node:crypto';

// Wire protocol constants. Newline-delimited JSON over a Unix domain socket.
export const PROTOCOL_VERSION = 1;
export const MAX_BODY_CHARS = 4000;
export const RATE_WINDOW_MS = 60000;
export const RATE_MAX_SENDS = 20;
export const DUP_WINDOW_MS = 60000;
export const THREAD_CAP = 500;
export const UNREAD_CAP = 200;
export const NOTIFY_BATCH_MS = 60000;
export const WAIT_MAX_MS = 300000;
export const WAIT_DEFAULT_MS = 120000;
export const FIRST_LINE_MAX = 120;

export type DeliveryState = 'queued' | 'sent' | 'delivered' | 'acknowledged' | 'replied';
export type Verdict = 'agree' | 'disagree' | 'info';
export type DeliveryMode = 'push' | 'notify' | 'silent';
export type PresenceKind = 'online' | 'busy' | 'idle' | 'offline' | 'presumed-offline';
export type ClientKind = 'mcp' | 'cli' | 'hook';
export type HookEvent = 'session-start' | 'prompt' | 'stop';
export type GroupAction = 'create' | 'list' | 'add' | 'remove' | 'delete';
export type WaitUntil = 'all_replied' | 'any_replied';

export type ErrorCode =
  | 'auth_required'
  | 'bad_token'
  | 'broker_closed'
  | 'broker_down'
  | 'unknown_handle'
  | 'unknown_group'
  | 'unknown_thread'
  | 'unknown_message'
  | 'duplicate'
  | 'rate_limited'
  | 'cap_thread'
  | 'cap_inbox'
  | 'bad_request'
  | 'timeout'
  | 'native_failed'
  | 'internal';

export interface MessageSummary {
  id: number;
  threadId: number;
  seq: number;
  sender: string;
  firstLine: string;
  truncated: boolean;
  references: string[];
  isReply: boolean;
  inReplyTo: number | null;
  verdict: Verdict | null;
  createdAt: number;
}

export interface MessageFull extends MessageSummary {
  body: string;
}

export interface DeliveryInfo {
  recipient: string;
  state: DeliveryState;
  sentAt: number | null;
  deliveredAt: number | null;
  ackedAt: number | null;
  repliedAt: number | null;
  latencyMs: number | null;
}

export interface SendResult {
  messageId: number;
  threadId: number;
  seq: number;
  deliveries: DeliveryInfo[];
}

export interface ReplyResult {
  messageId: number;
  threadId: number;
  seq: number;
}

export interface InboxThread {
  threadId: number;
  latestSeq: number;
  unread: number;
  messages: MessageSummary[];
}

export interface InboxResult {
  handle: string;
  unreadTotal: number;
  threads: InboxThread[];
}

export interface ThreadResult {
  threadId: number;
  latestSeq: number;
  messages: MessageFull[];
}

export interface RecipientMatrix {
  recipient: string;
  state: DeliveryState;
  verdict: Verdict | null;
  repliedLatencyMs: number | null;
  p50ReplyMs: number | null;
  p95ReplyMs: number | null;
}

export interface CollectResult {
  threadId: number;
  askMessageId: number;
  recipients: RecipientMatrix[];
  verdictTally: Record<Verdict, number>;
}

export interface PeerStatus {
  handle: string;
  display: string | null;
  role: string | null;
  isOwner: boolean;
  presence: PresenceKind;
  busySince: number | null;
  idleSince: number | null;
  lastActivityAgeMs: number | null;
  unread: number;
}

export interface StatusResult {
  now: number;
  peers: PeerStatus[];
}

export interface GroupsResult {
  name: string;
  members: string[];
  createdAt: number;
}

export interface ReadResult {
  messages: MessageFull[];
}

export interface AckResult {
  acknowledged: number[];
}

export interface FetchPushResult {
  messages: MessageFull[];
}

export interface RegisterResult {
  handle: string;
  owner: boolean;
}

export interface WaitResult {
  threadId: number;
  conditionMet: boolean;
  timedOut: boolean;
  matrix: RecipientMatrix[];
}

export interface PushPayload {
  message: MessageFull;
  mode: DeliveryMode;
  footer: string;
}

export interface NotificationPayload {
  kind: 'reply_arrived' | 'queued_delivered' | 'peer_offline';
  threadId: number | null;
  fromHandle: string | null;
  detail: string;
}

// Client messages. Every member carries the protocol version and a
// per-connection monotonic request id. The first line on any connection
// must be auth, before anything else.
export interface AuthMessage {
  v: number;
  id: number;
  type: 'auth';
  token: string;
}

export interface RegisterMessage {
  v: number;
  id: number;
  type: 'register';
  handle: string;
  display?: string;
  role?: string;
  clientKind: ClientKind;
}

export interface SendMessage {
  v: number;
  id: number;
  type: 'send';
  threadId: number | null;
  to: string[];
  body: string;
  references?: string[];
  mode?: DeliveryMode;
}

export interface ReplyMessage {
  v: number;
  id: number;
  type: 'reply';
  messageId: number;
  body: string;
  verdict?: Verdict;
  references?: string[];
}

export interface AckMessage {
  v: number;
  id: number;
  type: 'ack';
  ids: number[];
}

export interface ReadMessage {
  v: number;
  id: number;
  type: 'read';
  ids: number[];
}

export interface InboxMessage {
  v: number;
  id: number;
  type: 'inbox';
  unreadOnly: boolean;
}

export interface ThreadMessage {
  v: number;
  id: number;
  type: 'thread';
  threadId: number;
  sinceSeq: number | null;
  full: boolean;
}

export interface CollectMessage {
  v: number;
  id: number;
  type: 'collect';
  threadId: number;
}

export interface WaitMessage {
  v: number;
  id: number;
  type: 'wait';
  threadId: number;
  until: WaitUntil;
  timeoutMs: number;
}

export interface GroupsMessage {
  v: number;
  id: number;
  type: 'groups';
  action: GroupAction;
  name?: string;
  members?: string[];
}

export interface StatusMessage {
  v: number;
  id: number;
  type: 'status';
}

export interface HookMessage {
  v: number;
  id: number;
  type: 'hook';
  event: HookEvent;
  handle?: string;
  sessionHint?: string;
}

export interface NativeRegisterMessage {
  v: number;
  id: number;
  type: 'native-register';
  handle: string;
  socketPath: string;
  token: string;
}

export interface FetchPushMessage {
  v: number;
  id: number;
  type: 'fetch-push';
  handle: string;
  limit: number;
}

export type ClientMessage =
  | AuthMessage
  | RegisterMessage
  | SendMessage
  | ReplyMessage
  | AckMessage
  | ReadMessage
  | InboxMessage
  | ThreadMessage
  | CollectMessage
  | WaitMessage
  | GroupsMessage
  | StatusMessage
  | HookMessage
  | NativeRegisterMessage
  | FetchPushMessage;

export interface OkReply {
  v: number;
  id: number;
  ok: true;
  result: unknown;
}

export interface ErrReply {
  v: number;
  id: number;
  ok: false;
  error: {
    code: ErrorCode;
    message: string;
  };
}

export type ServerReply = OkReply | ErrReply;

export interface PushEvent {
  v: number;
  type: 'event';
  event: 'push';
  payload: PushPayload;
}

export interface NotificationEvent {
  v: number;
  type: 'event';
  event: 'notification';
  payload: NotificationPayload[];
}

export type ServerEvent = PushEvent | NotificationEvent;

export class MurmurError extends Error {
  readonly code: ErrorCode;

  constructor(code: ErrorCode, message: string) {
    super(message);
    this.name = 'MurmurError';
    this.code = code;
  }
}

export function encodeLine(msg: unknown): string {
  return JSON.stringify(msg) + '\n';
}

export class LineReader {
  private buffer = '';

  // Buffers text across calls and returns every complete line that has
  // arrived so far. Lines are stripped of surrounding whitespace and empty
  // lines are dropped.
  feed(chunk: string): string[] {
    this.buffer += chunk;
    const lines: string[] = [];
    let idx = this.buffer.indexOf('\n');
    while (idx !== -1) {
      const line = this.buffer.slice(0, idx).trim();
      this.buffer = this.buffer.slice(idx + 1);
      if (line.length > 0) {
        lines.push(line);
      }
      idx = this.buffer.indexOf('\n');
    }
    return lines;
  }
}

export function firstLine(body: string): string {
  const nl = body.indexOf('\n');
  const line = (nl === -1 ? body : body.slice(0, nl)).trim();
  if (line.length > FIRST_LINE_MAX) {
    return line.slice(0, FIRST_LINE_MAX);
  }
  return line;
}

export function truncateForDelivery(body: string): { delivered: string; truncated: boolean } {
  if (body.length <= MAX_BODY_CHARS) {
    return { delivered: body, truncated: false };
  }
  return {
    delivered: body.slice(0, MAX_BODY_CHARS) + '\n[truncated: full body stored, read with murmur read]',
    truncated: true,
  };
}

export function newToken(): string {
  return randomBytes(32).toString('hex');
}
