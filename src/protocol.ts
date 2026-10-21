import { randomBytes } from 'node:crypto';

// Wire protocol constants. Newline-delimited JSON over a Unix domain socket.
export const PROTOCOL_VERSION = 2;
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
export const HANDLE_RE = /^[a-z0-9][a-z0-9-]{1,47}$/;
// How long the broker keeps polling a receiver's transcript for proof after
// a push; later hook events of that session re-check without a deadline.
export const PROOF_POLL_MS = 120000;

export type DeliveryState = 'queued' | 'sent' | 'delivered' | 'acknowledged' | 'replied';
export type Verdict = 'agree' | 'disagree' | 'info' | 'choice';
export type DeliveryMode = 'push' | 'notify' | 'silent';
export type PresenceKind = 'online' | 'busy' | 'idle' | 'offline' | 'presumed-offline';
export type ClientKind = 'mcp' | 'cli' | 'hook' | 'adapter';
export type CliName = 'claude' | 'codex' | 'gemini' | 'zcode' | 'opencode' | 'hummin' | 'other';
export type HookEvent = 'session-start' | 'prompt' | 'stop' | 'session-end';
export type GroupAction = 'create' | 'list' | 'add' | 'remove' | 'delete';
export type WaitUntil = 'all_replied' | 'any_replied';

export type ErrorCode =
  | 'auth_required'
  | 'bad_token'
  | 'broker_closed'
  | 'broker_down'
  | 'socket_denied'
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
  // The session the owner typed this in (a /murmur message), else null.
  via: string | null;
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
  via: string | null;
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

// Digest by default: summaries only. full: true adds every body.
export interface ThreadResult {
  threadId: number;
  latestSeq: number;
  messages: Array<MessageSummary & { body?: string }>;
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
  client: CliName | null;
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
  // The rendered delivery text per message, same order.
  texts?: string[];
}

export interface RegisterResult {
  handle: string;
  owner: boolean;
}

export interface PruneResult {
  removed: string[];
}

export interface SessionResult {
  handle: string;
}

export interface DeliveredResult {
  delivered: number[];
}

// A reply held back from push while its recipient waited, handed over in the
// wait result instead. body is the delivered body (with the truncation pointer).
export interface WaitReply {
  id: number;
  seq: number;
  sender: string;
  verdict: Verdict | null;
  body: string;
}

export interface WaitResult {
  threadId: number;
  conditionMet: boolean;
  timedOut: boolean;
  matrix: RecipientMatrix[];
  replies: WaitReply[];
  // Only on a timed-out wait with a recipient still unreplied: who to wait for and what not to do.
  note?: string;
}

export interface PushPayload {
  message: MessageFull;
  mode: DeliveryMode;
  footer: string;
  // The exact text a session should see: tag line, body, reply footer. The
  // tag is what delivery proof searches for in the receiver's transcript.
  text: string;
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

// The agent CLI process a murmur client runs under (see proc.ts). pid plus
// start time, so a recycled pid never matches.
export interface HostRef {
  hostPid: number;
  hostStart: string;
}

// Identity, three ways. handle alone: that exact handle (scripts, --as,
// tests). host: the session running under that host process, created on the
// spot when no hook announced it yet; a handle given with host renames that
// session. ancestors (CLI): the nearest live session host among them, else
// the owner. owner: true forces the owner (the human surface); with ancestors
// it also records the live session among them as the via of its sends.
export interface RegisterMessage {
  v: number;
  id: number;
  type: 'register';
  handle?: string;
  display?: string;
  role?: string;
  clientKind: ClientKind;
  host?: HostRef;
  ancestors?: number[];
  client?: CliName;
  cwd?: string;
  owner?: boolean;
}

// Sent by hooks (one per lifecycle event) and by in-process adapters. Finds
// or creates the session, keeps its handle across resume and clear, and
// binds the connection to that handle. listen: push events for the handle
// arrive on this connection (adapters only).
export interface SessionMessage {
  v: number;
  id: number;
  type: 'session';
  event: HookEvent;
  client: CliName;
  cliSessionId: string | null;
  host: HostRef;
  transcriptPath?: string;
  // The name the user gave the session in its CLI (Claude's /rename).
  title?: string;
  cwd?: string;
  handle?: string;
  native?: { socketPath: string; token: string };
  listen?: boolean;
}

// An adapter confirms its host session accepted these pushed messages.
export interface DeliveredMessage {
  v: number;
  id: number;
  type: 'delivered';
  ids: number[];
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
  // Only this message and the replies to it, full delivered bodies.
  messageId?: number;
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

export interface PruneMessage {
  v: number;
  id: number;
  type: 'prune';
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
  | SessionMessage
  | DeliveredMessage
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
  | PruneMessage
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
    delivered: body.slice(0, MAX_BODY_CHARS) + '\n[truncated: full body stored, read it with murmur_thread full: true]',
    truncated: true,
  };
}

export function newToken(): string {
  return randomBytes(32).toString('hex');
}

// The search key for delivery proof. The trailing space keeps m42 from
// matching m420.
export function deliveryTag(messageId: number): string {
  return '[murmur m' + String(messageId) + ' ';
}

export function replyFooter(m: { id: number; threadId: number }): string {
  return (
    '(answer with murmur_reply messageId ' + String(m.id) + ', plain text never reaches the sender; from a shell: murmur reply ' +
    String(m.id) + ' <text>; its replies: murmur_thread ' + String(m.threadId) + ' messageId ' + String(m.id) + ')'
  );
}

// What a session sees for one message, on every transport: a tag line with
// ids, sender and recipients, the (possibly truncated) body, references, and
// how to reply.
export function renderDelivery(m: MessageSummary, deliveredBody: string, recipients: string[]): string {
  let head = deliveryTag(m.id) + 't' + String(m.threadId) + ' s' + String(m.seq) + ' from ' + m.sender;
  if (m.via !== null) {
    head += ' via ' + m.via;
  }
  if (m.inReplyTo !== null) {
    head += ' re m' + String(m.inReplyTo);
  }
  if (m.verdict !== null) {
    head += ' ' + m.verdict.toUpperCase();
  }
  head += ' to ' + recipients.join(', ') + ']';
  const refs = m.references.length > 0 ? '\nrefs: ' + m.references.join(', ') : '';
  return head + '\n' + deliveredBody + refs + '\n' + replyFooter(m);
}
