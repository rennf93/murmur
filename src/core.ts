import { Store } from './store.js';
import type { DeliveryRow, MessageRow, PeerRow } from './store.js';
import {
  DUP_WINDOW_MS,
  MurmurError,
  RATE_MAX_SENDS,
  RATE_WINDOW_MS,
  THREAD_CAP,
  UNREAD_CAP,
  firstLine,
  truncateForDelivery,
} from './protocol.js';
import type {
  CollectResult,
  DeliveryInfo,
  DeliveryMode,
  DeliveryState,
  FetchPushResult,
  GroupAction,
  GroupsResult,
  HookEvent,
  InboxResult,
  InboxThread,
  MessageFull,
  MessageSummary,
  PeerStatus,
  PresenceKind,
  RecipientMatrix,
  RegisterResult,
  ReplyResult,
  SendResult,
  StatusResult,
  ThreadResult,
  Verdict,
} from './protocol.js';

// The service layer owns every policy decision: recipient resolution, the
// rate, duplicate and cap limits, delivery state transitions, ask collection
// with reply latency percentiles, presence, and groups. It owns no sockets;
// the broker drives it and relays the returned push targets.

export interface CoreEvents {
  onAppend(arg: { threadId: number; messageId: number; isReply: boolean; sender: string }): void;
  onStateChange(arg: { threadId: number; recipient: string; from: string; to: string }): void;
}

export interface PushTarget {
  recipient: string;
  kind: 'native' | 'connection' | 'queued';
  message: MessageFull;
  footer: string;
}

export const HANDLE_RE = /^[a-z0-9][a-z0-9-]{1,31}$/;
const GROUP_NAME_RE = /^@?[a-z0-9][a-z0-9-]{1,31}$/;
const VERDICTS: readonly string[] = ['agree', 'disagree', 'info'];
const UNREAD_STATES: string[] = ['queued', 'sent', 'delivered'];
const OFFLINE_AFTER_MS = 5 * 60 * 1000;

// Linear interpolation over an ascending list. Null for an empty list.
// fraction 0.5 gives the median, 0.95 the p95.
export function percentile(sortedValues: number[], fraction: number): number | null {
  if (sortedValues.length === 0) {
    return null;
  }
  const pos = (sortedValues.length - 1) * fraction;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  const loValue = sortedValues[lo];
  const hiValue = sortedValues[hi];
  if (loValue === undefined || hiValue === undefined) {
    return null;
  }
  if (lo === hi) {
    return loValue;
  }
  return loValue + (hiValue - loValue) * (pos - lo);
}

function parseRefs(json: string): string[] {
  try {
    const parsed: unknown = JSON.parse(json);
    if (Array.isArray(parsed)) {
      return parsed.filter((v): v is string => typeof v === 'string');
    }
  } catch {
    // The store always writes valid JSON; unparsable refs degrade to none.
  }
  return [];
}

function parseVerdict(value: string | null): Verdict | null {
  if (value === 'agree' || value === 'disagree' || value === 'info') {
    return value;
  }
  return null;
}

function normalizeHookState(value: string | null): 'busy' | 'idle' | null {
  if (value === 'busy' || value === 'idle') {
    return value;
  }
  return null;
}

function freshestOf(a: number | null, b: number | null): number | null {
  if (a === null) {
    return b;
  }
  if (b === null) {
    return a;
  }
  return Math.max(a, b);
}

function stripGroupPrefix(name: string): string {
  return name.startsWith('@') ? name.slice(1) : name;
}

function summaryOf(row: MessageRow): MessageSummary {
  return {
    id: row.id,
    threadId: row.threadId,
    seq: row.seq,
    sender: row.sender,
    firstLine: firstLine(row.body),
    truncated: row.truncated,
    references: parseRefs(row.refsJson),
    isReply: row.isReply,
    inReplyTo: row.inReplyTo,
    verdict: parseVerdict(row.verdict),
    createdAt: row.createdAt,
  };
}

function fullOf(row: MessageRow): MessageFull {
  return { ...summaryOf(row), body: row.body };
}

function deliveryInfoOf(row: DeliveryRow): DeliveryInfo {
  return {
    recipient: row.recipient,
    state: row.state as DeliveryState,
    sentAt: row.sentAt,
    deliveredAt: row.deliveredAt,
    ackedAt: row.ackedAt,
    repliedAt: row.repliedAt,
    latencyMs: row.latencyMs,
  };
}

export class MurmurCore {
  private readonly store: Store;
  private readonly events: CoreEvents;
  private readonly nowFn: () => number;
  // Sliding send-rate window per sender and duplicate fingerprints. Both are
  // in-memory by design: limits reset when the broker restarts.
  private readonly sendTimes = new Map<string, number[]>();
  private readonly dupTimes = new Map<string, number[]>();
  // Current hook presence. The peers table columns hook_state and
  // last_hook_activity_at have no writer method in Store, so hook events are
  // tracked here for the process lifetime and status() falls back to the
  // persisted row values when no override exists.
  private readonly hookOverrides = new Map<string, { state: 'busy' | 'idle'; at: number }>();

  constructor(store: Store, events: CoreEvents, now?: () => number) {
    this.store = store;
    this.events = events;
    this.nowFn = now ?? Date.now;
  }

  private nowMs(): number {
    return this.nowFn();
  }

  register(arg: { handle: string; display?: string; role?: string; existing?: string }): RegisterResult {
    if (!HANDLE_RE.test(arg.handle)) {
      throw new MurmurError('bad_request', 'invalid handle: ' + arg.handle);
    }
    if (arg.existing !== undefined && arg.existing !== arg.handle) {
      if (this.store.getPeer(arg.existing) === null) {
        throw new MurmurError('unknown_handle', 'unknown handle: ' + arg.existing);
      }
      try {
        this.store.renamePeer(arg.existing, arg.handle);
      } catch (err) {
        // renamePeer rejects a target handle that is already occupied.
        throw new MurmurError('bad_request', err instanceof Error ? err.message : 'rename failed');
      }
    }
    // Owner is never granted here; owner rows are only created by init.
    this.store.upsertPeer(arg.handle, arg.display ?? null, arg.role ?? null, false, this.nowMs());
    return { handle: arg.handle, owner: false };
  }

  // Expands group entries (leading @) to their members, validates every
  // handle, dedups while preserving order, and rejects an empty result.
  resolveRecipients(to: string[]): string[] {
    const out: string[] = [];
    const seen = new Set<string>();
    const add = (handle: string): void => {
      if (this.store.getPeer(handle) === null) {
        throw new MurmurError('unknown_handle', 'unknown handle: ' + handle);
      }
      if (!seen.has(handle)) {
        seen.add(handle);
        out.push(handle);
      }
    };
    for (const entry of to) {
      if (!entry.startsWith('@')) {
        add(entry);
        continue;
      }
      const stripped = stripGroupPrefix(entry);
      const group = this.store.listGroups().find((g) => g.name === stripped);
      if (group === undefined) {
        throw new MurmurError('unknown_group', 'unknown group: ' + stripped);
      }
      for (const member of group.members) {
        add(member);
      }
    }
    if (out.length === 0) {
      throw new MurmurError('bad_request', 'no recipients resolved');
    }
    return out;
  }

  // Rate, duplicate and cap checks for one intended send. A send that passes
  // every check consumes one rate slot and one duplicate fingerprint; a send
  // rejected by any check consumes none.
  checkLimits(sender: string, body: string, recipients: string[], threadId: number | null): void {
    const ts = this.nowMs();
    const times = this.sendTimes.get(sender) ?? [];
    const recent = times.filter((t) => ts - t < RATE_WINDOW_MS);
    if (recent.length >= RATE_MAX_SENDS) {
      throw new MurmurError(
        'rate_limited',
        'sender ' + sender + ' already sent ' + String(recent.length) + ' messages within ' + String(RATE_WINDOW_MS) + 'ms',
      );
    }
    const fingerprint = sender + '\u0000' + body + '\u0000' + JSON.stringify(recipients);
    const dups = this.dupTimes.get(fingerprint) ?? [];
    const freshDups = dups.filter((t) => ts - t < DUP_WINDOW_MS);
    if (freshDups.length > 0) {
      throw new MurmurError(
        'duplicate',
        'identical message from ' + sender + ' already sent within ' + String(DUP_WINDOW_MS) + 'ms',
      );
    }
    if (threadId !== null && this.store.messageCount(threadId) >= THREAD_CAP) {
      throw new MurmurError('cap_thread', 'thread ' + String(threadId) + ' already holds ' + String(THREAD_CAP) + ' messages');
    }
    for (const recipient of recipients) {
      const unread = this.store.deliveryStatesForRecipient(recipient, UNREAD_STATES);
      if (unread >= UNREAD_CAP) {
        throw new MurmurError('cap_inbox', 'recipient ' + recipient + ' already has ' + String(unread) + ' unread deliveries');
      }
    }
    recent.push(ts);
    this.sendTimes.set(sender, recent);
    freshDups.push(ts);
    this.dupTimes.set(fingerprint, freshDups);
  }

  send(
    sender: string,
    arg: { threadId: number | null; to: string[]; body: string; references?: string[]; mode?: DeliveryMode },
  ): { result: SendResult; pushes: PushTarget[] } {
    const mode: DeliveryMode = arg.mode ?? 'push';
    const recipients = this.resolveRecipients(arg.to);
    this.checkLimits(sender, arg.body, recipients, arg.threadId);
    const ts = this.nowMs();
    let threadId: number;
    if (arg.threadId === null) {
      threadId = this.store.createThread(sender, ts);
    } else {
      if (this.store.messageCount(arg.threadId) === 0) {
        throw new MurmurError('unknown_thread', 'unknown thread: ' + String(arg.threadId));
      }
      threadId = arg.threadId;
    }
    const truncation = truncateForDelivery(arg.body);
    const appended = this.store.appendMessage({
      threadId,
      sender,
      body: arg.body,
      deliveredBody: truncation.delivered,
      truncated: truncation.truncated,
      refs: arg.references ?? [],
      mode,
      isReply: false,
      inReplyTo: null,
      verdict: null,
      createdAt: ts,
    });
    this.store.addDeliveries(appended.id, recipients, ts);
    this.events.onAppend({ threadId, messageId: appended.id, isReply: false, sender });
    const row = this.store.messageById(appended.id);
    if (row === null) {
      throw new MurmurError('internal', 'appended message vanished: ' + String(appended.id));
    }
    const deliveries = this.store.getDeliveries(appended.id).map(deliveryInfoOf);
    const message = fullOf(row);
    const footer =
      'thread ' + String(threadId) + ' seq ' + String(appended.seq) +
      ': reply with murmur_reply in_reply_to ' + String(appended.id) +
      '. The full thread is readable with murmur_thread ' + String(threadId) + '.';
    const pushes: PushTarget[] = recipients.map((recipient) => ({
      recipient,
      kind: this.pushKindFor(recipient, mode),
      message,
      footer,
    }));
    return { result: { messageId: appended.id, threadId, seq: appended.seq, deliveries }, pushes };
  }

  // Native transport row means the broker can hand the message to the
  // recipient's own messaging socket; otherwise a push still travels over
  // the recipient's live client connection. Non-push modes stay queued and
  // only surface through notifications or the hook pull.
  private pushKindFor(recipient: string, mode: DeliveryMode): PushTarget['kind'] {
    if (mode !== 'push') {
      return 'queued';
    }
    return this.store.getNativeTransport(recipient) !== null ? 'native' : 'connection';
  }

  reply(
    sender: string,
    arg: { messageId: number; body: string; verdict?: Verdict; references?: string[] },
  ): { result: ReplyResult } {
    if (arg.verdict !== undefined && !VERDICTS.includes(arg.verdict)) {
      throw new MurmurError('bad_request', 'invalid verdict: ' + arg.verdict);
    }
    const parent = this.store.messageById(arg.messageId);
    if (parent === null) {
      throw new MurmurError('unknown_message', 'unknown message: ' + String(arg.messageId));
    }
    const ts = this.nowMs();
    const appended = this.store.appendMessage({
      threadId: parent.threadId,
      sender,
      body: arg.body,
      deliveredBody: arg.body,
      truncated: false,
      refs: arg.references ?? [],
      mode: 'push',
      isReply: true,
      inReplyTo: parent.id,
      verdict: arg.verdict ?? null,
      createdAt: ts,
    });
    // The sender's own delivery row on the parent message closes the loop:
    // replied, with latency measured from the parent's creation.
    const latencyMs = ts - parent.createdAt;
    for (const delivery of this.store.getDeliveries(parent.id)) {
      if (delivery.recipient !== sender) {
        continue;
      }
      this.store.markReplied(parent.id, sender, ts, latencyMs);
      this.events.onStateChange({
        threadId: parent.threadId,
        recipient: sender,
        from: delivery.state,
        to: 'replied',
      });
    }
    this.events.onAppend({ threadId: parent.threadId, messageId: appended.id, isReply: true, sender });
    return { result: { messageId: appended.id, threadId: parent.threadId, seq: appended.seq } };
  }

  // Marks acknowledged each id with a delivery row for this handle that sits
  // in delivered or sent. Returns the count actually acknowledged.
  ack(handle: string, ids: number[]): number {
    const ts = this.nowMs();
    let count = 0;
    for (const id of ids) {
      const message = this.store.messageById(id);
      if (message === null) {
        continue;
      }
      const delivery = this.store.getDeliveries(id).find((d) => d.recipient === handle);
      if (delivery === undefined || (delivery.state !== 'delivered' && delivery.state !== 'sent')) {
        continue;
      }
      this.store.markAcked(id, handle, ts);
      this.events.onStateChange({
        threadId: message.threadId,
        recipient: handle,
        from: delivery.state,
        to: 'acknowledged',
      });
      count += 1;
    }
    return count;
  }

  // Full bodies for the requested messages the handle was addressed in,
  // moving queued or sent deliveries to delivered on the way.
  read(handle: string, ids: number[]): MessageFull[] {
    const ts = this.nowMs();
    const out: MessageFull[] = [];
    for (const row of this.store.readMessages(handle, ids)) {
      const delivery = this.store.getDeliveries(row.id).find((d) => d.recipient === handle);
      if (delivery !== undefined && (delivery.state === 'queued' || delivery.state === 'sent')) {
        this.store.markDelivered(row.id, handle, ts);
        this.events.onStateChange({
          threadId: row.threadId,
          recipient: handle,
          from: delivery.state,
          to: 'delivered',
        });
      }
      out.push(fullOf(row));
    }
    return out;
  }

  // Messages delivered to the handle, grouped by thread, most recently
  // active thread first. Per-thread unread counts come from the current
  // delivery states, so the sum matches unreadTotal in both filter modes.
  inbox(handle: string, unreadOnly: boolean): InboxResult {
    const { messages, unreadTotal } = this.store.inboxFor(handle, unreadOnly);
    const byThread = new Map<number, MessageRow[]>();
    for (const row of messages) {
      const existing = byThread.get(row.threadId);
      if (existing === undefined) {
        byThread.set(row.threadId, [row]);
      } else {
        existing.push(row);
      }
    }
    const threads: InboxThread[] = [];
    for (const [threadId, rows] of byThread) {
      let unread = 0;
      let latestSeq = -1;
      const summaries: MessageSummary[] = [];
      for (const row of rows) {
        const delivery = this.store.getDeliveries(row.id).find((d) => d.recipient === handle);
        if (delivery !== undefined && UNREAD_STATES.includes(delivery.state)) {
          unread += 1;
        }
        summaries.push(summaryOf(row));
        if (row.seq > latestSeq) {
          latestSeq = row.seq;
        }
      }
      threads.push({ threadId, latestSeq, unread, messages: summaries });
    }
    threads.sort((a, b) => b.latestSeq - a.latestSeq);
    return { handle, unreadTotal, threads };
  }

  threadView(threadId: number, sinceSeq: number | null): ThreadResult {
    const all = this.store.threadMessages(threadId, null);
    if (all.length === 0) {
      throw new MurmurError('unknown_thread', 'unknown thread: ' + String(threadId));
    }
    const last = all[all.length - 1];
    const latestSeq = last === undefined ? 0 : last.seq;
    const rows = sinceSeq === null ? all : this.store.threadMessages(threadId, sinceSeq);
    return { threadId, latestSeq, messages: rows.map(fullOf) };
  }

  // Matrix for the thread's latest ask: each recipient's delivery state,
  // their verdict and reply latency when they answered the ask in the
  // thread, and their all-time reply latency percentiles.
  collect(threadId: number): CollectResult {
    const all = this.store.threadMessages(threadId, null);
    if (all.length === 0) {
      throw new MurmurError('unknown_thread', 'unknown thread: ' + String(threadId));
    }
    const ask = this.store.latestAsk(threadId);
    if (ask === null) {
      throw new MurmurError('bad_request', 'thread ' + String(threadId) + ' has no ask yet');
    }
    const recipients: RecipientMatrix[] = [];
    const seen = new Set<string>();
    for (const delivery of this.store.getDeliveries(ask.id)) {
      if (seen.has(delivery.recipient)) {
        continue;
      }
      seen.add(delivery.recipient);
      // Latest reply by this recipient aimed at this ask, if any.
      let reply: MessageRow | null = null;
      for (const row of all) {
        if (row.isReply && row.sender === delivery.recipient && row.inReplyTo === ask.id) {
          reply = row;
        }
      }
      const latencies = this.store.replyLatencies(delivery.recipient).sort((a, b) => a - b);
      recipients.push({
        recipient: delivery.recipient,
        state: delivery.state as DeliveryState,
        verdict: reply === null ? null : parseVerdict(reply.verdict),
        repliedLatencyMs: reply === null ? null : delivery.latencyMs,
        p50ReplyMs: percentile(latencies, 0.5),
        p95ReplyMs: percentile(latencies, 0.95),
      });
    }
    const verdictTally: Record<Verdict, number> = { agree: 0, disagree: 0, info: 0 };
    for (const row of all) {
      if (!row.isReply) {
        continue;
      }
      const verdict = parseVerdict(row.verdict);
      if (verdict !== null) {
        verdictTally[verdict] += 1;
      }
    }
    return { threadId, askMessageId: ask.id, recipients, verdictTally };
  }

  // Presence per peer: live connection wins, then the hook state, then the
  // five minute window over last_seen_at, then presumed-offline for a peer
  // seen recently but not connected.
  status(live: (handle: string) => { online: boolean }): StatusResult {
    const ts = this.nowMs();
    const peers: PeerStatus[] = [];
    for (const peer of this.store.listPeers()) {
      const override = this.hookOverrides.get(peer.handle);
      const hookState = override !== undefined ? override.state : normalizeHookState(peer.hookState);
      const hookAt = override !== undefined ? override.at : peer.lastHookActivityAt;
      let presence: PresenceKind;
      let busySince: number | null = null;
      let idleSince: number | null = null;
      if (live(peer.handle).online) {
        presence = 'online';
      } else if (hookState === 'busy') {
        presence = 'busy';
        busySince = hookAt;
      } else if (hookState === 'idle') {
        presence = 'idle';
        idleSince = hookAt;
      } else if (peer.lastSeenAt === null || ts - peer.lastSeenAt > OFFLINE_AFTER_MS) {
        presence = 'offline';
      } else {
        presence = 'presumed-offline';
      }
      const lastActivity = freshestOf(peer.lastSeenAt, hookAt);
      peers.push({
        handle: peer.handle,
        display: peer.display,
        role: peer.role,
        isOwner: peer.isOwner,
        presence,
        busySince,
        idleSince,
        lastActivityAgeMs: lastActivity === null ? null : ts - lastActivity,
        unread: this.store.deliveryStatesForRecipient(peer.handle, UNREAD_STATES),
      });
    }
    return { now: ts, peers };
  }

  // Group membership only. Threads and message history never reference
  // groups, so deleting one touches neither.
  groups(action: GroupAction, name?: string, members?: string[]): GroupsResult[] {
    const ts = this.nowMs();
    switch (action) {
      case 'create': {
        if (name === undefined || !GROUP_NAME_RE.test(name)) {
          throw new MurmurError('bad_request', 'invalid group name: ' + String(name));
        }
        const stripped = stripGroupPrefix(name);
        if (this.store.listGroups().some((g) => g.name === stripped)) {
          throw new MurmurError('bad_request', 'group already exists: ' + stripped);
        }
        this.store.createGroup(stripped, ts);
        break;
      }
      case 'delete': {
        this.store.deleteGroup(this.requireGroup(name));
        break;
      }
      case 'add':
      case 'remove': {
        const group = this.requireGroup(name);
        const handles = this.requireMembers(members);
        for (const handle of handles) {
          if (this.store.getPeer(handle) === null) {
            throw new MurmurError('unknown_handle', 'unknown handle: ' + handle);
          }
        }
        if (action === 'add') {
          const found = this.store.listGroups().find((g) => g.name === group);
          const current = found === undefined ? new Set<string>() : new Set(found.members);
          for (const handle of handles) {
            // Adding an existing member is a no-op, not an error.
            if (!current.has(handle)) {
              this.store.addMember(group, handle, ts);
            }
          }
        } else {
          for (const handle of handles) {
            // Removing a non-member is a no-op, not an error.
            this.store.removeMember(group, handle);
          }
        }
        break;
      }
      case 'list':
        break;
    }
    return this.store.listGroups().map((g) => ({
      name: g.name,
      members: [...g.members].sort(),
      createdAt: g.createdAt,
    }));
  }

  private requireGroup(name: string | undefined): string {
    if (name === undefined) {
      throw new MurmurError('bad_request', 'group name required');
    }
    const stripped = stripGroupPrefix(name);
    if (!this.store.listGroups().some((g) => g.name === stripped)) {
      throw new MurmurError('unknown_group', 'unknown group: ' + stripped);
    }
    return stripped;
  }

  private requireMembers(members: string[] | undefined): string[] {
    if (members === undefined || members.length === 0) {
      throw new MurmurError('bad_request', 'members required');
    }
    return members;
  }

  // Records a hook lifecycle event. prompt means the session is working
  // (busy); session-start and stop leave it idle. Fires nothing.
  hookState(handle: string, event: HookEvent): void {
    this.ensurePeer(handle);
    const state = event === 'prompt' ? 'busy' : 'idle';
    this.hookOverrides.set(handle, { state, at: this.nowMs() });
  }

  registerNative(arg: { handle: string; socketPath: string; token: string }): void {
    this.ensurePeer(arg.handle);
    this.store.upsertNativeTransport(arg.handle, arg.socketPath, arg.token, this.nowMs());
  }

  // The hook injection pull: queued or sent push-mode messages, newest
  // first, marked delivered as they are handed out.
  pendingPush(handle: string, limit: number): FetchPushResult {
    const ts = this.nowMs();
    const messages: MessageFull[] = [];
    for (const entry of this.store.pendingPushFor(handle, limit)) {
      const delivery = this.store.getDeliveries(entry.message.id).find((d) => d.recipient === handle);
      if (delivery !== undefined && (delivery.state === 'queued' || delivery.state === 'sent')) {
        this.store.markDelivered(entry.message.id, handle, ts);
        this.events.onStateChange({
          threadId: entry.message.threadId,
          recipient: handle,
          from: delivery.state,
          to: 'delivered',
        });
      }
      messages.push(fullOf(entry.message));
    }
    return { messages };
  }

  // Stamps last_seen_at on authenticated activity. upsertPeer refreshes
  // display, role and last_seen_at, so the current values are passed back
  // to keep them intact.
  touchSeen(handle: string): void {
    const peer: PeerRow | null = this.store.getPeer(handle);
    if (peer === null) {
      return;
    }
    this.store.upsertPeer(handle, peer.display, peer.role, peer.isOwner, this.nowMs());
  }

  // Creates the peer row only when it is missing; an existing row is left
  // untouched so its display, role and last_seen_at survive.
  private ensurePeer(handle: string): void {
    if (this.store.getPeer(handle) === null) {
      this.store.upsertPeer(handle, null, null, false, this.nowMs());
    }
  }
}
