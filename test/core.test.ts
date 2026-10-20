import Database from 'better-sqlite3';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { MurmurCore, fullOf, percentile } from '../src/core.js';
import type { CoreEvents } from '../src/core.js';
import {
  DUP_WINDOW_MS,
  MAX_BODY_CHARS,
  MurmurError,
  RATE_MAX_SENDS,
  RATE_WINDOW_MS,
  THREAD_CAP,
  UNREAD_CAP,
} from '../src/protocol.js';
import type { Verdict } from '../src/protocol.js';
import { Store } from '../src/store.js';
import type { DeliveryRow } from '../src/store.js';
import type { ProcInfo } from '../src/proc.js';

let dir: string;
let dbPath: string;
let store: Store;
let now: number;
let appends: Array<{ threadId: number; messageId: number; isReply: boolean; sender: string }>;
let changes: Array<{ threadId: number; recipient: string; from: string; to: string }>;
let core: MurmurCore;
let procs: Map<number, ProcInfo>;
let procThrows: boolean;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'murmur-core-'));
  dbPath = join(dir, 'murmur.db');
  store = Store.open(dbPath);
  now = 1_000_000;
  appends = [];
  changes = [];
  const events: CoreEvents = {
    onAppend: (arg) => {
      appends.push({ ...arg });
    },
    onStateChange: (arg) => {
      changes.push({ ...arg });
    },
  };
  procs = new Map();
  procThrows = false;
  core = new MurmurCore(store, events, () => now, () => {
    if (procThrows) {
      throw new Error('ps failed');
    }
    return procs;
  });
});

afterEach(() => {
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

function advance(ms: number): void {
  now += ms;
}

function setNow(ms: number): void {
  now = ms;
}

// Marks a host process alive in the fake process table; its start is 'S<pid>'.
function alive(pid: number): { hostPid: number; hostStart: string } {
  procs.set(pid, { ppid: 1, start: 'S' + String(pid), comm: 'agent' });
  return { hostPid: pid, hostStart: 'S' + String(pid) };
}

function register(...handles: string[]): void {
  for (const handle of handles) {
    core.register({ handle });
  }
}

function expectCoreError(fn: () => void, code: string, part?: string): void {
  let caught: unknown;
  try {
    fn();
  } catch (err) {
    caught = err;
  }
  expect(caught).toBeInstanceOf(MurmurError);
  expect((caught as MurmurError).code).toBe(code);
  if (part !== undefined) {
    expect((caught as MurmurError).message).toContain(part);
  }
}

interface RawOpts {
  sender?: string;
  body?: string;
  isReply?: boolean;
  inReplyTo?: number | null;
  verdict?: string | null;
  mode?: string;
  createdAt?: number;
}

function appendRaw(threadId: number, opts: RawOpts = {}): { id: number; seq: number } {
  return store.appendMessage({
    threadId,
    sender: opts.sender ?? 'builder',
    body: opts.body ?? 'raw',
    deliveredBody: opts.body ?? 'raw',
    truncated: false,
    refs: [],
    mode: opts.mode ?? 'push',
    isReply: opts.isReply ?? false,
    inReplyTo: opts.inReplyTo ?? null,
    verdict: opts.verdict ?? null,
    createdAt: opts.createdAt ?? 1,
  });
}

// UNREAD_CAP unread delivery rows for one handle, for inbox cap tests.
function unreadDeliveries(handle: string, count: number): void {
  const tid = store.createThread('builder', 1);
  for (let i = 0; i < count; i++) {
    const { id } = appendRaw(tid, { createdAt: i + 1 });
    store.addDeliveries(id, [handle], i + 1);
  }
}

describe('percentile', () => {
  it('returns null for an empty list', () => {
    expect(percentile([], 0.5)).toBeNull();
  });

  it('returns the single value for one element', () => {
    expect(percentile([7], 0.5)).toBe(7);
  });

  it('interpolates between neighbours', () => {
    expect(percentile([1, 2], 0.5)).toBe(1.5);
    expect(percentile([1, 2, 3, 4, 5], 0.95)).toBeCloseTo(4.8, 10);
  });

  it('lands exactly on a member at fraction 0 and 1', () => {
    expect(percentile([1, 2, 3], 0)).toBe(1);
    expect(percentile([1, 2, 3], 1)).toBe(3);
  });

  it('returns null when the fraction walks off the list', () => {
    expect(percentile([1, 2], 2)).toBeNull();
    expect(percentile([1, 2], -1)).toBeNull();
  });
});

describe('register', () => {
  it('creates peers with optional display and role', () => {
    const res = core.register({ handle: 'alpha', display: 'Alpha', role: 'reviewer' });
    expect(res).toEqual({ handle: 'alpha', owner: false });
    core.register({ handle: 'beta' });
    const peers = core.status(() => ({ online: false })).peers;
    expect(peers.find((p) => p.handle === 'alpha')).toMatchObject({ display: 'Alpha', role: 'reviewer', isOwner: false });
    expect(peers.find((p) => p.handle === 'beta')).toMatchObject({ display: null, role: null, isOwner: false });
  });

  it.each(['A', '-bad', 'a', 'a'.repeat(33), 'has space'])('rejects the invalid handle %s', (handle) => {
    expectCoreError(() => core.register({ handle }), 'bad_request', 'invalid handle');
  });

  it('renames a peer and keeps its profile', () => {
    core.register({ handle: 'old-handle', display: 'Legacy', role: 'keeper' });
    const res = core.register({ handle: 'new-handle', existing: 'old-handle', display: 'Legacy', role: 'keeper' });
    expect(res).toEqual({ handle: 'new-handle', owner: false });
    const peers = core.status(() => ({ online: false })).peers;
    expect(peers.map((p) => p.handle)).toEqual(['new-handle']);
    expect(peers[0]).toMatchObject({ display: 'Legacy', role: 'keeper' });
  });

  it('rejects a rename from an unknown handle', () => {
    expectCoreError(() => core.register({ handle: 'fresh', existing: 'ghost' }), 'unknown_handle', 'ghost');
  });

  it('rejects a rename onto an occupied handle', () => {
    register('first', 'second');
    expectCoreError(() => core.register({ handle: 'second', existing: 'first' }), 'bad_request', 'already exists');
  });

  it('treats a same-handle reregistration as a plain upsert', () => {
    core.register({ handle: 'same', existing: 'same' });
    const handles = core.status(() => ({ online: false })).peers.map((p) => p.handle);
    expect(handles).toEqual(['same']);
  });
});

describe('resolveRecipients', () => {
  it('keeps order, validates handles and dedups', () => {
    register('alpha', 'beta');
    expect(core.resolveRecipients(['alpha', 'beta'])).toEqual(['alpha', 'beta']);
    expect(core.resolveRecipients(['alpha', 'alpha'])).toEqual(['alpha']);
    expectCoreError(() => core.resolveRecipients(['alpha', 'ghost']), 'unknown_handle', 'ghost');
  });

  it('expands groups, dedups against direct entries and rejects empty results', () => {
    register('alpha', 'beta', 'gamma');
    core.groups('create', 'team');
    core.groups('add', 'team', ['alpha', 'beta']);
    expect(core.resolveRecipients(['@team', 'beta', 'gamma'])).toEqual(['alpha', 'beta', 'gamma']);
    expectCoreError(() => core.resolveRecipients(['@missing']), 'unknown_group', 'missing');
    core.groups('create', 'empty');
    expectCoreError(() => core.resolveRecipients(['@empty']), 'bad_request', 'no recipients resolved');
    expectCoreError(() => core.resolveRecipients([]), 'bad_request', 'no recipients resolved');
  });
});

describe('checkLimits', () => {
  it('enforces the sliding send rate window', () => {
    for (let i = 0; i < RATE_MAX_SENDS; i++) {
      core.checkLimits('spammer', 'body ' + String(i), ['r'], null);
    }
    expectCoreError(() => core.checkLimits('spammer', 'one more', ['r'], null), 'rate_limited', 'within 60000ms');
    advance(RATE_WINDOW_MS + 1);
    expect(() => core.checkLimits('spammer', 'after the window', ['r'], null)).not.toThrowError();
  });

  it('enforces the duplicate window per fingerprint', () => {
    core.checkLimits('dupe', 'same body', ['r'], null);
    expectCoreError(() => core.checkLimits('dupe', 'same body', ['r'], null), 'duplicate', 'identical message');
    core.checkLimits('dupe', 'other body', ['r'], null);
    advance(DUP_WINDOW_MS + 1);
    expect(() => core.checkLimits('dupe', 'same body', ['r'], null)).not.toThrowError();
  });

  it('enforces the thread cap and skips it for new threads', () => {
    const tid = store.createThread('builder', 1);
    for (let i = 0; i < THREAD_CAP; i++) {
      appendRaw(tid, { createdAt: i + 1, body: 'm' + String(i) });
    }
    expectCoreError(() => core.checkLimits('s', 'x', ['r'], tid), 'cap_thread');
    expect(() => core.checkLimits('s', 'x', ['r'], null)).not.toThrowError();
  });

  it('enforces the inbox cap per recipient', () => {
    register('free-peer');
    unreadDeliveries('hoarder', UNREAD_CAP);
    expectCoreError(() => core.checkLimits('s', 'x', ['free-peer', 'hoarder'], null), 'cap_inbox', 'hoarder');
    expect(() => core.checkLimits('s2', 'x', ['free-peer'], null)).not.toThrowError();
  });
});

describe('send', () => {
  it('opens a new thread, queues deliveries and returns connection pushes', () => {
    register('sender', 'beta');
    const { result, pushes } = core.send('sender', { threadId: null, to: ['beta'], body: 'hello' });
    expect(result.seq).toBe(0);
    expect(result.threadId).toBeGreaterThan(0);
    expect(result.deliveries).toHaveLength(1);
    expect(result.deliveries[0]).toEqual({
      recipient: 'beta',
      state: 'queued',
      sentAt: null,
      deliveredAt: null,
      ackedAt: null,
      repliedAt: null,
      latencyMs: null,
    });
    expect(pushes).toHaveLength(1);
    expect(pushes[0]?.recipient).toBe('beta');
    expect(pushes[0]?.kind).toBe('connection');
    expect(pushes[0]?.message.body).toBe('hello');
    expect(pushes[0]?.footer).toContain('murmur_reply messageId ' + String(result.messageId));
    expect(pushes[0]?.footer).toContain('murmur_thread ' + String(result.threadId));
    expect(pushes[0]?.text).toContain(
      '[murmur m' + String(result.messageId) + ' t' + String(result.threadId) + ' s0 from sender to beta]\nhello\n',
    );
    expect(appends).toEqual([
      { threadId: result.threadId, messageId: result.messageId, isReply: false, sender: 'sender' },
    ]);
    expect(changes).toEqual([]);
  });

  it('truncates the delivered body past the cap but keeps the full body', () => {
    register('sender', 'beta');
    const body = 'x'.repeat(MAX_BODY_CHARS + 10);
    const { result, pushes } = core.send('sender', { threadId: null, to: ['beta'], body });
    const verify = Store.open(dbPath);
    try {
      const row = verify.messageById(result.messageId);
      expect(row?.truncated).toBe(true);
      expect(row?.body).toBe(body);
      expect(row?.deliveredBody?.startsWith('x'.repeat(MAX_BODY_CHARS))).toBe(true);
      expect(row?.deliveredBody).toContain('[truncated');
    } finally {
      verify.close();
    }
    expect(pushes[0]?.message.body).toBe(body);
    const view = core.threadView(result.threadId, null);
    expect(view.messages[0]?.truncated).toBe(true);
    expect(view.messages[0]?.firstLine).toHaveLength(120);
  });

  it('appends to an existing thread and rejects unknown threads', () => {
    register('sender', 'beta');
    const first = core.send('sender', { threadId: null, to: ['beta'], body: 'one' });
    const second = core.send('sender', { threadId: first.result.threadId, to: ['beta'], body: 'two' });
    expect(second.result.seq).toBe(1);
    expect(second.result.threadId).toBe(first.result.threadId);
    expectCoreError(() => core.send('sender', { threadId: 987654, to: ['beta'], body: 'nope' }), 'unknown_thread');
  });

  it('classifies pushes as native, connection or queued', () => {
    register('sender', 'beta', 'gamma');
    core.registerNative({ handle: 'gamma', socketPath: join(dir, 'native.sock'), token: 'nt' });
    const { pushes } = core.send('sender', { threadId: null, to: ['beta', 'gamma'], body: 'x' });
    expect(pushes.map((p) => [p.recipient, p.kind])).toEqual([
      ['beta', 'connection'],
      ['gamma', 'native'],
    ]);
    const notified = core.send('sender', { threadId: null, to: ['beta'], body: 'y', mode: 'notify' });
    expect(notified.pushes[0]?.kind).toBe('queued');
  });

  it('auto creates the peer for a native transport on an unknown handle', () => {
    core.registerNative({ handle: 'native-new', socketPath: join(dir, 'n.sock'), token: 't' });
    const handles = core.status(() => ({ online: false })).peers.map((p) => p.handle);
    expect(handles).toEqual(['native-new']);
  });

  it('stores references and the delivery mode', () => {
    register('sender', 'beta');
    const { result } = core.send('sender', {
      threadId: null,
      to: ['beta'],
      body: 'x',
      references: ['ref-1'],
    });
    const verify = Store.open(dbPath);
    try {
      expect(JSON.parse(verify.messageById(result.messageId)?.refsJson ?? 'null')).toEqual(['ref-1']);
      expect(verify.messageById(result.messageId)?.mode).toBe('push');
    } finally {
      verify.close();
    }
  });
});

describe('reply', () => {
  it('correlates a reply, moves the sender delivery to replied and records latency', () => {
    register('alpha', 'beta', 'gamma');
    setNow(1_000_000);
    const ask = core.send('alpha', { threadId: null, to: ['beta', 'gamma'], body: 'decide' });
    setNow(1_500_000);
    const { result } = core.reply('beta', { messageId: ask.result.messageId, body: 'yes', verdict: 'agree' });
    expect(result.messageId).toBeGreaterThan(ask.result.messageId);
    expect(result.threadId).toBe(ask.result.threadId);
    expect(result.seq).toBe(1);
    expect(appends[1]).toEqual({ threadId: ask.result.threadId, messageId: result.messageId, isReply: true, sender: 'beta' });
    expect(changes).toEqual([{ threadId: ask.result.threadId, recipient: 'beta', from: 'queued', to: 'replied' }]);
    const verify = Store.open(dbPath);
    try {
      const row = verify.messageById(result.messageId);
      expect(row).toMatchObject({ isReply: true, inReplyTo: ask.result.messageId, verdict: 'agree' });
      const betaDelivery = verify.getDeliveries(ask.result.messageId).find((d) => d.recipient === 'beta');
      expect(betaDelivery).toMatchObject({ state: 'replied', repliedAt: 1_500_000, latencyMs: 500_000 });
      expect(verify.getDeliveries(ask.result.messageId).find((d) => d.recipient === 'gamma')?.state).toBe('queued');
    } finally {
      verify.close();
    }
    const view = core.threadView(ask.result.threadId, null);
    expect(view.messages[1]).toMatchObject({
      isReply: true,
      inReplyTo: ask.result.messageId,
      sender: 'beta',
      verdict: 'agree',
    });
  });

  it('appends replies from senders without a delivery row without state changes', () => {
    register('alpha', 'beta');
    const ask = core.send('alpha', { threadId: null, to: ['beta'], body: 'q' });
    const before = changes.length;
    const { result } = core.reply('alpha', { messageId: ask.result.messageId, body: 'note' });
    expect(result.seq).toBe(1);
    expect(changes).toHaveLength(before);
    expect(appends).toHaveLength(2);
  });

  it('rejects invalid verdicts and unknown parents', () => {
    register('alpha', 'beta');
    const ask = core.send('alpha', { threadId: null, to: ['beta'], body: 'q' });
    expectCoreError(
      () => core.reply('beta', { messageId: ask.result.messageId, body: 'x', verdict: 'maybe' as Verdict }),
      'bad_request',
      'invalid verdict',
    );
    expectCoreError(() => core.reply('beta', { messageId: 424242, body: 'x' }), 'unknown_message');
  });

  it('stores reply references and a null verdict by default', () => {
    register('alpha', 'beta');
    const ask = core.send('alpha', { threadId: null, to: ['beta'], body: 'q' });
    const { result } = core.reply('beta', { messageId: ask.result.messageId, body: 'r', references: ['ctx-9'] });
    const verify = Store.open(dbPath);
    try {
      expect(JSON.parse(verify.messageById(result.messageId)?.refsJson ?? 'null')).toEqual(['ctx-9']);
      expect(verify.messageById(result.messageId)?.verdict).toBeNull();
    } finally {
      verify.close();
    }
  });
});

describe('ack and read', () => {
  it('moves queued to delivered on read and delivered to acknowledged on ack', () => {
    register('sender', 'receiver');
    const { result } = core.send('sender', { threadId: null, to: ['receiver'], body: 'hi' });
    const id = result.messageId;
    expect(core.ack('receiver', [id])).toBe(0);
    const full = core.read('receiver', [id]);
    expect(full).toHaveLength(1);
    expect(full[0]?.body).toBe('hi');
    expect(changes).toContainEqual({ threadId: result.threadId, recipient: 'receiver', from: 'queued', to: 'delivered' });
    expect(core.ack('receiver', [id])).toBe(1);
    expect(changes).toContainEqual({
      threadId: result.threadId,
      recipient: 'receiver',
      from: 'delivered',
      to: 'acknowledged',
    });
    expect(core.ack('receiver', [id])).toBe(0);
  });

  it('acks a sent state and skips unknown ids and foreign rows', () => {
    register('sender', 'receiver');
    const { result } = core.send('sender', { threadId: null, to: ['receiver'], body: 'hi' });
    const verify = Store.open(dbPath);
    try {
      verify.updateDelivery(result.messageId, 'receiver', { state: 'sent', sentAt: 1 });
    } finally {
      verify.close();
    }
    expect(core.ack('receiver', [result.messageId])).toBe(1);
    expect(changes.at(-1)).toMatchObject({ from: 'sent', to: 'acknowledged' });
    expect(core.ack('receiver', [424242])).toBe(0);
    expect(core.ack('sender', [result.messageId])).toBe(0);
    expect(core.ack('receiver', [result.messageId])).toBe(0);
  });

  it('read only returns addressed messages and skips already delivered rows', () => {
    register('sender', 'peer-a', 'peer-b');
    const m1 = core.send('sender', { threadId: null, to: ['peer-a'], body: 'for a' });
    const m2 = core.send('sender', { threadId: null, to: ['peer-b'], body: 'for b' });
    expect(core.read('peer-a', [m1.result.messageId, m2.result.messageId]).map((m) => m.id)).toEqual([
      m1.result.messageId,
    ]);
    const stateCount = changes.length;
    expect(core.read('peer-a', [m1.result.messageId]).map((m) => m.id)).toEqual([m1.result.messageId]);
    expect(changes).toHaveLength(stateCount);
    expect(core.read('nobody', [m1.result.messageId])).toEqual([]);
    expect(core.read('peer-b', [m1.result.messageId, m2.result.messageId]).map((m) => m.id)).toEqual([
      m2.result.messageId,
    ]);
  });
});

describe('inbox', () => {
  it('groups by thread, counts unread and sorts by latest activity', () => {
    register('sender', 'reader');
    const t1 = core.send('sender', { threadId: null, to: ['reader'], body: 'one' });
    const t2 = core.send('sender', { threadId: null, to: ['reader'], body: 'two' });
    const t3 = core.send('sender', { threadId: t1.result.threadId, to: ['reader'], body: 'three' });
    const inbox = core.inbox('reader', false);
    expect(inbox.handle).toBe('reader');
    expect(inbox.unreadTotal).toBe(3);
    expect(inbox.threads).toHaveLength(2);
    expect(inbox.threads[0]).toMatchObject({ threadId: t1.result.threadId, latestSeq: 1, unread: 2 });
    expect(inbox.threads[0]?.messages).toHaveLength(2);
    expect(inbox.threads[1]).toMatchObject({ threadId: t2.result.threadId, latestSeq: 0, unread: 1 });
    const unreadOnly = core.inbox('reader', true);
    expect(unreadOnly.unreadTotal).toBe(3);
    expect(unreadOnly.threads).toHaveLength(2);
    core.read('reader', [t1.result.messageId, t2.result.messageId, t3.result.messageId]);
    // delivered is still an unread state, so only the ack clears the badge.
    core.ack('reader', [t1.result.messageId, t2.result.messageId, t3.result.messageId]);
    expect(core.inbox('reader', true)).toEqual({ handle: 'reader', unreadTotal: 0, threads: [] });
    const all = core.inbox('reader', false);
    expect(all.unreadTotal).toBe(0);
    expect(all.threads.map((t) => t.unread)).toEqual([0, 0]);
    expect(core.inbox('stranger', false)).toEqual({ handle: 'stranger', unreadTotal: 0, threads: [] });
  });
});

describe('threadView', () => {
  it('returns full messages with sinceSeq filtering', () => {
    register('sender', 'beta');
    const first = core.send('sender', { threadId: null, to: ['beta'], body: 'first' });
    core.send('sender', { threadId: first.result.threadId, to: ['beta'], body: 'second' });
    const tid = first.result.threadId;
    expect(core.threadView(tid, null).latestSeq).toBe(1);
    expect(core.threadView(tid, null).messages.map((m) => m.seq)).toEqual([0, 1]);
    expect(core.threadView(tid, 0).messages.map((m) => m.seq)).toEqual([1]);
    const tail = core.threadView(tid, 99);
    expect(tail.latestSeq).toBe(1);
    expect(tail.messages).toEqual([]);
    expectCoreError(() => core.threadView(404404, null), 'unknown_thread');
  });
});

describe('collect', () => {
  it('builds the verdict matrix with reply percentiles', () => {
    register('alpha', 'beta', 'gamma', 'delta');
    setNow(500_000);
    const warm = core.send('alpha', { threadId: null, to: ['beta'], body: 'warm' });
    setNow(500_100);
    core.reply('beta', { messageId: warm.result.messageId, body: 'warm reply' });
    setNow(1_000_000);
    const ask = core.send('alpha', { threadId: null, to: ['beta', 'gamma', 'delta'], body: 'decide' });
    setNow(1_001_000);
    core.reply('beta', { messageId: ask.result.messageId, body: 'yes', verdict: 'agree' });
    setNow(1_003_000);
    core.reply('gamma', { messageId: ask.result.messageId, body: 'fyi', verdict: 'info' });
    setNow(1_010_000);
    core.reply('alpha', { messageId: ask.result.messageId, body: 'noted' });
    const matrix = core.collect(ask.result.threadId);
    expect(matrix.threadId).toBe(ask.result.threadId);
    expect(matrix.askMessageId).toBe(ask.result.messageId);
    expect(matrix.recipients.map((r) => r.recipient)).toEqual(['beta', 'delta', 'gamma']);
    expect(matrix.recipients.find((r) => r.recipient === 'beta')).toEqual({
      recipient: 'beta',
      state: 'replied',
      verdict: 'agree',
      repliedLatencyMs: 1000,
      p50ReplyMs: 550,
      p95ReplyMs: 955,
    });
    expect(matrix.recipients.find((r) => r.recipient === 'delta')).toEqual({
      recipient: 'delta',
      state: 'queued',
      verdict: null,
      repliedLatencyMs: null,
      p50ReplyMs: null,
      p95ReplyMs: null,
    });
    expect(matrix.recipients.find((r) => r.recipient === 'gamma')).toEqual({
      recipient: 'gamma',
      state: 'replied',
      verdict: 'info',
      repliedLatencyMs: 3000,
      p50ReplyMs: 3000,
      p95ReplyMs: 3000,
    });
    expect(matrix.verdictTally).toEqual({ agree: 1, disagree: 0, info: 1 });
  });

  it('uses the latest reply per recipient', () => {
    register('alpha', 'beta');
    const ask = core.send('alpha', { threadId: null, to: ['beta'], body: 'pick' });
    advance(100);
    core.reply('beta', { messageId: ask.result.messageId, body: 'first', verdict: 'agree' });
    advance(200);
    core.reply('beta', { messageId: ask.result.messageId, body: 'second', verdict: 'disagree' });
    const matrix = core.collect(ask.result.threadId);
    expect(matrix.recipients[0]).toMatchObject({
      recipient: 'beta',
      verdict: 'disagree',
      repliedLatencyMs: 300,
      p50ReplyMs: 300,
      p95ReplyMs: 300,
    });
    expect(matrix.verdictTally).toEqual({ agree: 1, disagree: 1, info: 0 });
  });

  it('rejects unknown threads and threads without an ask', () => {
    expectCoreError(() => core.collect(31337), 'unknown_thread');
    const tid = store.createThread('builder', 1);
    appendRaw(tid, { isReply: true, inReplyTo: 1, sender: 'x' });
    expectCoreError(() => core.collect(tid), 'bad_request', 'no ask yet');
  });
});

describe('references hardening', () => {
  it('degrades malformed refs_json to an empty list and drops non strings', () => {
    register('sender', 'beta');
    const { result } = core.send('sender', { threadId: null, to: ['beta'], body: 'x' });
    const raw = new Database(dbPath);
    try {
      const cases: Array<[string, string[]]> = [
        ['not json at all', []],
        ['42', []],
        ['{"a":1}', []],
        ['["keep",7,null,"also"]', ['keep', 'also']],
      ];
      for (const [json, expected] of cases) {
        raw.prepare('UPDATE messages SET refs_json = ? WHERE id = ?').run(json, result.messageId);
        const view = core.threadView(result.threadId, null);
        expect(view.messages[0]?.references).toEqual(expected);
      }
    } finally {
      raw.close();
    }
  });
});

describe('status', () => {
  it('derives presence tiers from live connections, hook state and last seen', () => {
    register('live-peer', 'busy-peer', 'gone-peer', 'stale-peer', 'idle-db-peer', 'bad-hook-peer');
    advance(10_000);
    core.hookState('live-peer', 'prompt');
    core.hookState('busy-peer', 'prompt');
    advance(10_000);
    const raw = new Database(dbPath);
    try {
      raw
        .prepare(
          "UPDATE peers SET hook_state = 'idle', last_hook_activity_at = ?, last_seen_at = NULL WHERE handle = 'idle-db-peer'",
        )
        .run(now);
      raw.prepare("UPDATE peers SET hook_state = 'zzz' WHERE handle = 'bad-hook-peer'").run();
      raw.prepare("UPDATE peers SET last_seen_at = ? WHERE handle = 'stale-peer'").run(100_000);
      raw.prepare("UPDATE peers SET last_seen_at = NULL WHERE handle = 'gone-peer'").run();
    } finally {
      raw.close();
    }
    core.send('live-peer', { threadId: null, to: ['busy-peer'], body: 'pending question' });
    const live = (handle: string): { online: boolean } => ({ online: handle === 'live-peer' });
    const status = core.status(live);
    expect(status.now).toBe(now);
    const byHandle = new Map(status.peers.map((p) => [p.handle, p]));
    expect(byHandle.get('live-peer')).toMatchObject({
      presence: 'online',
      busySince: null,
      idleSince: null,
      lastActivityAgeMs: now - 1_010_000,
      unread: 0,
    });
    expect(byHandle.get('busy-peer')).toMatchObject({
      presence: 'busy',
      busySince: 1_010_000,
      idleSince: null,
      lastActivityAgeMs: now - 1_010_000,
      unread: 1,
    });
    expect(byHandle.get('idle-db-peer')).toMatchObject({
      presence: 'idle',
      idleSince: now,
      busySince: null,
      lastActivityAgeMs: 0,
      unread: 0,
    });
    expect(byHandle.get('bad-hook-peer')).toMatchObject({
      presence: 'presumed-offline',
      busySince: null,
      idleSince: null,
      lastActivityAgeMs: now - 1_000_000,
    });
    expect(byHandle.get('gone-peer')).toMatchObject({
      presence: 'offline',
      busySince: null,
      idleSince: null,
      lastActivityAgeMs: null,
    });
    expect(byHandle.get('stale-peer')).toMatchObject({ presence: 'offline' });
  });

  it('reads presence from the newest live session of a handle with several', () => {
    core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'old', host: { hostPid: 1001, hostStart: 'S1001' }, handle: 'twin' });
    advance(1000);
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'new', host: alive(1002), handle: 'twin' });
    const twin = core.status(() => ({ online: false })).peers.find((p) => p.handle === 'twin');
    expect(twin).toMatchObject({ presence: 'busy', client: 'claude' });
  });

  it('refreshes presence through touchSeen and ignores unknown handles', () => {
    register('peer');
    advance(400_000);
    expect(core.status(() => ({ online: false })).peers[0]?.presence).toBe('offline');
    core.touchSeen('peer');
    const after = core.status(() => ({ online: false })).peers[0];
    expect(after).toMatchObject({ presence: 'presumed-offline', lastActivityAgeMs: 0 });
    core.touchSeen('ghost');
    expect(core.status(() => ({ online: false })).peers.map((p) => p.handle)).toEqual(['peer']);
  });

  it('tracks hook state transitions and auto creates the peer', () => {
    core.hookState('hooky', 'session-start');
    let peers = core.status(() => ({ online: false })).peers;
    expect(peers.map((p) => p.handle)).toEqual(['hooky']);
    expect(peers[0]).toMatchObject({ presence: 'idle', idleSince: now });
    advance(1000);
    core.hookState('hooky', 'prompt');
    peers = core.status(() => ({ online: false })).peers;
    expect(peers[0]).toMatchObject({ presence: 'busy', busySince: now });
    advance(1000);
    core.hookState('hooky', 'stop');
    peers = core.status(() => ({ online: false })).peers;
    expect(peers[0]).toMatchObject({ presence: 'idle', idleSince: now });
  });
});

describe('groups', () => {
  it('creates, lists, populates and deletes groups', () => {
    register('alpha', 'beta', 'gamma');
    core.groups('create', 'dev');
    core.groups('create', '@ops');
    core.groups('add', 'dev', ['beta', 'alpha']);
    core.groups('add', 'dev', ['alpha']);
    core.groups('add', 'dev', ['gamma']);
    core.groups('remove', 'dev', ['gamma']);
    core.groups('remove', 'dev', ['gamma']);
    const listed = core.groups('list');
    expect(listed).toEqual([
      { name: 'dev', members: ['alpha', 'beta'], createdAt: 1_000_000 },
      { name: 'ops', members: [], createdAt: 1_000_000 },
    ]);
    core.groups('delete', 'dev');
    expect(core.groups('list').map((g) => g.name)).toEqual(['ops']);
  });

  it('validates group names, existence and members', () => {
    register('alpha');
    core.groups('create', 'dev');
    expectCoreError(() => core.groups('create', 'dev'), 'bad_request', 'already exists');
    expectCoreError(() => core.groups('create', 'Bad Name'), 'bad_request', 'invalid group name');
    expectCoreError(() => core.groups('create', 'x'), 'bad_request', 'invalid group name');
    expectCoreError(() => core.groups('create', 'a'.repeat(40)), 'bad_request', 'invalid group name');
    expectCoreError(() => core.groups('add', 'missing', ['alpha']), 'unknown_group');
    expectCoreError(() => core.groups('add', 'dev', ['ghost']), 'unknown_handle', 'ghost');
    expectCoreError(() => core.groups('add', 'dev'), 'bad_request', 'members required');
    expectCoreError(() => core.groups('add', 'dev', []), 'bad_request', 'members required');
    expectCoreError(() => core.groups('delete', 'missing'), 'unknown_group');
    expectCoreError(() => core.groups('delete'), 'bad_request', 'group name required');
  });

  it('keeps thread history when a group is deleted', () => {
    register('alpha', 'beta');
    core.groups('create', 'dev');
    core.groups('add', 'dev', ['beta']);
    const { result } = core.send('alpha', { threadId: null, to: ['@dev'], body: 'broadcast' });
    core.groups('delete', 'dev');
    expect(core.groups('list')).toEqual([]);
    expect(core.threadView(result.threadId, null).messages).toHaveLength(1);
    expect(core.collect(result.threadId).recipients.map((r) => r.recipient)).toEqual(['beta']);
  });
});

describe('pendingPush', () => {
  it('hands out queued pushes newest first and marks them delivered', () => {
    register('sender', 'receiver');
    const m1 = core.send('sender', { threadId: null, to: ['receiver'], body: 'first' });
    const m2 = core.send('sender', { threadId: null, to: ['receiver'], body: 'second' });
    const before = changes.length;
    const batch = core.pendingPush('receiver', 10);
    expect(batch.messages.map((m) => m.id)).toEqual([m2.result.messageId, m1.result.messageId]);
    expect(changes.slice(before)).toEqual([
      { threadId: m2.result.threadId, recipient: 'receiver', from: 'queued', to: 'delivered' },
      { threadId: m1.result.threadId, recipient: 'receiver', from: 'queued', to: 'delivered' },
    ]);
    expect(core.pendingPush('receiver', 10).messages).toEqual([]);
  });

  it('skips sent rows, respects the limit and skips non push modes', () => {
    register('sender', 'receiver');
    const m1 = core.send('sender', { threadId: null, to: ['receiver'], body: 'first' });
    core.send('sender', { threadId: null, to: ['receiver'], body: 'second' });
    core.send('sender', { threadId: null, to: ['receiver'], body: 'notice', mode: 'notify' });
    const verify = Store.open(dbPath);
    try {
      verify.updateDelivery(m1.result.messageId, 'receiver', { state: 'sent', sentAt: 1 });
    } finally {
      verify.close();
    }
    const capped = core.pendingPush('receiver', 1);
    expect(capped.messages.map((m) => m.body)).toEqual(['second']);
    // A sent delivery already left through a transport: never pulled again.
    expect(core.pendingPush('receiver', 10).messages).toEqual([]);
    expect(changes.filter((c) => c.to === 'delivered')).toHaveLength(1);
  });
});

describe('default clock', () => {
  it('falls back to the wall clock when none is injected', () => {
    const events: CoreEvents = {
      onAppend: () => undefined,
      onStateChange: () => undefined,
    };
    const wallCore = new MurmurCore(store, events);
    expect(wallCore.groups('list')).toEqual([]);
    // The default process table reads the real ps output.
    expect(wallCore.sessionForAncestors([process.pid])).toBeNull();
  });
});

describe('defensive store guards', () => {
  it('throws internal when an appended message vanishes before readback', () => {
    register('sender', 'beta');
    const vanished = new Proxy(store, {
      get(target, prop) {
        if (prop === 'messageById') {
          return () => null;
        }
        return Reflect.get(target, prop) as unknown;
      },
    });
    const broken = new MurmurCore(
      vanished as Store,
      { onAppend: () => undefined, onStateChange: () => undefined },
      () => now,
    );
    expectCoreError(
      () => broken.send('sender', { threadId: null, to: ['beta'], body: 'x' }),
      'internal',
      'appended message vanished',
    );
  });

  it('dedups duplicate delivery rows when collecting a thread', () => {
    register('alpha', 'beta');
    const ask = core.send('alpha', { threadId: null, to: ['beta'], body: 'pick' });
    const duplicated = new Proxy(store, {
      get(target, prop) {
        if (prop === 'getDeliveries') {
          return (messageId: number): DeliveryRow[] => {
            const rows = target.getDeliveries(messageId);
            return rows.length > 0 ? [...rows, rows[0] as DeliveryRow] : rows;
          };
        }
        return Reflect.get(target, prop) as unknown;
      },
    });
    const dupCore = new MurmurCore(
      duplicated as Store,
      { onAppend: () => undefined, onStateChange: () => undefined },
      () => now,
    );
    const matrix = dupCore.collect(ask.result.threadId);
    expect(matrix.recipients.map((r) => r.recipient)).toEqual(['beta']);
    expect(matrix.recipients).toHaveLength(1);
  });

  it('maps a non error rename failure to a bad request', () => {
    register('moving');
    const hostile = new Proxy(store, {
      get(target, prop) {
        if (prop === 'renamePeer') {
          return () => {
            throw 'disk haunted';
          };
        }
        return Reflect.get(target, prop) as unknown;
      },
    });
    const hostileCore = new MurmurCore(
      hostile as Store,
      { onAppend: () => undefined, onStateChange: () => undefined },
      () => now,
    );
    expectCoreError(() => hostileCore.register({ handle: 'moved', existing: 'moving' }), 'bad_request', 'rename failed');
  });

  it('treats a thread whose messages cannot be read as empty', () => {
    const sparse = new Proxy(store, {
      get(target, prop) {
        if (prop === 'threadMessages') {
          return (): ReturnType<Store['threadMessages']> => new Array(1) as unknown as ReturnType<Store['threadMessages']>;
        }
        return Reflect.get(target, prop) as unknown;
      },
    });
    const sparseCore = new MurmurCore(
      sparse as Store,
      { onAppend: () => undefined, onStateChange: () => undefined },
      () => now,
    );
    const view = sparseCore.threadView(7, null);
    expect(view.threadId).toBe(7);
    expect(view.latestSeq).toBe(0);
    expect(view.messages).toHaveLength(1);
  });

  it('adds members to a group that vanishes between the membership checks', () => {
    register('alpha');
    core.groups('create', 'dev');
    let listCalls = 0;
    const flaky = new Proxy(store, {
      get(target, prop) {
        if (prop === 'listGroups') {
          return () => {
            listCalls += 1;
            return listCalls === 1 ? target.listGroups() : [];
          };
        }
        return Reflect.get(target, prop) as unknown;
      },
    });
    const flakyCore = new MurmurCore(
      flaky as Store,
      { onAppend: () => undefined, onStateChange: () => undefined },
      () => now,
    );
    flakyCore.groups('add', 'dev', ['alpha']);
    expect(core.groups('list')[0]?.members).toEqual(['alpha']);
  });
});

describe('resolveRecipients bare names and groups', () => {
  it('falls back to a group for a bare name no peer has', () => {
    register('alpha', 'beta');
    core.groups('create', 'team');
    core.groups('add', 'team', ['alpha', 'beta']);
    expect(core.resolveRecipients(['team'])).toEqual(['alpha', 'beta']);
    expectCoreError(() => core.resolveRecipients(['ghost']), 'unknown_handle', 'ghost');
  });

  it('prefers the peer for a bare name but always uses the group for @name', () => {
    register('alpha', 'team');
    core.groups('create', 'team');
    core.groups('add', 'team', ['alpha']);
    expect(core.resolveRecipients(['team'])).toEqual(['team']);
    expect(core.resolveRecipients(['@team'])).toEqual(['alpha']);
    register('lonely');
    expectCoreError(() => core.resolveRecipients(['@lonely']), 'unknown_group', 'lonely');
  });
});

describe('reply fan-out', () => {
  it('reaches every other participant with a rendered text and links the parent', () => {
    register('alpha', 'beta', 'gamma');
    const ask = core.send('alpha', { threadId: null, to: ['beta', 'gamma'], body: 'decide' });
    const { result, pushes } = core.reply('beta', { messageId: ask.result.messageId, body: 'yes', verdict: 'agree', references: ['r1'] });
    expect(pushes.map((p) => [p.recipient, p.kind]).sort()).toEqual([
      ['alpha', 'connection'],
      ['gamma', 'connection'],
    ]);
    expect(pushes[0]?.text).toContain(
      'm' + String(result.messageId) + ' t' + String(result.threadId) + ' s1 from beta re m' + String(ask.result.messageId) + ' AGREE to gamma, alpha]',
    );
    expect(pushes[0]?.text).toContain('refs: r1');
    expect(core.deliveriesOf(result.messageId).map((d) => [d.recipient, d.state])).toEqual([
      ['alpha', 'queued'],
      ['gamma', 'queued'],
    ]);
    expect(core.deliveriesOf(ask.result.messageId).find((d) => d.recipient === 'beta')?.state).toBe('replied');
  });

  it('includes later participants and drops handles that are no longer peers', () => {
    register('alpha', 'beta', 'gamma');
    const ask = core.send('alpha', { threadId: null, to: ['beta', 'gamma'], body: 'q' });
    core.reply('gamma', { messageId: ask.result.messageId, body: 'one' });
    store.deletePeer('beta');
    const { pushes } = core.reply('alpha', { messageId: ask.result.messageId, body: 'two' });
    expect(pushes.map((p) => p.recipient)).toEqual(['gamma']);
  });

  it('uses the parent mode, native transports and truncates the delivered body', () => {
    register('alpha', 'beta');
    core.registerNative({ handle: 'alpha', socketPath: join(dir, 'a.sock'), token: 't' });
    const ask = core.send('alpha', { threadId: null, to: ['beta'], body: 'q', mode: 'notify' });
    const body = 'y'.repeat(MAX_BODY_CHARS + 5);
    const { result, pushes } = core.reply('beta', { messageId: ask.result.messageId, body });
    expect(pushes.map((p) => p.kind)).toEqual(['queued']);
    expect(pushes[0]?.text).toContain('[truncated');
    expect(pushes[0]?.message.body).toBe(body);
    expect(store.messageById(result.messageId)).toMatchObject({ mode: 'notify', truncated: true, body });
    const push = core.send('alpha', { threadId: null, to: ['beta'], body: 'q2' });
    expect(core.reply('beta', { messageId: push.result.messageId, body: 'r' }).pushes.map((p) => p.kind)).toEqual(['native']);
  });

  it('applies the send limits to replies', () => {
    register('alpha', 'beta');
    const ask = core.send('alpha', { threadId: null, to: ['beta'], body: 'q' });
    core.reply('beta', { messageId: ask.result.messageId, body: 'same' });
    expectCoreError(() => core.reply('beta', { messageId: ask.result.messageId, body: 'same' }), 'duplicate');
    unreadDeliveries('alpha', UNREAD_CAP);
    expectCoreError(() => core.reply('beta', { messageId: ask.result.messageId, body: 'other' }), 'cap_inbox', 'alpha');
  });
});

describe('threadView digest', () => {
  it('returns summaries without bodies when full is false', () => {
    register('alpha', 'beta');
    const { result } = core.send('alpha', { threadId: null, to: ['beta'], body: 'line one\nline two' });
    const digest = core.threadView(result.threadId, null, false).messages[0];
    expect(digest).toMatchObject({ firstLine: 'line one' });
    expect(digest).not.toHaveProperty('body');
    const full = core.threadView(result.threadId, null).messages[0];
    expect(full).toEqual(fullOf(store.messageById(result.messageId) as never));
    expect(full).toHaveProperty('body', 'line one\nline two');
  });
});

describe('sessionEvent', () => {
  it('creates a session with an allocated handle, a peer and an idle state', () => {
    const host = alive(100);
    const row = core.sessionEvent({
      event: 'session-start', client: 'claude', cliSessionId: 'c1', host, transcriptPath: '/t.jsonl', cwd: '/home/me/My Proj!',
    });
    expect(row).toMatchObject({
      client: 'claude', cliSessionId: 'c1', handle: 'claude-my-proj', hostPid: 100, hostStart: 'S100',
      transcriptPath: '/t.jsonl', cwd: '/home/me/My Proj!', state: 'idle', stateAt: now,
    });
    expect(store.getPeer('claude-my-proj')).not.toBeNull();
  });

  it('maps prompt to busy and session-end to ended and drops the native transport', () => {
    const host = alive(100);
    const native = { socketPath: '/n.sock', token: 'tok' };
    const start = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'c1', host, native });
    expect(start.handle).toBe('claude');
    expect(store.getNativeTransport('claude')).toEqual({ owner: 'claude', socketPath: '/n.sock', token: 'tok' });
    advance(10);
    expect(core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'c1', host })).toMatchObject({ state: 'busy', stateAt: now });
    expect(store.getNativeTransport('claude')).not.toBeNull();
    expect(core.sessionEvent({ event: 'stop', client: 'claude', cliSessionId: 'c1', host }).state).toBe('idle');
    expect(core.sessionEvent({ event: 'session-end', client: 'claude', cliSessionId: 'c1', host }).state).toBe('ended');
    expect(store.getNativeTransport('claude')).toBeNull();
  });

  it('finds a resumed session by its cli id and adopts the new host and fields', () => {
    const first = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'c1', host: alive(100), cwd: '/a' });
    const host2 = alive(200);
    const resumed = core.sessionEvent({
      event: 'session-start', client: 'claude', cliSessionId: 'c1', host: host2, transcriptPath: '/new.jsonl', cwd: '/b',
    });
    expect(resumed).toMatchObject({ id: first.id, handle: first.handle, hostPid: 200, hostStart: 'S200', transcriptPath: '/new.jsonl', cwd: '/b' });
    expect(store.listSessions()).toHaveLength(1);
  });

  it('keeps the session across clear (new cli id, same host) and with a null id', () => {
    const host = alive(100);
    const first = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'c1', host, cwd: '/a', transcriptPath: '/t' });
    const cleared = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'c2', host });
    expect(cleared).toMatchObject({ id: first.id, cliSessionId: 'c2', cwd: '/a', transcriptPath: '/t' });
    const nullId = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: null, host });
    expect(nullId).toMatchObject({ id: first.id, cliSessionId: 'c2' });
  });

  it('honors a claimable explicit handle on create and falls back when it is not claimable', () => {
    const owner = core.register({ handle: 'boss' });
    store.upsertPeer('boss', null, null, true, now);
    expect(owner.handle).toBe('boss');
    const a = core.sessionEvent({ event: 'session-start', client: 'codex', cliSessionId: null, host: alive(1), handle: 'custom' });
    expect(a.handle).toBe('custom');
    const b = core.sessionEvent({ event: 'session-start', client: 'codex', cliSessionId: null, host: alive(2), handle: 'boss', cwd: '/x/repo' });
    expect(b.handle).toBe('codex-repo');
    const c = core.sessionEvent({ event: 'session-start', client: 'codex', cliSessionId: null, host: alive(3), handle: 'custom', cwd: '/x/repo' });
    expect(c.handle).toBe('codex-repo-2');
    const d = core.sessionEvent({ event: 'session-start', client: 'codex', cliSessionId: null, host: alive(4), handle: 'BAD' });
    expect(d.handle).toBe('codex');
  });

  it('renames an existing session to an explicit claimable handle only', () => {
    const host = alive(1);
    const live2 = core.sessionEvent({ event: 'session-start', client: 'gemini', cliSessionId: 'g2', host: alive(2), handle: 'taken' });
    const row = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'c1', host });
    const same = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'c1', host, handle: row.handle });
    expect(same.handle).toBe(row.handle);
    const blocked = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'c1', host, handle: live2.handle });
    expect(blocked.handle).toBe(row.handle);
    const renamed = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'c1', host, handle: 'nice-name' });
    expect(renamed.handle).toBe('nice-name');
    expect(store.getPeer(row.handle)).toBeNull();
  });
});

describe('renameSession', () => {
  function session(pid: number, client: 'claude' | 'codex' = 'claude', cwd = '/p' + String(pid)): ReturnType<MurmurCore['sessionEvent']> {
    return core.sessionEvent({ event: 'session-start', client, cliSessionId: 'id' + String(pid), host: alive(pid), cwd });
  }

  it('rejects invalid handles and returns the same handle untouched', () => {
    const row = session(1);
    expectCoreError(() => core.renameSession(row, 'BAD HANDLE'), 'bad_request', 'invalid handle');
    expect(core.renameSession(row, row.handle)).toBe(row.handle);
  });

  it('renames onto a free name with the whole identity', () => {
    const row = session(1);
    expect(core.renameSession(row, 'brand-new')).toBe('brand-new');
    expect(store.sessionById(row.id)?.handle).toBe('brand-new');
    expect(store.getPeer(row.handle)).toBeNull();
  });

  it('refuses the owner handle and a handle held by a live session', () => {
    const row = session(1);
    const other = session(2, 'codex');
    store.upsertPeer('boss', null, null, true, now);
    expectCoreError(() => core.renameSession(row, 'boss'), 'bad_request', 'taken');
    expectCoreError(() => core.renameSession(row, other.handle), 'bad_request', 'taken');
  });

  it('takes over a dead peer and deletes the abandoned peer when it has no history', () => {
    const row = session(1);
    const dead = session(2, 'codex');
    procs.delete(2);
    store.upsertNativeTransport(row.handle, '/r.sock', 'rt', now);
    expect(core.renameSession(row, dead.handle)).toBe(dead.handle);
    expect(store.sessionById(row.id)?.handle).toBe(dead.handle);
    expect(store.getPeer('claude-p1')).toBeNull();
    expect(store.getNativeTransport(dead.handle)?.token).toBe('rt');
  });

  it('keeps the abandoned peer when it has history or other sessions', () => {
    const row = session(1);
    const dead = session(2, 'codex');
    procs.delete(2);
    store.createGroup('grp', now);
    store.addMember('grp', row.handle, now);
    core.renameSession(row, dead.handle);
    expect(store.getPeer('claude-p1')).not.toBeNull();

    const row3 = session(3);
    const sibling = alive(4);
    store.insertSession({ client: 'claude', cliSessionId: 's4', handle: row3.handle, hostPid: sibling.hostPid, hostStart: sibling.hostStart, transcriptPath: null, cwd: null, now });
    const dead2 = session(5, 'codex');
    procs.delete(5);
    core.renameSession(row3, dead2.handle);
    expect(store.getPeer(row3.handle)).not.toBeNull();
  });

  it('can claim a registered peer that has no session at all', () => {
    const row = session(1);
    register('idle-peer');
    expect(core.renameSession(row, 'idle-peer')).toBe('idle-peer');
    expect(store.getPeer('claude-p1')).toBeNull();
  });
});

describe('allocateHandle', () => {
  const start = (pid: number, client: 'claude' | 'codex', cwd?: string): string =>
    core.sessionEvent({
      event: 'session-start', client, cliSessionId: 'x' + String(pid), host: alive(pid), ...(cwd !== undefined ? { cwd } : {}),
    }).handle;

  it('uses the client alone without a cwd or with an unusable one', () => {
    expect(start(1, 'claude')).toBe('claude');
    expect(start(2, 'claude', '/')).toBe('claude-2');
    expect(start(3, 'claude', '/tmp/---')).toBe('claude-3');
  });

  it('sanitizes and truncates folder names without a trailing dash', () => {
    expect(start(1, 'codex', '/w/__Weird..Name__')).toBe('codex-weird-name');
    expect(start(2, 'claude', '/w/' + 'a'.repeat(20) + '-' + 'b'.repeat(10))).toBe('claude-' + 'a'.repeat(20));
    expect(start(3, 'claude', '/w/' + 'c'.repeat(40))).toBe('claude-' + 'c'.repeat(21));
  });

  it('adds numeric suffixes while live sessions hold the name and reuses dead ones', () => {
    expect(start(1, 'claude', '/w/app')).toBe('claude-app');
    expect(start(2, 'claude', '/w/app')).toBe('claude-app-2');
    expect(start(3, 'claude', '/w/app')).toBe('claude-app-3');
    procs.delete(2);
    expect(start(4, 'claude', '/w/app')).toBe('claude-app-2');
  });
});

describe('sessionForHost and sessionForAncestors', () => {
  it('finds a session by host and optionally creates one', () => {
    const host = alive(10);
    expect(core.sessionForHost(host)).toBeNull();
    const created = core.sessionForHost(host, { client: 'zcode' });
    expect(created).toMatchObject({ client: 'zcode', handle: 'zcode', state: 'idle', cwd: null });
    expect(core.sessionForHost(host, { client: 'zcode' })?.id).toBe(created?.id);
    expect(core.sessionForHost(alive(11), { client: 'codex', cwd: '/w/thing' })?.handle).toBe('codex-thing');
  });

  it('returns the nearest live session among the ancestors', () => {
    const outer = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'o', host: alive(10) });
    const inner = core.sessionEvent({ event: 'session-start', client: 'codex', cliSessionId: 'i', host: alive(20) });
    expect(core.sessionForAncestors([20, 10])?.id).toBe(inner.id);
    expect(core.sessionForAncestors([30, 10])?.id).toBe(outer.id);
    expect(core.sessionForAncestors([30, 40])).toBeNull();
    expect(core.sessionForAncestors([])).toBeNull();
  });

  it('skips ended and dead sessions and survives a failing process table', () => {
    const a = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'a', host: alive(10) });
    const b = core.sessionEvent({ event: 'session-start', client: 'codex', cliSessionId: 'b', host: alive(20) });
    core.sessionEvent({ event: 'session-end', client: 'codex', cliSessionId: 'b', host: alive(20) });
    expect(core.sessionForAncestors([20, 10])?.id).toBe(a.id);
    procs.delete(10);
    expect(core.sessionForAncestors([20, 10])).toBeNull();
    alive(10);
    expect(b.id).not.toBe(a.id);
    procThrows = true;
    expect(core.sessionForAncestors([10])).toBeNull();
  });
});

describe('status from sessions', () => {
  const peerOf = (handle: string): ReturnType<MurmurCore['status']>['peers'][number] => {
    const found = core.status(() => ({ online: false })).peers.find((p) => p.handle === handle);
    expect(found).toBeDefined();
    return found as ReturnType<MurmurCore['status']>['peers'][number];
  };

  it('reports busy, idle and online from the live session state', () => {
    const host = alive(1);
    const row = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'c', host, handle: 'worker' });
    expect(peerOf('worker')).toMatchObject({ presence: 'busy', busySince: now, idleSince: null, client: 'claude' });
    advance(50);
    core.sessionEvent({ event: 'stop', client: 'claude', cliSessionId: 'c', host });
    expect(peerOf('worker')).toMatchObject({ presence: 'idle', idleSince: now, busySince: null, lastActivityAgeMs: 0 });
    const raw = new Database(dbPath);
    try {
      raw.prepare('UPDATE sessions SET state = NULL, state_at = NULL WHERE id = ?').run(row.id);
    } finally {
      raw.close();
    }
    expect(peerOf('worker')).toMatchObject({ presence: 'online', busySince: null, idleSince: null });
  });

  it('ignores ended sessions, dead hosts and a failing process table', () => {
    const host = alive(1);
    core.sessionEvent({ event: 'prompt', client: 'codex', cliSessionId: 'c', host, handle: 'worker' });
    core.sessionEvent({ event: 'session-end', client: 'codex', cliSessionId: 'c', host });
    expect(peerOf('worker')).toMatchObject({ presence: 'presumed-offline', client: 'codex' });
    core.sessionEvent({ event: 'prompt', client: 'codex', cliSessionId: 'c', host });
    expect(peerOf('worker').presence).toBe('busy');
    procs.delete(1);
    expect(peerOf('worker').presence).toBe('presumed-offline');
    procs.set(1, { ppid: 1, start: 'S1', comm: 'x' });
    expect(peerOf('worker').presence).toBe('busy');
    procThrows = true;
    expect(peerOf('worker').presence).toBe('presumed-offline');
  });

  it('reports a null client for peers without sessions', () => {
    register('plain');
    expect(peerOf('plain').client).toBeNull();
  });
});
