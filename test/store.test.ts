import Database from 'better-sqlite3';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Store } from '../src/store.js';

let dir: string;
let dbPath: string;
let store: Store;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'murmur-store-'));
  dbPath = join(dir, 'murmur.db');
  store = Store.open(dbPath);
});

afterEach(() => {
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

interface AppendOpts {
  sender?: string;
  body?: string;
  refs?: string[];
  mode?: string;
  isReply?: boolean;
  inReplyTo?: number | null;
  verdict?: string | null;
  truncated?: boolean;
  createdAt?: number;
}

function appendMessage(threadId: number, opts: AppendOpts = {}): { id: number; seq: number } {
  return store.appendMessage({
    threadId,
    sender: opts.sender ?? 's',
    body: opts.body ?? 'body',
    deliveredBody: opts.body ?? 'body',
    truncated: opts.truncated ?? false,
    refs: opts.refs ?? [],
    mode: opts.mode ?? 'push',
    isReply: opts.isReply ?? false,
    inReplyTo: opts.inReplyTo ?? null,
    verdict: opts.verdict ?? null,
    createdAt: opts.createdAt ?? 1,
  });
}

// Intercepts db.prepare so the matching aggregate query returns no row from
// get(), exercising the defensive fallbacks against an impossible-by-SQLite
// state. The real statement handles everything else.
function withMissingGetRow(matches: (sql: string) => boolean, run: () => void): void {
  const anyStore = store as unknown as { db: Database.Database; statements: Map<string, unknown> };
  // Statements are cached per SQL text: start from an empty cache so the
  // patched prepare is reached, and drop what it produced afterwards.
  anyStore.statements.clear();
  const realPrepare = anyStore.db.prepare.bind(anyStore.db);
  const patched = ((sql: string) => {
    const stmt = realPrepare(sql);
    if (matches(sql)) {
      const wrapped = Object.create(stmt) as typeof stmt;
      wrapped.get = () => undefined;
      return wrapped;
    }
    return stmt;
  }) as unknown as typeof anyStore.db.prepare;
  anyStore.db.prepare = patched;
  try {
    run();
  } finally {
    anyStore.db.prepare = realPrepare;
    anyStore.statements.clear();
  }
}

describe('Store', () => {
  it('creates the full schema and turns on WAL and foreign keys', () => {
    const raw = new Database(dbPath);
    try {
      const tables = (
        raw.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all() as Array<{
          name: string;
        }>
      ).map((r) => r.name);
      for (const table of ['deliveries', 'group_members', 'groups', 'messages', 'native_transports', 'peers', 'sessions', 'threads']) {
        expect(tables).toContain(table);
      }
      expect(raw.pragma('journal_mode', { simple: true })).toBe('wal');
      expect(raw.pragma('foreign_keys', { simple: true })).toBe(1);
    } finally {
      raw.close();
    }
  });

  it('upserts peers, refreshing profile fields and never dropping the owner flag', () => {
    store.upsertPeer('alpha', 'Alpha', 'dev', false, 10);
    expect(store.getPeer('alpha')).toEqual({
      handle: 'alpha',
      display: 'Alpha',
      role: 'dev',
      isOwner: false,
      createdAt: 10,
      lastSeenAt: 10,
      lastHookActivityAt: null,
      hookState: null,
    });
    store.upsertPeer('alpha', null, null, true, 20);
    expect(store.getPeer('alpha')).toMatchObject({ display: 'Alpha', role: 'dev', isOwner: true, lastSeenAt: 20 });
    store.upsertPeer('alpha', 'Again', 'boss', false, 30);
    expect(store.getPeer('alpha')).toMatchObject({ display: 'Again', role: 'boss', isOwner: true, lastSeenAt: 30 });
    store.upsertPeer('beta', null, null, false, 40);
    expect(store.listPeers().map((p) => p.handle)).toEqual(['alpha', 'beta']);
    expect(store.getPeer('missing')).toBeNull();
  });

  it('renames a peer and repoints the native transport atomically', () => {
    store.upsertPeer('old', 'Display', null, false, 5);
    store.upsertNativeTransport('old', '/tmp/native.sock', 'tok', 6);
    store.renamePeer('old', 'new');
    expect(store.getPeer('old')).toBeNull();
    expect(store.getPeer('new')).toMatchObject({ display: 'Display' });
    expect(store.getNativeTransport('old')).toBeNull();
    expect(store.getNativeTransport('new')).toEqual({ owner: 'new', socketPath: '/tmp/native.sock', token: 'tok' });
  });

  it('refuses to rename onto an occupied handle and allows a same-handle rename', () => {
    store.upsertPeer('a', null, null, false, 1);
    store.upsertPeer('b', null, null, false, 2);
    expect(() => store.renamePeer('a', 'b')).toThrowError('peer handle already exists: b');
    expect(() => store.renamePeer('a', 'a')).not.toThrowError();
    expect(store.getPeer('a')).not.toBeNull();
    expect(store.getPeer('b')).not.toBeNull();
  });

  it('runs the group lifecycle without stale members after recreate', () => {
    store.createGroup('dev', 1);
    store.createGroup('ops', 2);
    store.addMember('dev', 'zeta', 3);
    store.addMember('dev', 'alpha', 3);
    store.addMember('ops', 'beta', 4);
    expect(store.listGroups()).toEqual([
      { name: 'dev', createdAt: 1, members: ['alpha', 'zeta'] },
      { name: 'ops', createdAt: 2, members: ['beta'] },
    ]);
    store.removeMember('dev', 'zeta');
    expect(store.listGroups()[0]?.members).toEqual(['alpha']);
    store.deleteGroup('dev');
    expect(store.listGroups().map((g) => g.name)).toEqual(['ops']);
    store.createGroup('dev', 9);
    expect(store.listGroups().find((g) => g.name === 'dev')?.members).toEqual([]);
  });

  it('assigns per-thread seq values monotonically across interleaved appends', () => {
    const threadA = store.createThread('s', 1);
    const threadB = store.createThread('s', 1);
    const ids: number[] = [];
    ids.push(appendMessage(threadA, { createdAt: 2 }).id);
    ids.push(appendMessage(threadB, { createdAt: 3 }).id);
    expect(store.threadMessages(threadA, null).map((m) => m.seq)).toEqual([0]);
    expect(store.threadMessages(threadB, null).map((m) => m.seq)).toEqual([0]);
    ids.push(appendMessage(threadA, { createdAt: 4 }).id);
    ids.push(appendMessage(threadA, { createdAt: 5 }).id);
    ids.push(appendMessage(threadB, { createdAt: 6 }).id);
    expect(store.threadMessages(threadA, null).map((m) => m.seq)).toEqual([0, 1, 2]);
    expect(store.threadMessages(threadB, null).map((m) => m.seq)).toEqual([0, 1]);
    for (let i = 1; i < ids.length; i++) {
      expect(ids[i]).toBeGreaterThan(ids[i - 1] as number);
    }
  });

  it('stores and maps message fields round trip', () => {
    const tid = store.createThread('s', 1);
    const { id, seq } = appendMessage(tid, {
      sender: 'alice',
      body: 'full body',
      refs: ['r1', 'r2'],
      mode: 'notify',
      isReply: true,
      inReplyTo: 42,
      verdict: 'agree',
      truncated: true,
      createdAt: 7,
    });
    expect(seq).toBe(0);
    const row = store.messageById(id);
    expect(row).toEqual({
      id,
      threadId: tid,
      seq: 0,
      sender: 'alice',
      body: 'full body',
      deliveredBody: 'full body',
      truncated: true,
      refsJson: '["r1","r2"]',
      mode: 'notify',
      isReply: true,
      inReplyTo: 42,
      verdict: 'agree',
      createdAt: 7,
    });
    expect(store.messageById(99999)).toBeNull();
    expect(store.messageCount(tid)).toBe(1);
    expect(store.messageCount(99999)).toBe(0);
  });

  it('adds queued delivery rows and applies partial patches', () => {
    const tid = store.createThread('s', 1);
    const { id } = appendMessage(tid, {});
    store.addDeliveries(id, ['b', 'a'], 99);
    const rows = store.getDeliveries(id);
    expect(rows.map((r) => r.recipient)).toEqual(['a', 'b']);
    expect(rows[0]).toEqual({
      messageId: id,
      recipient: 'a',
      state: 'queued',
      sentAt: null,
      deliveredAt: null,
      ackedAt: null,
      repliedAt: null,
      latencyMs: null,
    });
    store.updateDelivery(id, 'a', {});
    expect(store.getDeliveries(id)[0]?.state).toBe('queued');
    store.updateDelivery(id, 'a', { state: 'sent', sentAt: 5 });
    store.updateDelivery(id, 'a', { state: 'delivered', deliveredAt: 6 });
    store.updateDelivery(id, 'a', { ackedAt: 7 });
    store.updateDelivery(id, 'a', { state: 'replied', repliedAt: 8, latencyMs: 9 });
    expect(store.getDeliveries(id)[0]).toEqual({
      messageId: id,
      recipient: 'a',
      state: 'replied',
      sentAt: 5,
      deliveredAt: 6,
      ackedAt: 7,
      repliedAt: 8,
      latencyMs: 9,
    });
    expect(store.getDeliveries(424242)).toEqual([]);
  });

  it('marks delivered, acked and replied with timestamps', () => {
    const tid = store.createThread('s', 1);
    const { id } = appendMessage(tid, {});
    store.addDeliveries(id, ['r'], 1);
    store.markDelivered(id, 'r', 10);
    expect(store.getDeliveries(id)[0]).toMatchObject({ state: 'delivered', deliveredAt: 10 });
    store.markAcked(id, 'r', 11);
    expect(store.getDeliveries(id)[0]).toMatchObject({ state: 'acknowledged', ackedAt: 11 });
    store.markReplied(id, 'r', 12, 250);
    expect(store.getDeliveries(id)[0]).toMatchObject({ state: 'replied', repliedAt: 12, latencyMs: 250 });
  });

  it('filters thread messages by sinceSeq', () => {
    const tid = store.createThread('s', 1);
    appendMessage(tid, { createdAt: 1 });
    appendMessage(tid, { createdAt: 2 });
    appendMessage(tid, { createdAt: 3 });
    expect(store.threadMessages(tid, null)).toHaveLength(3);
    expect(store.threadMessages(tid, 1).map((m) => m.seq)).toEqual([2]);
    expect(store.threadMessages(tid, 999)).toEqual([]);
    expect(store.threadMessages(424242, null)).toEqual([]);
  });

  it('finds the latest ask and the thread recipient handles', () => {
    const tid = store.createThread('s', 1);
    const ask1 = appendMessage(tid, {});
    const reply = appendMessage(tid, { isReply: true, inReplyTo: ask1.id });
    const ask2 = appendMessage(tid, {});
    expect(store.latestAsk(tid)?.id).toBe(ask2.id);
    expect(store.latestAsk(424242)).toBeNull();
    store.addDeliveries(ask2.id, ['beta', 'alpha'], 1);
    store.addDeliveries(reply.id, ['gamma'], 1);
    expect(store.threadRecipientHandles(tid)).toEqual(['alpha', 'beta', 'gamma']);
    expect(store.threadRecipientHandles(424242)).toEqual([]);
  });

  it('scopes readMessages to the recipient and skips empty id lists', () => {
    const tid = store.createThread('s', 1);
    const m1 = appendMessage(tid, { createdAt: 1 });
    const m2 = appendMessage(tid, { createdAt: 2 });
    store.addDeliveries(m1.id, ['a'], 1);
    store.addDeliveries(m2.id, ['b'], 1);
    expect(store.readMessages('a', [])).toEqual([]);
    expect(store.readMessages('a', [m1.id, m2.id]).map((m) => m.id)).toEqual([m1.id]);
    expect(store.readMessages('b', [m1.id, m2.id]).map((m) => m.id)).toEqual([m2.id]);
    expect(store.readMessages('nobody', [m1.id])).toEqual([]);
  });

  it('computes reply latencies per recipient and skips null rows', () => {
    const tid = store.createThread('s', 1);
    const m1 = appendMessage(tid, {});
    const m2 = appendMessage(tid, {});
    const m3 = appendMessage(tid, {});
    store.addDeliveries(m1.id, ['r'], 1);
    store.addDeliveries(m2.id, ['r'], 1);
    store.addDeliveries(m3.id, ['q'], 1);
    store.markReplied(m1.id, 'r', 10, 100);
    store.markReplied(m2.id, 'r', 11, 300);
    store.markReplied(m3.id, 'q', 12, 999);
    expect(store.replyLatencies('r')).toEqual([100, 300]);
    expect(store.replyLatencies('q')).toEqual([999]);
    expect(store.replyLatencies('nobody')).toEqual([]);
  });

  it('lists pending push messages filtered, ordered and capped', () => {
    const tid = store.createThread('s', 1);
    const m1 = appendMessage(tid, { createdAt: 1 });
    const m2 = appendMessage(tid, { createdAt: 2 });
    const m3 = appendMessage(tid, { createdAt: 3, mode: 'notify' });
    const m4 = appendMessage(tid, { createdAt: 4 });
    const m5 = appendMessage(tid, { createdAt: 5 });
    for (const message of [m1, m2, m3, m4, m5]) {
      store.addDeliveries(message.id, ['r'], 1);
    }
    store.markDelivered(m5.id, 'r', 6);
    expect(store.pendingPushFor('r', 10).map((e) => e.message.id)).toEqual([m4.id, m2.id, m1.id]);
    expect(store.pendingPushFor('r', 2).map((e) => e.message.id)).toEqual([m4.id, m2.id]);
    store.markReplied(m4.id, 'r', 9, 5);
    expect(store.pendingPushFor('r', 10).map((e) => e.message.id)).toEqual([m2.id, m1.id]);
    store.updateDelivery(m1.id, 'r', { state: 'sent' });
    expect(store.pendingPushFor('r', 10).map((e) => e.message.id)).toEqual([m2.id]);
    expect(store.pendingPushFor('nobody', 10)).toEqual([]);
  });

  it('computes inbox rows and unread totals regardless of the filter flag', () => {
    const tid = store.createThread('s', 1);
    const m1 = appendMessage(tid, { createdAt: 1 });
    const m2 = appendMessage(tid, { createdAt: 2 });
    const m3 = appendMessage(tid, { createdAt: 3 });
    const m4 = appendMessage(tid, { createdAt: 4 });
    for (const message of [m1, m2, m3, m4]) {
      store.addDeliveries(message.id, ['r'], 1);
    }
    store.markDelivered(m2.id, 'r', 5);
    store.markAcked(m3.id, 'r', 6);
    store.markReplied(m4.id, 'r', 7, 10);
    expect(store.inboxFor('r', true).messages.map((m) => m.id)).toEqual([m1.id, m2.id]);
    expect(store.inboxFor('r', false).messages).toHaveLength(4);
    expect(store.inboxFor('r', true).unreadTotal).toBe(2);
    expect(store.inboxFor('r', false).unreadTotal).toBe(2);
    expect(store.inboxFor('nobody', false)).toEqual({ messages: [], unreadTotal: 0 });
  });

  it('counts delivery states per recipient for explicit state lists', () => {
    const tid = store.createThread('s', 1);
    const m1 = appendMessage(tid, {});
    const m2 = appendMessage(tid, {});
    store.addDeliveries(m1.id, ['r'], 1);
    store.addDeliveries(m2.id, ['r'], 1);
    store.markDelivered(m1.id, 'r', 2);
    expect(store.deliveryStatesForRecipient('r', ['queued'])).toBe(1);
    expect(store.deliveryStatesForRecipient('r', ['queued', 'delivered'])).toBe(2);
    expect(store.deliveryStatesForRecipient('r', [])).toBe(0);
    expect(store.deliveryStatesForRecipient('nobody', ['queued'])).toBe(0);
  });

  it('keeps message history and deliveries when a group is deleted', () => {
    store.upsertPeer('alpha', null, null, false, 1);
    store.upsertPeer('beta', null, null, false, 2);
    store.createGroup('dev', 3);
    store.addMember('dev', 'beta', 4);
    const tid = store.createThread('alpha', 5);
    const { id } = appendMessage(tid, { sender: 'alpha' });
    store.addDeliveries(id, ['beta'], 6);
    store.deleteGroup('dev');
    expect(store.listGroups()).toEqual([]);
    expect(store.messageCount(tid)).toBe(1);
    expect(store.getDeliveries(id)).toHaveLength(1);
    expect(store.threadRecipientHandles(tid)).toEqual(['beta']);
  });

  it('upserts, reads and deletes native transports', () => {
    store.upsertNativeTransport('a', '/first.sock', 't1', 1);
    store.upsertNativeTransport('a', '/second.sock', 't2', 2);
    expect(store.getNativeTransport('a')).toEqual({ owner: 'a', socketPath: '/second.sock', token: 't2' });
    expect(store.getNativeTransport('b')).toBeNull();
    store.deleteNativeTransport('a');
    expect(store.getNativeTransport('a')).toBeNull();
  });

  it('falls back to zero counts when an aggregate row is missing', () => {
    const tid = store.createThread('s', 1);
    appendMessage(tid, {});
    store.addDeliveries(1, ['r'], 1);
    withMissingGetRow((sql) => sql.includes('COUNT(*)'), () => {
      expect(store.messageCount(tid)).toBe(0);
      expect(store.deliveryStatesForRecipient('r', ['queued'])).toBe(0);
    });
    expect(store.messageCount(tid)).toBe(1);
    expect(store.deliveryStatesForRecipient('r', ['queued'])).toBe(1);
  });

  it('falls back to seq 0 when the seq row is missing', () => {
    const tid = store.createThread('s', 1);
    withMissingGetRow((sql) => sql.includes('next_seq'), () => {
      expect(appendMessage(tid, {}).seq).toBe(0);
    });
    expect(store.threadMessages(tid, null).map((m) => m.seq)).toEqual([0]);
  });

  it('restricts the database and its wal and shm files to the owner', () => {
    store.upsertPeer('a', null, null, false, 1);
    for (const file of [dbPath, dbPath + '-wal', dbPath + '-shm']) {
      expect(statSync(file).mode & 0o777).toBe(0o600);
    }
  });

  it('cascades a rename through every table that references the handle', () => {
    store.upsertPeer('old', null, null, false, 1);
    store.upsertPeer('other', null, null, false, 1);
    const sid = store.insertSession({
      client: 'claude', cliSessionId: 'c1', handle: 'old', hostPid: 5, hostStart: 'S', transcriptPath: null, cwd: null, now: 1,
    });
    store.createGroup('dev', 1);
    store.addMember('dev', 'old', 1);
    const tid = store.createThread('old', 1);
    const m = appendMessage(tid, { sender: 'old' });
    const m2 = appendMessage(tid, { sender: 'other' });
    store.addDeliveries(m2.id, ['old'], 1);
    store.upsertNativeTransport('old', '/n.sock', 't', 1);
    store.renamePeer('old', 'fresh');
    expect(store.sessionById(sid)?.handle).toBe('fresh');
    expect(store.listGroups()[0]?.members).toEqual(['fresh']);
    expect(store.messageById(m.id)?.sender).toBe('fresh');
    expect(store.getDeliveries(m2.id)[0]?.recipient).toBe('fresh');
    expect(store.getNativeTransport('fresh')?.token).toBe('t');
    const raw = new Database(dbPath, { readonly: true });
    try {
      expect(raw.prepare('SELECT created_by FROM threads WHERE id = ?').get(tid)).toEqual({ created_by: 'fresh' });
    } finally {
      raw.close();
    }
  });

  it('inserts, finds, lists and patches sessions', () => {
    const a = store.insertSession({
      client: 'claude', cliSessionId: 'c1', handle: 'h', hostPid: 10, hostStart: 'T', transcriptPath: '/t', cwd: '/w', now: 5,
    });
    const b = store.insertSession({
      client: 'codex', cliSessionId: null, handle: 'h', hostPid: 10, hostStart: 'T', transcriptPath: null, cwd: null, now: 9,
    });
    expect(store.sessionById(a)).toEqual({
      id: a, client: 'claude', cliSessionId: 'c1', handle: 'h', hostPid: 10, hostStart: 'T',
      transcriptPath: '/t', cwd: '/w', state: null, stateAt: null, startedAt: 5, updatedAt: 5,
    });
    expect(store.sessionById(999)).toBeNull();
    expect(store.sessionByCliId('claude', 'c1')?.id).toBe(a);
    expect(store.sessionByCliId('codex', 'c1')).toBeNull();
    expect(store.sessionByHost(10, 'T')?.id).toBe(b);
    expect(store.sessionByHost(10, 'other')).toBeNull();
    expect(store.sessionsForHandle('h').map((r) => r.id)).toEqual([b, a]);
    expect(store.sessionsForHandle('nobody')).toEqual([]);
    expect(store.listSessions().map((r) => r.id)).toEqual([b, a]);
    store.updateSession(a, {});
    expect(store.sessionById(a)?.client).toBe('claude');
    store.updateSession(a, {
      client: 'gemini', cliSessionId: 'c2', hostPid: 11, hostStart: 'U', transcriptPath: null, cwd: null,
      state: 'busy', stateAt: 20, updatedAt: 30,
    });
    expect(store.sessionById(a)).toEqual({
      id: a, client: 'gemini', cliSessionId: 'c2', handle: 'h', hostPid: 11, hostStart: 'U',
      transcriptPath: null, cwd: null, state: 'busy', stateAt: 20, startedAt: 5, updatedAt: 30,
    });
  });

  it('reassigns a session, moving the native transport to the target handle', () => {
    const id = store.insertSession({
      client: 'claude', cliSessionId: null, handle: 'from', hostPid: 1, hostStart: 'T', transcriptPath: null, cwd: null, now: 1,
    });
    store.upsertNativeTransport('from', '/f.sock', 'tf', 1);
    store.upsertNativeTransport('to', '/t.sock', 'tt', 1);
    store.reassignSession(id, 'from', 'to');
    expect(store.sessionById(id)?.handle).toBe('to');
    expect(store.getNativeTransport('from')).toBeNull();
    expect(store.getNativeTransport('to')).toEqual({ owner: 'to', socketPath: '/f.sock', token: 'tf' });
  });

  it('reports history and deletes a peer with its native transport', () => {
    store.upsertPeer('p', null, null, false, 1);
    expect(store.peerHasHistory('p')).toBe(false);
    store.createGroup('g', 1);
    store.addMember('g', 'p', 1);
    expect(store.peerHasHistory('p')).toBe(true);
    store.removeMember('g', 'p');
    const tid = store.createThread('x', 1);
    appendMessage(tid, { sender: 'p' });
    expect(store.peerHasHistory('p')).toBe(true);
    const other = appendMessage(tid, { sender: 'x' });
    store.addDeliveries(other.id, ['q'], 1);
    expect(store.peerHasHistory('q')).toBe(true);
    store.upsertNativeTransport('p', '/s', 't', 1);
    store.deletePeer('p');
    expect(store.getPeer('p')).toBeNull();
    expect(store.getNativeTransport('p')).toBeNull();
  });
  // Each hot read or write must reach its rows through an index. The SQL is
  // taken from the statement cache after one call, so the plan checked is the
  // plan that runs.
  it('plans every hot statement without a full table scan', () => {
    const anyStore = store as unknown as { statements: Map<string, unknown> };
    const plansOf = (call: () => void): string[] => {
      anyStore.statements.clear();
      call();
      const raw = new Database(dbPath);
      try {
        return [...anyStore.statements.keys()].flatMap((sql) => {
          const marks = Array.from({ length: (sql.match(/\?/g) ?? []).length }, () => 1);
          const rows = raw.prepare('EXPLAIN QUERY PLAN ' + sql).all(...marks) as Array<{ detail: string }>;
          return rows.map((r) => r.detail);
        });
      } finally {
        raw.close();
      }
    };
    store.upsertPeer('a', null, null, false, 1);
    const tid = store.createThread('a', 1);
    const { id } = appendMessage(tid, { sender: 'a' });
    store.addDeliveries(id, ['b'], 1);
    store.insertSession({ client: 'claude', cliSessionId: 'x', handle: 'b', hostPid: 1, hostStart: 'S', transcriptPath: null, cwd: null, now: 1 });
    const calls: Array<[string, () => void]> = [
      ['pendingPushFor', () => store.pendingPushFor('b', 10)],
      ['deliveryStatesForRecipient', () => store.deliveryStatesForRecipient('b', ['queued', 'sent'])],
      ['getDeliveries', () => store.getDeliveries(id)],
      ['inboxFor', () => store.inboxFor('b', true)],
      ['replyLatencies', () => store.replyLatencies('b')],
      ['threadRecipientHandles', () => store.threadRecipientHandles(tid)],
      ['threadMessages', () => store.threadMessages(tid, 0)],
      ['latestAsk', () => store.latestAsk(tid)],
      ['messageCount', () => store.messageCount(tid)],
      ['messageById', () => store.messageById(id)],
      ['appendMessage', () => appendMessage(tid)],
      ['sessionByHost', () => store.sessionByHost(1, 'S')],
      ['sessionByCliId', () => store.sessionByCliId('claude', 'x')],
      ['sessionsForHandle', () => store.sessionsForHandle('b')],
      ['markDelivered', () => store.markDelivered(id, 'b', 2)],
    ];
    for (const [name, call] of calls) {
      const scans = plansOf(call).filter((detail) => /^SCAN\b/.test(detail));
      expect(scans, name).toEqual([]);
    }
  });
});
