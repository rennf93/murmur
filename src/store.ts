import Database from 'better-sqlite3';

// SQLite persistence layer for the murmur broker. better-sqlite3 is fully
// synchronous, so every method here is sync. All timestamps are epoch
// milliseconds stored as INTEGER. Booleans are stored as 0/1 because SQLite
// has no boolean type and better-sqlite3 refuses to bind JS booleans.

export interface PeerRow {
  handle: string;
  display: string | null;
  role: string | null;
  isOwner: boolean;
  createdAt: number;
  lastSeenAt: number | null;
  lastHookActivityAt: number | null;
  hookState: string | null;
}

export interface DeliveryRow {
  messageId: number;
  recipient: string;
  state: string;
  sentAt: number | null;
  deliveredAt: number | null;
  ackedAt: number | null;
  repliedAt: number | null;
  latencyMs: number | null;
}

export interface MessageRow {
  id: number;
  threadId: number;
  seq: number;
  sender: string;
  body: string;
  deliveredBody: string;
  truncated: boolean;
  refsJson: string;
  mode: string;
  isReply: boolean;
  inReplyTo: number | null;
  verdict: string | null;
  createdAt: number;
}

// Raw row shapes as they come back from SQLite (snake_case, 0/1 booleans).
interface PeerRecord {
  handle: string;
  display: string | null;
  role: string | null;
  is_owner: number;
  created_at: number;
  last_seen_at: number | null;
  last_hook_activity_at: number | null;
  hook_state: string | null;
}

interface GroupRecord {
  name: string;
  created_at: number;
}

interface HandleRecord {
  handle: string;
}

interface MessageRecord {
  id: number;
  thread_id: number;
  seq: number;
  sender: string;
  body: string;
  delivered_body: string;
  truncated: number;
  refs_json: string;
  mode: string;
  is_reply: number;
  in_reply_to: number | null;
  verdict: string | null;
  created_at: number;
}

interface DeliveryRecord {
  message_id: number;
  recipient: string;
  state: string;
  sent_at: number | null;
  delivered_at: number | null;
  acked_at: number | null;
  replied_at: number | null;
  latency_ms: number | null;
}

interface NativeTransportRecord {
  owner: string;
  socket_path: string;
  token: string;
}

interface NextSeqRecord {
  next_seq: number;
}

interface CountRecord {
  n: number;
}

interface LatencyRecord {
  latency_ms: number | null;
}

interface AppendMessageArgs {
  threadId: number;
  sender: string;
  body: string;
  deliveredBody: string;
  truncated: boolean;
  refs: string[];
  mode: string;
  isReply: boolean;
  inReplyTo: number | null;
  verdict: string | null;
  createdAt: number;
}

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS peers (
  handle TEXT PRIMARY KEY,
  display TEXT,
  role TEXT,
  is_owner INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  last_seen_at INTEGER,
  last_hook_activity_at INTEGER,
  hook_state TEXT
);
CREATE TABLE IF NOT EXISTS groups (
  name TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS group_members (
  group_name TEXT NOT NULL,
  handle TEXT NOT NULL,
  added_at INTEGER NOT NULL,
  PRIMARY KEY (group_name, handle)
);
CREATE TABLE IF NOT EXISTS threads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at INTEGER NOT NULL,
  created_by TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  thread_id INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  sender TEXT NOT NULL,
  body TEXT NOT NULL,
  delivered_body TEXT NOT NULL,
  truncated INTEGER NOT NULL,
  refs_json TEXT NOT NULL,
  mode TEXT NOT NULL,
  is_reply INTEGER NOT NULL,
  in_reply_to INTEGER,
  verdict TEXT,
  created_at INTEGER NOT NULL,
  UNIQUE (thread_id, seq)
);
CREATE TABLE IF NOT EXISTS deliveries (
  message_id INTEGER NOT NULL,
  recipient TEXT NOT NULL,
  state TEXT NOT NULL,
  sent_at INTEGER,
  delivered_at INTEGER,
  acked_at INTEGER,
  replied_at INTEGER,
  latency_ms INTEGER,
  PRIMARY KEY (message_id, recipient)
);
CREATE TABLE IF NOT EXISTS native_transports (
  owner TEXT PRIMARY KEY,
  socket_path TEXT NOT NULL,
  token TEXT NOT NULL,
  registered_at INTEGER NOT NULL
);
`;

// Delivery states that still count as unread for inbox purposes.
const UNREAD_STATES_SQL = "('queued','sent','delivered')";

function toPeerRow(r: PeerRecord): PeerRow {
  return {
    handle: r.handle,
    display: r.display,
    role: r.role,
    isOwner: r.is_owner === 1,
    createdAt: r.created_at,
    lastSeenAt: r.last_seen_at,
    lastHookActivityAt: r.last_hook_activity_at,
    hookState: r.hook_state,
  };
}

function toMessageRow(r: MessageRecord): MessageRow {
  return {
    id: r.id,
    threadId: r.thread_id,
    seq: r.seq,
    sender: r.sender,
    body: r.body,
    deliveredBody: r.delivered_body,
    truncated: r.truncated === 1,
    refsJson: r.refs_json,
    mode: r.mode,
    isReply: r.is_reply === 1,
    inReplyTo: r.in_reply_to,
    verdict: r.verdict,
    createdAt: r.created_at,
  };
}

function toDeliveryRow(r: DeliveryRecord): DeliveryRow {
  return {
    messageId: r.message_id,
    recipient: r.recipient,
    state: r.state,
    sentAt: r.sent_at,
    deliveredAt: r.delivered_at,
    ackedAt: r.acked_at,
    repliedAt: r.replied_at,
    latencyMs: r.latency_ms,
  };
}

function placeholders(n: number): string {
  // Array.from with length 0 yields an empty list and join yields '', so the
  // empty case needs no separate branch.
  return Array.from({ length: n }, () => '?').join(', ');
}

export class Store {
  private readonly db: Database.Database;
  // Serialized once in the constructor: appendMessage runs inside a
  // transaction that computes the per-thread seq.
  private readonly appendMessageTx: (arg: AppendMessageArgs) => { id: number; seq: number };

  private constructor(db: Database.Database) {
    this.db = db;
    this.appendMessageTx = db.transaction((arg: AppendMessageArgs) => this.appendMessageInner(arg));
  }

  static open(dbPath: string): Store {
    const db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');
    db.exec(SCHEMA_SQL);
    return new Store(db);
  }

  close(): void {
    this.db.close();
  }

  // Inserts the peer or refreshes display, role and last_seen_at on the
  // existing row. is_owner can only flip false to true, never back.
  upsertPeer(handle: string, display: string | null, role: string | null, isOwner: boolean, now: number): void {
    this.db
      .prepare(
        `INSERT INTO peers (handle, display, role, is_owner, created_at, last_seen_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(handle) DO UPDATE SET
           display = excluded.display,
           role = excluded.role,
           last_seen_at = excluded.last_seen_at,
           is_owner = MAX(peers.is_owner, excluded.is_owner)`,
      )
      .run(handle, display, role, isOwner ? 1 : 0, now, now);
  }

  // Moves the peers row to the new handle and repoints the native transport
  // registration, atomically. Throws a plain Error when a peer already
  // occupies the target handle. Other references (messages, group members)
  // are intentionally left as they are.
  renamePeer(oldHandle: string, newHandle: string): void {
    if (oldHandle !== newHandle) {
      const dup = this.db.prepare('SELECT handle FROM peers WHERE handle = ?').get(newHandle);
      if (dup !== undefined) {
        throw new Error('peer handle already exists: ' + newHandle);
      }
    }
    const move = this.db.transaction(() => {
      this.db.prepare('UPDATE peers SET handle = ? WHERE handle = ?').run(newHandle, oldHandle);
      this.db.prepare('UPDATE native_transports SET owner = ? WHERE owner = ?').run(newHandle, oldHandle);
    });
    move();
  }

  getPeer(handle: string): PeerRow | null {
    const rec = this.db.prepare('SELECT * FROM peers WHERE handle = ?').get(handle) as PeerRecord | undefined;
    return rec === undefined ? null : toPeerRow(rec);
  }

  listPeers(): PeerRow[] {
    const recs = this.db.prepare('SELECT * FROM peers ORDER BY handle ASC').all() as PeerRecord[];
    return recs.map(toPeerRow);
  }

  createGroup(name: string, now: number): void {
    this.db.prepare('INSERT INTO groups (name, created_at) VALUES (?, ?)').run(name, now);
  }

  // Deletes the group and its membership rows together so a recreated group
  // of the same name does not inherit stale members.
  deleteGroup(name: string): void {
    const drop = this.db.transaction(() => {
      this.db.prepare('DELETE FROM group_members WHERE group_name = ?').run(name);
      this.db.prepare('DELETE FROM groups WHERE name = ?').run(name);
    });
    drop();
  }

  addMember(group: string, handle: string, now: number): void {
    this.db.prepare('INSERT INTO group_members (group_name, handle, added_at) VALUES (?, ?, ?)').run(group, handle, now);
  }

  removeMember(group: string, handle: string): void {
    this.db.prepare('DELETE FROM group_members WHERE group_name = ? AND handle = ?').run(group, handle);
  }

  listGroups(): { name: string; createdAt: number; members: string[] }[] {
    const groups = this.db.prepare('SELECT name, created_at FROM groups ORDER BY name ASC').all() as GroupRecord[];
    const membersStmt = this.db.prepare(
      'SELECT handle FROM group_members WHERE group_name = ? ORDER BY added_at ASC, handle ASC',
    );
    return groups.map((g) => {
      const members = membersStmt.all(g.name) as HandleRecord[];
      return { name: g.name, createdAt: g.created_at, members: members.map((m) => m.handle) };
    });
  }

  createThread(createdBy: string, now: number): number {
    const info = this.db.prepare('INSERT INTO threads (created_at, created_by) VALUES (?, ?)').run(now, createdBy);
    return Number(info.lastInsertRowid);
  }

  appendMessage(arg: {
    threadId: number;
    sender: string;
    body: string;
    deliveredBody: string;
    truncated: boolean;
    refs: string[];
    mode: string;
    isReply: boolean;
    inReplyTo: number | null;
    verdict: string | null;
    createdAt: number;
  }): { id: number; seq: number } {
    return this.appendMessageTx(arg);
  }

  private appendMessageInner(arg: AppendMessageArgs): { id: number; seq: number } {
    const seqRec = this.db
      .prepare('SELECT COALESCE(MAX(seq), -1) + 1 AS next_seq FROM messages WHERE thread_id = ?')
      .get(arg.threadId) as NextSeqRecord | undefined;
    const seq = seqRec === undefined ? 0 : seqRec.next_seq;
    const info = this.db
      .prepare(
        `INSERT INTO messages (thread_id, seq, sender, body, delivered_body, truncated, refs_json, mode, is_reply, in_reply_to, verdict, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        arg.threadId,
        seq,
        arg.sender,
        arg.body,
        arg.deliveredBody,
        arg.truncated ? 1 : 0,
        JSON.stringify(arg.refs),
        arg.mode,
        arg.isReply ? 1 : 0,
        arg.inReplyTo,
        arg.verdict,
        arg.createdAt,
      );
    return { id: Number(info.lastInsertRowid), seq };
  }

  addDeliveries(messageId: number, recipients: string[], now: number): void {
    const stmt = this.db.prepare(
      `INSERT INTO deliveries (message_id, recipient, state, sent_at, delivered_at, acked_at, replied_at, latency_ms)
       VALUES (?, ?, 'queued', NULL, NULL, NULL, NULL, NULL)`,
    );
    const insertAll = this.db.transaction((recipients: string[]) => {
      for (const recipient of recipients) {
        stmt.run(messageId, recipient);
      }
    });
    // now is accepted for interface symmetry; queued rows carry no timestamp.
    void now;
    insertAll(recipients);
  }

  // Applies only the keys present in the patch; absent keys keep their
  // current values.
  updateDelivery(
    messageId: number,
    recipient: string,
    patch: {
      state?: string;
      sentAt?: number;
      deliveredAt?: number;
      ackedAt?: number;
      repliedAt?: number;
      latencyMs?: number;
    },
  ): void {
    const sets: string[] = [];
    const params: (string | number)[] = [];
    if (patch.state !== undefined) {
      sets.push('state = ?');
      params.push(patch.state);
    }
    if (patch.sentAt !== undefined) {
      sets.push('sent_at = ?');
      params.push(patch.sentAt);
    }
    if (patch.deliveredAt !== undefined) {
      sets.push('delivered_at = ?');
      params.push(patch.deliveredAt);
    }
    if (patch.ackedAt !== undefined) {
      sets.push('acked_at = ?');
      params.push(patch.ackedAt);
    }
    if (patch.repliedAt !== undefined) {
      sets.push('replied_at = ?');
      params.push(patch.repliedAt);
    }
    if (patch.latencyMs !== undefined) {
      sets.push('latency_ms = ?');
      params.push(patch.latencyMs);
    }
    if (sets.length === 0) {
      return;
    }
    params.push(messageId, recipient);
    this.db
      .prepare('UPDATE deliveries SET ' + sets.join(', ') + ' WHERE message_id = ? AND recipient = ?')
      .run(...params);
  }

  getDeliveries(messageId: number): DeliveryRow[] {
    const recs = this.db
      .prepare('SELECT * FROM deliveries WHERE message_id = ? ORDER BY recipient ASC')
      .all(messageId) as DeliveryRecord[];
    return recs.map(toDeliveryRow);
  }

  threadMessages(threadId: number, sinceSeq: number | null): MessageRow[] {
    const sql =
      'SELECT * FROM messages WHERE thread_id = ?' +
      (sinceSeq === null ? '' : ' AND seq > ?') +
      ' ORDER BY seq ASC';
    const stmt = this.db.prepare(sql);
    const recs = (sinceSeq === null ? stmt.all(threadId) : stmt.all(threadId, sinceSeq)) as MessageRecord[];
    return recs.map(toMessageRow);
  }

  latestAsk(threadId: number): MessageRow | null {
    const rec = this.db
      .prepare('SELECT * FROM messages WHERE thread_id = ? AND is_reply = 0 ORDER BY seq DESC LIMIT 1')
      .get(threadId) as MessageRecord | undefined;
    return rec === undefined ? null : toMessageRow(rec);
  }

  messageById(id: number): MessageRow | null {
    const rec = this.db.prepare('SELECT * FROM messages WHERE id = ?').get(id) as MessageRecord | undefined;
    return rec === undefined ? null : toMessageRow(rec);
  }

  messageCount(threadId: number): number {
    const rec = this.db.prepare('SELECT COUNT(*) AS n FROM messages WHERE thread_id = ?').get(threadId) as
      | CountRecord
      | undefined;
    return rec === undefined ? 0 : rec.n;
  }

  markDelivered(messageId: number, recipient: string, now: number): void {
    this.db
      .prepare("UPDATE deliveries SET state = 'delivered', delivered_at = ? WHERE message_id = ? AND recipient = ?")
      .run(now, messageId, recipient);
  }

  markAcked(messageId: number, recipient: string, now: number): void {
    this.db
      .prepare("UPDATE deliveries SET state = 'acknowledged', acked_at = ? WHERE message_id = ? AND recipient = ?")
      .run(now, messageId, recipient);
  }

  markReplied(messageId: number, recipient: string, now: number, latencyMs: number): void {
    this.db
      .prepare(
        "UPDATE deliveries SET state = 'replied', replied_at = ?, latency_ms = ? WHERE message_id = ? AND recipient = ?",
      )
      .run(now, latencyMs, messageId, recipient);
  }

  // Messages delivered to this handle. unreadTotal is the handle-wide count
  // of unread deliveries; grouping into threads is the caller's job.
  inboxFor(handle: string, unreadOnly: boolean): { messages: MessageRow[]; unreadTotal: number } {
    const unreadTotal = this.deliveryStatesForRecipient(handle, ['queued', 'sent', 'delivered']);
    const sql =
      'SELECT m.* FROM messages m JOIN deliveries d ON d.message_id = m.id WHERE d.recipient = ?' +
      (unreadOnly ? ' AND d.state IN ' + UNREAD_STATES_SQL : '') +
      ' ORDER BY m.created_at ASC, m.id ASC';
    const recs = this.db.prepare(sql).all(handle) as MessageRecord[];
    return { messages: recs.map(toMessageRow), unreadTotal };
  }

  // Returns only the requested messages that also have a delivery row for
  // this recipient, so a peer cannot read messages it was not addressed in.
  readMessages(handle: string, ids: number[]): MessageRow[] {
    if (ids.length === 0) {
      return [];
    }
    const recs = this.db
      .prepare(
        'SELECT m.* FROM messages m JOIN deliveries d ON d.message_id = m.id ' +
          'WHERE d.recipient = ? AND m.id IN (' + placeholders(ids.length) + ') ORDER BY m.id ASC',
      )
      .all(handle, ...ids) as MessageRecord[];
    return recs.map(toMessageRow);
  }

  replyLatencies(handle: string): number[] {
    const recs = this.db
      .prepare('SELECT latency_ms FROM deliveries WHERE recipient = ? AND latency_ms IS NOT NULL')
      .all(handle) as LatencyRecord[];
    return recs.map((r) => r.latency_ms).filter((v): v is number => v !== null);
  }

  // Queued or sent push-mode messages for this recipient, newest first.
  pendingPushFor(handle: string, limit: number): { message: MessageRow }[] {
    const recs = this.db
      .prepare(
        "SELECT m.* FROM messages m JOIN deliveries d ON d.message_id = m.id " +
          "WHERE d.recipient = ? AND d.state IN ('queued','sent') AND m.mode = 'push' " +
          'ORDER BY m.created_at DESC, m.id DESC LIMIT ?',
      )
      .all(handle, limit) as MessageRecord[];
    return recs.map((r) => ({ message: toMessageRow(r) }));
  }

  deliveryStatesForRecipient(recipient: string, states: string[]): number {
    if (states.length === 0) {
      return 0;
    }
    const rec = this.db
      .prepare(
        'SELECT COUNT(*) AS n FROM deliveries WHERE recipient = ? AND state IN (' + placeholders(states.length) + ')',
      )
      .get(recipient, ...states) as CountRecord | undefined;
    return rec === undefined ? 0 : rec.n;
  }

  // Every recipient that has a delivery row for any message in the thread.
  threadRecipientHandles(threadId: number): string[] {
    const recs = this.db
      .prepare(
        'SELECT DISTINCT d.recipient FROM deliveries d JOIN messages m ON m.id = d.message_id ' +
          'WHERE m.thread_id = ? ORDER BY d.recipient ASC',
      )
      .all(threadId) as Array<{ recipient: string }>;
    return recs.map((r) => r.recipient);
  }

  upsertNativeTransport(owner: string, socketPath: string, token: string, now: number): void {
    this.db
      .prepare(
        `INSERT INTO native_transports (owner, socket_path, token, registered_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(owner) DO UPDATE SET
           socket_path = excluded.socket_path,
           token = excluded.token,
           registered_at = excluded.registered_at`,
      )
      .run(owner, socketPath, token, now);
  }

  getNativeTransport(owner: string): { owner: string; socketPath: string; token: string } | null {
    const rec = this.db
      .prepare('SELECT owner, socket_path, token FROM native_transports WHERE owner = ?')
      .get(owner) as NativeTransportRecord | undefined;
    return rec === undefined ? null : { owner: rec.owner, socketPath: rec.socket_path, token: rec.token };
  }

  deleteNativeTransport(owner: string): void {
    this.db.prepare('DELETE FROM native_transports WHERE owner = ?').run(owner);
  }
}
