import { closeSync, openSync, readSync, statSync } from 'node:fs';
import { PROOF_POLL_MS, deliveryTag } from './protocol.js';

// Delivery proof from the receiver's side. After a push, the broker watches
// the receiving session's own transcript for the message tag; finding it is
// the evidence that the session took the message in, which no socket ack
// can show. Polls for a while, then waits for that session's next lifecycle
// event (recheck) with no deadline.

interface Watch {
  messageId: number;
  recipient: string;
  path: string;
  tag: string;
  offset: number;
  pollUntil: number;
  createdAt: number;
}

// Watches nobody proved within a day are dropped; the delivery stays sent.
const FORGET_AFTER_MS = 24 * 60 * 60 * 1000;

export class ProofWatcher {
  private readonly watches = new Map<string, Watch>();
  private timer: NodeJS.Timeout | null = null;
  private readonly onProof: (messageId: number, recipient: string) => void;
  private readonly pollMs: number;
  private readonly now: () => number;

  constructor(onProof: (messageId: number, recipient: string) => void, pollMs = 1000, now: () => number = Date.now) {
    this.onProof = onProof;
    this.pollMs = pollMs;
    this.now = now;
  }

  // The transcript size right now. Taken before a push so the watch starts
  // below anything the push can cause.
  static offsetOf(path: string): number {
    try {
      return statSync(path).size;
    } catch {
      return 0;
    }
  }

  watch(messageId: number, recipient: string, path: string, offset: number): void {
    const ts = this.now();
    this.watches.set(String(messageId) + '\u0000' + recipient, {
      messageId,
      recipient,
      path,
      tag: deliveryTag(messageId),
      offset,
      pollUntil: ts + PROOF_POLL_MS,
      createdAt: ts,
    });
    this.timer ??= setInterval(() => this.tick(), this.pollMs);
  }

  // Scans every pending watch of this recipient now, past its poll window
  // too. Called on the recipient's hook events.
  recheck(recipient: string): void {
    for (const watch of [...this.watches.values()]) {
      if (watch.recipient === recipient) {
        this.scan(watch);
      }
    }
  }

  // The recipient's handle changed: its pending watches follow it.
  rename(from: string, to: string): void {
    for (const [key, watch] of [...this.watches]) {
      if (watch.recipient === from) {
        this.watches.delete(key);
        watch.recipient = to;
        this.watches.set(String(watch.messageId) + '\u0000' + to, watch);
      }
    }
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.watches.clear();
  }

  get size(): number {
    return this.watches.size;
  }

  private tick(): void {
    const ts = this.now();
    for (const watch of [...this.watches.values()]) {
      if (ts - watch.createdAt > FORGET_AFTER_MS) {
        this.drop(watch);
      } else if (ts <= watch.pollUntil) {
        this.scan(watch);
      }
    }
    if (![...this.watches.values()].some((w) => ts <= w.pollUntil) && this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  // Reads only what was appended since the last scan, keeping a tag-sized
  // overlap so a tag split across two scans is still found.
  private scan(watch: Watch): void {
    // Most scans find nothing new, so the size check comes before the open.
    let size: number;
    try {
      size = statSync(watch.path).size;
    } catch {
      return;
    }
    if (size <= watch.offset) {
      return;
    }
    let fd: number;
    try {
      fd = openSync(watch.path, 'r');
    } catch {
      return;
    }
    try {
      const buf = Buffer.alloc(size - watch.offset);
      readSync(fd, buf, 0, buf.length, watch.offset);
      if (buf.includes(watch.tag)) {
        this.drop(watch);
        this.onProof(watch.messageId, watch.recipient);
        return;
      }
      watch.offset = Math.max(watch.offset, size - Buffer.byteLength(watch.tag));
    } finally {
      closeSync(fd);
    }
  }

  private drop(watch: Watch): void {
    this.watches.delete(String(watch.messageId) + '\u0000' + watch.recipient);
  }
}
