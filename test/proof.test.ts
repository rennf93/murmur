import { appendFileSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { PROOF_POLL_MS, deliveryTag } from '../src/protocol.js';
import { ProofWatcher } from '../src/proof.js';

// An open that fails after a successful stat; file modes cannot force it
// when the suite runs as root (CI containers).
const fsState = vi.hoisted(() => ({ failOpen: false }));
vi.mock('node:fs', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs')>();
  return {
    ...actual,
    openSync: (...args: Parameters<typeof actual.openSync>) => {
      if (fsState.failOpen) {
        throw Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' });
      }
      return actual.openSync(...args);
    },
  };
});

const DAY = 24 * 60 * 60 * 1000;
let dir: string;
let file: string;
let clock: number;
let hits: Array<[number, string]>;
let w: ProofWatcher;

beforeEach(() => {
  vi.useFakeTimers();
  dir = mkdtempSync(join(tmpdir(), 'mm-proof-'));
  file = join(dir, 't.jsonl');
  writeFileSync(file, 'old content\n');
  clock = 1000;
  hits = [];
  w = new ProofWatcher((id, r) => hits.push([id, r]), 10, () => clock);
});

afterEach(() => {
  w.stop();
  vi.useRealTimers();
  rmSync(dir, { recursive: true, force: true });
});

describe('ProofWatcher', () => {
  it('offsetOf is the file size, or 0 when missing', () => {
    expect(ProofWatcher.offsetOf(file)).toBe(12);
    expect(ProofWatcher.offsetOf(join(dir, 'none'))).toBe(0);
  });

  it('finds the tag by polling, fires once and drops the watch', () => {
    w.watch(5, 'bob', file, ProofWatcher.offsetOf(file));
    expect(w.size).toBe(1);
    vi.advanceTimersByTime(10);
    expect(hits).toEqual([]);
    appendFileSync(file, 'x ' + deliveryTag(5) + 't1 ...\n');
    vi.advanceTimersByTime(10);
    expect(hits).toEqual([[5, 'bob']]);
    expect(w.size).toBe(0);
    vi.advanceTimersByTime(50);
    expect(hits).toHaveLength(1);
  });

  it('rename rekeys the matching watches only', () => {
    w.watch(5, 'bob', file, ProofWatcher.offsetOf(file));
    w.watch(6, 'carol', file, ProofWatcher.offsetOf(file));
    w.rename('bob', 'dave');
    expect(w.size).toBe(2);
    appendFileSync(file, deliveryTag(5) + deliveryTag(6) + '\n');
    vi.advanceTimersByTime(10);
    expect(hits).toEqual([
      [6, 'carol'],
      [5, 'dave'],
    ]);
  });

  it('ignores a tag that sits before the starting offset and a prefix like m50', () => {
    appendFileSync(file, deliveryTag(5));
    w.watch(5, 'bob', file, ProofWatcher.offsetOf(file));
    appendFileSync(file, deliveryTag(50) + '\n');
    vi.advanceTimersByTime(10);
    expect(hits).toEqual([]);
  });

  it('finds a tag split across two scans through the overlap', () => {
    w.watch(7, 'bob', file, ProofWatcher.offsetOf(file));
    const tag = deliveryTag(7);
    appendFileSync(file, 'zzzz' + tag.slice(0, 5));
    vi.advanceTimersByTime(10);
    expect(hits).toEqual([]);
    appendFileSync(file, tag.slice(5) + 'rest');
    vi.advanceTimersByTime(10);
    expect(hits).toEqual([[7, 'bob']]);
  });

  it('tolerates a missing file and a file that shrank', () => {
    w.watch(1, 'bob', join(dir, 'missing'), 0);
    w.watch(2, 'bob', file, 9999);
    vi.advanceTimersByTime(10);
    expect(hits).toEqual([]);
    expect(w.size).toBe(2);
  });

  it('tolerates a transcript that grew but cannot be opened', () => {
    w.watch(6, 'bob', file, 0);
    fsState.failOpen = true;
    try {
      vi.advanceTimersByTime(10);
      expect(hits).toEqual([]);
      expect(w.size).toBe(1);
    } finally {
      fsState.failOpen = false;
    }
  });

  it('stops polling after the poll window and only recheck scans, per recipient', () => {
    w.watch(3, 'bob', file, 0);
    w.watch(4, 'carol', file, 0);
    clock += PROOF_POLL_MS + 1;
    vi.advanceTimersByTime(10);
    appendFileSync(file, deliveryTag(3) + deliveryTag(4));
    vi.advanceTimersByTime(100);
    expect(hits).toEqual([]);
    w.recheck('bob');
    expect(hits).toEqual([[3, 'bob']]);
    expect(w.size).toBe(1);
    w.recheck('carol');
    expect(hits).toEqual([[3, 'bob'], [4, 'carol']]);
  });

  it('keeps the timer while any watch is inside its window', () => {
    w.watch(3, 'bob', file, 0);
    clock += PROOF_POLL_MS + 1;
    w.watch(4, 'bob', file, 0);
    vi.advanceTimersByTime(10);
    appendFileSync(file, deliveryTag(4));
    vi.advanceTimersByTime(10);
    expect(hits).toEqual([[4, 'bob']]);
  });

  it('forgets a watch after 24 hours', () => {
    w.watch(3, 'bob', file, 0);
    clock += DAY + 1;
    vi.advanceTimersByTime(10);
    expect(w.size).toBe(0);
  });

  it('stop clears watches and the timer, and is safe twice', () => {
    w.watch(3, 'bob', file, 0);
    w.stop();
    expect(w.size).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
    w.stop();
  });

  it('uses Date.now and a 1s poll by default', () => {
    const d = new ProofWatcher(() => undefined);
    d.watch(1, 'bob', file, 0);
    expect(vi.getTimerCount()).toBe(1);
    d.stop();
  });
});
