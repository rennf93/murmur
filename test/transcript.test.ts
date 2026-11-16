import { execFileSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, utimesSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { hasToolUse, newestWriteMs, subagentTranscripts, tailLines, usableTranscript } from '../src/transcript.js';

let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'murmur-transcript-'));
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

const use = (id: string): string => JSON.stringify({ type: 'assistant', message: { content: [{ type: 'tool_use', id }] } });

function write(name: string, lines: string[]): string {
  const path = join(dir, name);
  writeFileSync(path, lines.join('\n') + '\n');
  return path;
}

describe('usableTranscript and tailLines', () => {
  it('accept a regular .jsonl file and nothing else', () => {
    const good = write('s.jsonl', ['{}']);
    const text = write('s.txt', ['{}']);
    const link = join(dir, 'link.jsonl');
    symlinkSync(good, link);
    const fifo = join(dir, 'fifo.jsonl');
    execFileSync('mkfifo', [fifo]);
    expect(usableTranscript(good)).toBe(true);
    for (const bad of [text, link, fifo, dir, join(dir, 'missing.jsonl')]) {
      expect(usableTranscript(bad)).toBe(false);
      expect(tailLines(bad, 1024)).toBeNull();
    }
  });

  it('returns whole lines only', () => {
    const path = write('t.jsonl', ['first-line', 'x'.repeat(100), 'last-line']);
    expect(tailLines(path, 1 << 20)).toEqual(['first-line', 'x'.repeat(100), 'last-line', '']);
    expect(tailLines(path, 40)).toEqual(['last-line', '']);
  });

  it.skipIf(process.getuid?.() === 0)('reads as no when the file cannot be opened', () => {
    const path = write('locked.jsonl', ['{}']);
    chmodSync(path, 0o000);
    expect(usableTranscript(path)).toBe(true);
    expect(tailLines(path, 10)).toBeNull();
  });
});

describe('hasToolUse', () => {
  it('matches the exact id of a tool_use block, in the transcript or a subagent transcript', () => {
    const path = write('s.jsonl', [use('toolu_real'), JSON.stringify({ type: 'user', message: { content: [{ type: 'text', text: 'toolu_quoted' }] } }), 'not json "toolu_junk"', JSON.stringify({ message: { content: 'toolu_str' } })]);
    mkdirSync(join(dir, 's', 'subagents'), { recursive: true });
    writeFileSync(join(dir, 's', 'subagents', 'agent-1.jsonl'), use('toolu_sub') + '\n');
    writeFileSync(join(dir, 's', 'subagents', 'agent-1.txt'), use('toolu_txt') + '\n');
    expect(hasToolUse(path, 'toolu_real')).toBe(true);
    expect(hasToolUse(path, 'toolu_sub')).toBe(true);
    for (const id of ['toolu_re', 'toolu_realx', 'toolu_quoted', 'toolu_junk', 'toolu_str', 'toolu_txt']) {
      expect(hasToolUse(path, id)).toBe(false);
    }
    expect(subagentTranscripts(path)).toEqual([join(dir, 's', 'subagents', 'agent-1.jsonl')]);
    expect(subagentTranscripts(join(dir, 'none.jsonl'))).toEqual([]);
  });
});

describe('newestWriteMs', () => {
  it('is the newest of the transcript and its subagent transcripts, or null', () => {
    const path = write('s.jsonl', ['{}']);
    const old = new Date(Date.now() - 3 * 3600 * 1000);
    utimesSync(path, old, old);
    expect(newestWriteMs(path)).toBeLessThan(Date.now() - 2 * 3600 * 1000);
    mkdirSync(join(dir, 's', 'subagents'), { recursive: true });
    writeFileSync(join(dir, 's', 'subagents', 'agent-1.jsonl'), '{}\n');
    expect(newestWriteMs(path)).toBeGreaterThan(Date.now() - 60 * 1000);
    expect(newestWriteMs(join(dir, 'none.jsonl'))).toBeNull();
  });
});
