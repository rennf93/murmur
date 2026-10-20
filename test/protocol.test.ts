import { describe, expect, it } from 'vitest';
import {
  FIRST_LINE_MAX,
  MAX_BODY_CHARS,
  PROTOCOL_VERSION,
  LineReader,
  MurmurError,
  encodeLine,
  firstLine,
  newToken,
  HANDLE_RE,
  PROOF_POLL_MS,
  deliveryTag,
  replyFooter,
  renderDelivery,
  type MessageSummary,
  truncateForDelivery,
} from '../src/protocol.js';

describe('wire constants', () => {
  it('exposes the expected protocol values', () => {
    expect(PROTOCOL_VERSION).toBe(2);
    expect(MAX_BODY_CHARS).toBe(4000);
    expect(FIRST_LINE_MAX).toBe(120);
  });
});

describe('LineReader', () => {
  it('reassembles a line delivered in partial chunks', () => {
    const reader = new LineReader();
    expect(reader.feed('hel')).toEqual([]);
    expect(reader.feed('lo')).toEqual([]);
    expect(reader.feed('\n')).toEqual(['hello']);
  });

  it('returns several complete lines from one chunk', () => {
    const reader = new LineReader();
    expect(reader.feed('a\nbb\nccc\n')).toEqual(['a', 'bb', 'ccc']);
    expect(reader.feed('')).toEqual([]);
  });

  it('skips empty lines and trims surrounding whitespace', () => {
    const reader = new LineReader();
    expect(reader.feed('\n\n\n   \nx\n\n  y  \n')).toEqual(['x', 'y']);
  });

  it('keeps a trailing partial line buffered across calls', () => {
    const reader = new LineReader();
    expect(reader.feed('one\ntwo')).toEqual(['one']);
    expect(reader.feed('\nthree\n')).toEqual(['two', 'three']);
  });
});

describe('firstLine', () => {
  it('takes the first line and trims it', () => {
    expect(firstLine('hello\nworld\n')).toBe('hello');
    expect(firstLine('  padded  \nnext')).toBe('padded');
    expect(firstLine('solo')).toBe('solo');
    expect(firstLine('')).toBe('');
  });

  it('caps the line at 120 characters', () => {
    expect(firstLine('x'.repeat(121))).toBe('x'.repeat(120));
    expect(firstLine('x'.repeat(121) + '\nrest')).toBe('x'.repeat(120));
    expect(firstLine('y'.repeat(120) + '\nrest')).toBe('y'.repeat(120));
    expect(firstLine('y'.repeat(120))).toHaveLength(120);
  });
});

describe('truncateForDelivery', () => {
  it('leaves a body at exactly the cap untouched', () => {
    const body = 'a'.repeat(MAX_BODY_CHARS);
    expect(truncateForDelivery(body)).toEqual({ delivered: body, truncated: false });
    const under = 'a'.repeat(MAX_BODY_CHARS - 1);
    expect(truncateForDelivery(under)).toEqual({ delivered: under, truncated: false });
  });

  it('truncates a body past the cap and appends the marker', () => {
    const body = 'b'.repeat(MAX_BODY_CHARS + 1);
    const out = truncateForDelivery(body);
    expect(out.truncated).toBe(true);
    expect(out.delivered.startsWith('b'.repeat(MAX_BODY_CHARS))).toBe(true);
    expect(out.delivered.endsWith('\n[truncated: full body stored, read it with murmur_thread full: true]')).toBe(true);
    expect(out.delivered).toHaveLength(MAX_BODY_CHARS + '\n[truncated: full body stored, read it with murmur_thread full: true]'.length);
  });
});

describe('encodeLine', () => {
  it('appends a trailing newline to the JSON payload', () => {
    expect(encodeLine({ v: 1, type: 'auth' })).toBe('{"v":1,"type":"auth"}\n');
    expect(encodeLine('plain')).toBe('"plain"\n');
  });
});

describe('MurmurError', () => {
  it('carries the wire error code on an Error', () => {
    const err = new MurmurError('bad_token', 'nope');
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('MurmurError');
    expect(err.code).toBe('bad_token');
    expect(err.message).toBe('nope');
  });

  it('preserves the code for every kind of value', () => {
    expect(new MurmurError('internal', 'm').code).toBe('internal');
    expect(new MurmurError('cap_inbox', 'm').code).toBe('cap_inbox');
  });
});

describe('newToken', () => {
  it('returns 64 hex characters and differs across calls', () => {
    const token = newToken();
    expect(token).toMatch(/^[0-9a-f]{64}$/);
    expect(newToken()).not.toBe(token);
  });
});

describe('v0.2 helpers', () => {
  it('exposes HANDLE_RE and PROOF_POLL_MS', () => {
    expect(PROOF_POLL_MS).toBe(120000);
    expect(HANDLE_RE.test('claude-app')).toBe(true);
    expect(HANDLE_RE.test('A')).toBe(false);
    expect(HANDLE_RE.test('-ab')).toBe(false);
  });

  it('deliveryTag ends with a space so m42 never matches m420', () => {
    expect(deliveryTag(42)).toBe('[murmur m42 ');
    expect('[murmur m420 '.includes(deliveryTag(42))).toBe(false);
  });

  it('replyFooter names the reply and thread commands', () => {
    expect(replyFooter({ id: 7, threadId: 3 })).toBe(
      '(reply: murmur_reply messageId 7, or from a shell: murmur reply 7 <text>; whole thread: murmur_thread 3)',
    );
  });

  const base: MessageSummary = {
    id: 5, threadId: 2, seq: 1, sender: 'alice', firstLine: 'x', truncated: false,
    references: [], isReply: false, inReplyTo: null, verdict: null, createdAt: 0,
  };

  it('renderDelivery renders the minimal form', () => {
    expect(renderDelivery(base, 'body', ['bob'])).toBe(
      '[murmur m5 t2 s1 from alice to bob]\nbody\n' + replyFooter(base),
    );
  });

  it('renderDelivery renders parent, verdict, several recipients and refs', () => {
    const m = { ...base, inReplyTo: 4, verdict: 'agree' as const, references: ['a.ts', 'b.ts'] };
    expect(renderDelivery(m, 'body', ['bob', 'carol'])).toBe(
      '[murmur m5 t2 s1 from alice re m4 AGREE to bob, carol]\nbody\nrefs: a.ts, b.ts\n' + replyFooter(m),
    );
  });
});
