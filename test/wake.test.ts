import { beforeEach, describe, expect, it, vi } from 'vitest';

const execFile = vi.hoisted(() => vi.fn());
vi.mock('node:child_process', () => ({ execFile }));

import { defaultWake } from '../src/wake.js';
import type { SessionRow } from '../src/store.js';

const session = (client: string, cliSessionId: string | null): SessionRow => ({ client, cliSessionId }) as SessionRow;

beforeEach(() => {
  execFile.mockReset();
});

describe('defaultWake', () => {
  it('resolves false without running anything for a non-codex client', async () => {
    await expect(defaultWake(session('claude', 'abc'), 'hi')).resolves.toBe(false);
    expect(execFile).not.toHaveBeenCalled();
  });

  it('resolves false for codex without a cliSessionId', async () => {
    await expect(defaultWake(session('codex', null), 'hi')).resolves.toBe(false);
    expect(execFile).not.toHaveBeenCalled();
  });

  it('queues on the codex thread and resolves true on exit 0', async () => {
    execFile.mockImplementation((_c: string, _a: string[], _o: object, cb: (e: Error | null) => void) => cb(null));
    await expect(defaultWake(session('codex', 'thr-1'), 'hello')).resolves.toBe(true);
    expect(execFile).toHaveBeenCalledWith(
      'codex',
      ['queue', '--thread', 'thr-1', '--message', 'hello'],
      { timeout: 10000 },
      expect.any(Function),
    );
  });

  it('resolves false when codex fails', async () => {
    execFile.mockImplementation((_c: string, _a: string[], _o: object, cb: (e: Error | null) => void) => cb(new Error('x')));
    await expect(defaultWake(session('codex', 'thr-1'), 'hello')).resolves.toBe(false);
  });
});
