import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ensureHome, getPaths, readToken, verifyPerms } from '../src/config.js';
import { MurmurError } from '../src/protocol.js';

// Injectable failure hooks for the mocked fs calls. Everything else
// delegates to the real node:fs implementation.
const fsHooks = vi.hoisted(() => ({
  failChmod: (_path: string): Error | null => null,
  failWrite: (_path: string): Error | null => null,
}));

vi.mock('node:fs', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs')>();
  return {
    ...actual,
    chmodSync(path: Parameters<typeof actual.chmodSync>[0], mode: Parameters<typeof actual.chmodSync>[1]): void {
      const failure = fsHooks.failChmod(String(path));
      if (failure !== null) {
        throw failure;
      }
      actual.chmodSync(path, mode);
    },
    writeFileSync(...args: Parameters<typeof actual.writeFileSync>): ReturnType<typeof actual.writeFileSync> {
      const failure = fsHooks.failWrite(String(args[0]));
      if (failure !== null) {
        throw failure;
      }
      return actual.writeFileSync(...args);
    },
  };
});

let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'murmur-config-'));
  fsHooks.failChmod = () => null;
  fsHooks.failWrite = () => null;
});

afterEach(() => {
  vi.unstubAllEnvs();
  fsHooks.failChmod = () => null;
  fsHooks.failWrite = () => null;
  rmSync(dir, { recursive: true, force: true });
});

function expectMurmurError(fn: () => void, code: string, messagePart?: string): void {
  let caught: unknown;
  try {
    fn();
  } catch (err) {
    caught = err;
  }
  expect(caught).toBeInstanceOf(MurmurError);
  expect((caught as MurmurError).code).toBe(code);
  if (messagePart !== undefined) {
    expect((caught as MurmurError).message).toContain(messagePart);
  }
}

describe('getPaths', () => {
  it('prefers the explicit argument over the env and the default', () => {
    vi.stubEnv('MURMUR_HOME', join(dir, 'from-env'));
    const paths = getPaths(join(dir, 'from-arg'));
    expect(paths.home).toBe(join(dir, 'from-arg'));
    expect(paths.socketPath).toBe(join(dir, 'from-arg', 'bus.sock'));
    expect(paths.dbPath).toBe(join(dir, 'from-arg', 'murmur.db'));
    expect(paths.tokenPath).toBe(join(dir, 'from-arg', 'token'));
  });

  it('falls back to MURMUR_HOME when no argument is given', () => {
    vi.stubEnv('MURMUR_HOME', join(dir, 'from-env'));
    expect(getPaths().home).toBe(join(dir, 'from-env'));
  });

  it('falls back to ~/.murmur when neither is set', () => {
    vi.stubEnv('MURMUR_HOME', undefined);
    expect(getPaths().home).toBe(join(homedir(), '.murmur'));
  });
});

describe('ensureHome', () => {
  it('creates the home with mode 0700 and a fresh token with mode 0600', () => {
    const paths = getPaths(join(dir, 'home'));
    ensureHome(paths);
    expect(statSync(paths.home).mode & 0o777).toBe(0o700);
    expect(statSync(paths.tokenPath).mode & 0o777).toBe(0o600);
    expect(readFileSync(paths.tokenPath, 'utf8')).toMatch(/^[0-9a-f]{64}$/);
  });

  it('is idempotent and keeps the existing token', () => {
    const paths = getPaths(join(dir, 'home'));
    ensureHome(paths);
    const token = readFileSync(paths.tokenPath, 'utf8');
    ensureHome(paths);
    expect(readFileSync(paths.tokenPath, 'utf8')).toBe(token);
    expect(statSync(paths.home).mode & 0o777).toBe(0o700);
    expect(statSync(paths.tokenPath).mode & 0o777).toBe(0o600);
  });

  it('throws when the home mode cannot be set', () => {
    const paths = getPaths(join(dir, 'home'));
    fsHooks.failChmod = (path) => (path === paths.home ? new Error('denied by policy') : null);
    expect(() => ensureHome(paths)).toThrowError(/cannot set mode 0700/);
  });

  it('throws when the token mode cannot be set', () => {
    const paths = getPaths(join(dir, 'home'));
    fsHooks.failChmod = (path) => (path === paths.tokenPath ? new Error('denied by policy') : null);
    expect(() => ensureHome(paths)).toThrowError(/cannot set mode 0600/);
  });

  it('survives a lost token creation race with EEXIST', () => {
    const paths = getPaths(join(dir, 'home'));
    fsHooks.failWrite = (path) => {
      if (path === paths.tokenPath) {
        const err = new Error('file exists') as NodeJS.ErrnoException;
        err.code = 'EEXIST';
        return err;
      }
      return null;
    };
    expect(() => ensureHome(paths)).not.toThrowError();
  });

  it('throws when the token cannot be written', () => {
    const paths = getPaths(join(dir, 'home'));
    fsHooks.failWrite = (path) => (path === paths.tokenPath ? new Error('disk full') : null);
    expect(() => ensureHome(paths)).toThrowError(/cannot create token file/);
  });
});

describe('readToken', () => {
  it('reads and trims the token', () => {
    const paths = getPaths(join(dir, 'home'));
    mkdirSync(paths.home, { recursive: true });
    writeFileSync(paths.tokenPath, '  abc123 \n', 'utf8');
    expect(readToken(paths)).toBe('abc123');
  });

  it('throws bad_token when the file is missing', () => {
    const paths = getPaths(join(dir, 'home'));
    mkdirSync(paths.home, { recursive: true });
    expectMurmurError(() => readToken(paths), 'bad_token', 'cannot read token file');
  });

  it('throws bad_token when the file is empty', () => {
    const paths = getPaths(join(dir, 'home'));
    mkdirSync(paths.home, { recursive: true });
    writeFileSync(paths.tokenPath, '   \n', 'utf8');
    expectMurmurError(() => readToken(paths), 'bad_token', 'is empty');
  });
});

describe('verifyPerms', () => {
  it('passes after ensureHome', () => {
    const paths = getPaths(join(dir, 'home'));
    ensureHome(paths);
    expect(() => verifyPerms(paths)).not.toThrowError();
  });

  it('fails when the token file mode is 0644', () => {
    const paths = getPaths(join(dir, 'home'));
    ensureHome(paths);
    chmodSync(paths.tokenPath, 0o644);
    expectMurmurError(() => verifyPerms(paths), 'internal', 'expected 600');
  });

  it('fails when the home directory mode is not 0700', () => {
    const paths = getPaths(join(dir, 'home'));
    ensureHome(paths);
    chmodSync(paths.home, 0o755);
    expectMurmurError(() => verifyPerms(paths), 'internal', 'expected 700');
  });

  it('fails when the paths cannot be stat-ed', () => {
    const paths = getPaths(join(dir, 'missing-home'));
    expectMurmurError(() => verifyPerms(paths), 'internal', 'cannot stat');
  });
});
