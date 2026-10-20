import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
import { createConnection } from 'node:net';
import type { Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Broker } from '../src/broker.js';
import { MurmurClient } from '../src/client.js';
import { ensureHome, getPaths, readToken, verifyPerms } from '../src/config.js';
import type { MurmurPaths } from '../src/config.js';
import { MurmurCore } from '../src/core.js';
import { LineReader, MurmurError, encodeLine } from '../src/protocol.js';
import { Store } from '../src/store.js';

// Security tests: file modes, token handling, the socket mode after start,
// and the 1 MiB line guard. A thin node:fs wrapper lets single chmod or
// write calls fail on demand so the error branches in ensureHome and
// Broker.start are reachable; with no flag set every call is the real one.

const fsState = vi.hoisted(() => ({
  failChmodSuffix: null as string | null,
  failWriteSuffix: null as string | null,
  writeErrorCode: 'EACCES',
}));

vi.mock('node:fs', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs')>();
  return {
    ...actual,
    chmodSync: (path: import('node:fs').PathLike, mode: import('node:fs').Mode): void => {
      if (fsState.failChmodSuffix !== null && String(path).endsWith(fsState.failChmodSuffix)) {
        throw new Error('simulated chmod failure');
      }
      actual.chmodSync(path, mode);
    },
    writeFileSync: (
      path: import('node:fs').PathOrFileDescriptor,
      data: string | NodeJS.ArrayBufferView,
      options?: import('node:fs').WriteFileOptions,
    ): void => {
      if (fsState.failWriteSuffix !== null && String(path).endsWith(fsState.failWriteSuffix)) {
        const err = new Error('simulated write failure') as NodeJS.ErrnoException;
        err.code = fsState.writeErrorCode;
        throw err;
      }
      actual.writeFileSync(path, data, options);
    },
  };
});

const MIB = 1024 * 1024;

interface Stack {
  home: string;
  paths: MurmurPaths;
  token: string;
  store: Store;
  broker: Broker;
}

const homes: string[] = [];
const stacks: Stack[] = [];
const sockets: Socket[] = [];
const clients: MurmurClient[] = [];

function freshHome(): string {
  const home = mkdtempSync(join(tmpdir(), 'murmur-sec-'));
  homes.push(home);
  return home;
}

async function buildStack(): Promise<Stack> {
  const home = freshHome();
  const paths = getPaths(home);
  ensureHome(paths);
  const store = Store.open(paths.dbPath);
  const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} });
  const broker = await Broker.start({ paths, store, core });
  const stack: Stack = { home, paths, token: readToken(paths), store, broker };
  stacks.push(stack);
  return stack;
}

function modeOf(path: string): number {
  return statSync(path).mode & 0o777;
}

interface Raw {
  socket: Socket;
  lines: string[];
  closed: Promise<void>;
  nextLine: () => Promise<string>;
}

function rawConnect(socketPath: string): Promise<Raw> {
  return new Promise((resolve, reject) => {
    const socket = createConnection(socketPath);
    sockets.push(socket);
    const reader = new LineReader();
    const lines: string[] = [];
    const waiters: Array<(line: string | null) => void> = [];
    let closedResolve: (() => void) | null = null;
    const closed = new Promise<void>((resolveClose) => {
      closedResolve = resolveClose;
    });
    socket.setEncoding('utf8');
    socket.on('data', (chunk: string) => {
      for (const line of reader.feed(chunk)) {
        lines.push(line);
        const waiter = waiters.shift();
        if (waiter !== undefined) {
          waiter(line);
        }
      }
    });
    socket.on('error', () => {
      // A reset while flushing a huge write is possible; close still fires.
    });
    socket.on('close', () => {
      for (const waiter of waiters.splice(0)) {
        waiter(null);
      }
      if (closedResolve !== null) {
        closedResolve();
      }
    });
    socket.once('connect', () => {
      resolve({
        socket,
        lines,
        closed,
        nextLine: () =>
          new Promise<string>((res, rej) => {
            waiters.push((line: string | null) => {
              if (line === null) {
                rej(new Error('closed before a line arrived'));
              } else {
                res(line);
              }
            });
          }),
      });
    });
    socket.once('error', reject);
  });
}

afterEach(async () => {
  fsState.failChmodSuffix = null;
  fsState.failWriteSuffix = null;
  fsState.writeErrorCode = 'EACCES';
  for (const client of clients.splice(0)) {
    client.close();
  }
  for (const socket of sockets.splice(0)) {
    socket.destroy();
  }
  for (const stack of stacks.splice(0)) {
    await stack.broker.stop();
    stack.store.close();
  }
  for (const home of homes.splice(0)) {
    rmSync(home, { recursive: true, force: true });
  }
});

describe('file permissions', () => {
  it('creates a 0700 home and a 0600 token that verifyPerms accepts', () => {
    const paths = getPaths(freshHome());
    ensureHome(paths);
    expect(modeOf(paths.home)).toBe(0o700);
    expect(modeOf(paths.tokenPath)).toBe(0o600);
    expect(readToken(paths)).toMatch(/^[0-9a-f]{64}$/);
    expect(() => verifyPerms(paths)).not.toThrow();
    // A second call keeps the existing token.
    const before = readFileSync(paths.tokenPath, 'utf8');
    ensureHome(paths);
    expect(readFileSync(paths.tokenPath, 'utf8')).toBe(before);
  });

  it('rejects a home directory whose mode was tampered with', () => {
    const paths = getPaths(freshHome());
    ensureHome(paths);
    chmodSync(paths.home, 0o755);
    let caught: unknown = null;
    try {
      verifyPerms(paths);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(MurmurError);
    expect((caught as MurmurError).code).toBe('internal');
    expect((caught as MurmurError).message).toBe('home directory ' + paths.home + ' has mode 755, expected 700');
    chmodSync(paths.home, 0o700);
    expect(() => verifyPerms(paths)).not.toThrow();
  });

  it('rejects a token file whose mode was tampered with', () => {
    const paths = getPaths(freshHome());
    ensureHome(paths);
    chmodSync(paths.tokenPath, 0o644);
    expect(() => verifyPerms(paths)).toThrow('token file ' + paths.tokenPath + ' has mode 644, expected 600');
    chmodSync(paths.tokenPath, 0o600);
    expect(() => verifyPerms(paths)).not.toThrow();
  });

  it('reports paths it cannot stat', () => {
    const paths = getPaths(join(freshHome(), 'missing'));
    let caught: unknown = null;
    try {
      verifyPerms(paths);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(MurmurError);
    expect((caught as MurmurError).code).toBe('internal');
    expect((caught as MurmurError).message).toContain('cannot stat murmur paths under ' + paths.home);
  });

  it('rejects a missing or empty token file', () => {
    const paths = getPaths(freshHome());
    expect(() => readToken(paths)).toThrow(MurmurError);
    try {
      readToken(paths);
    } catch (err) {
      expect((err as MurmurError).code).toBe('bad_token');
      expect((err as MurmurError).message).toContain('cannot read token file');
    }
    ensureHome(paths);
    writeFileSync(paths.tokenPath, '  \n', 'utf8');
    try {
      readToken(paths);
      throw new Error('expected readToken to throw');
    } catch (err) {
      expect((err as MurmurError).code).toBe('bad_token');
      expect((err as MurmurError).message).toBe('token file ' + paths.tokenPath + ' is empty');
    }
    // Surrounding whitespace is trimmed from a real token.
    writeFileSync(paths.tokenPath, '  abc123  \n', 'utf8');
    expect(readToken(paths)).toBe('abc123');
  });

  it('surfaces a failing chmod on the home directory', () => {
    const paths = getPaths(freshHome());
    fsState.failChmodSuffix = basename(paths.home);
    expect(() => ensureHome(paths)).toThrow('murmur: cannot set mode 0700 on home directory ' + paths.home);
  });

  it('surfaces a failing token creation', () => {
    const paths = getPaths(freshHome());
    fsState.failWriteSuffix = 'token';
    fsState.writeErrorCode = 'EACCES';
    expect(() => ensureHome(paths)).toThrow('murmur: cannot create token file ' + paths.tokenPath);
    expect(existsSync(paths.tokenPath)).toBe(false);
  });

  it('tolerates losing the token creation race', () => {
    const paths = getPaths(freshHome());
    fsState.failWriteSuffix = 'token';
    fsState.writeErrorCode = 'EEXIST';
    expect(() => ensureHome(paths)).not.toThrow();
    expect(modeOf(paths.home)).toBe(0o700);
  });

  it('surfaces a failing chmod on the token file', () => {
    const paths = getPaths(freshHome());
    fsState.failChmodSuffix = 'token';
    expect(() => ensureHome(paths)).toThrow('murmur: cannot set mode 0600 on token file ' + paths.tokenPath);
  });
});

describe('broker socket security', () => {
  it('sets the socket file to 0600 on start and keeps home and token modes', async () => {
    const stack = await buildStack();
    expect(existsSync(stack.paths.socketPath)).toBe(true);
    expect(modeOf(stack.paths.socketPath)).toBe(0o600);
    expect(modeOf(stack.paths.tokenPath)).toBe(0o600);
    expect(modeOf(stack.paths.home)).toBe(0o700);
    expect(() => verifyPerms(stack.paths)).not.toThrow();
  });

  it('rejects a wrong token and accepts the right one', async () => {
    const stack = await buildStack();
    await expect(
      MurmurClient.connect({ socketPath: stack.paths.socketPath, token: stack.token + 'x' }),
    ).rejects.toMatchObject({ code: 'bad_token' });
    await expect(MurmurClient.connect({ socketPath: stack.paths.socketPath, token: '' })).rejects.toMatchObject({
      code: 'bad_token',
    });
    const ok = await MurmurClient.connect({ socketPath: stack.paths.socketPath, token: stack.token });
    clients.push(ok);
    expect(ok.closed).toBe(false);
  });

  it('rejects auth when the token file turned empty or vanished', async () => {
    const stack = await buildStack();
    writeFileSync(stack.paths.tokenPath, '', 'utf8');
    const empty = await rawConnect(stack.paths.socketPath);
    empty.socket.write(encodeLine({ v: 1, id: 1, type: 'auth', token: stack.token }));
    const emptyReply = JSON.parse(await empty.nextLine()) as { ok: boolean; error: { code: string; message: string } };
    expect(emptyReply.ok).toBe(false);
    expect(emptyReply.error.code).toBe('bad_token');
    expect(emptyReply.error.message).toContain('is empty');
    await empty.closed;
    unlinkSync(stack.paths.tokenPath);
    const missing = await rawConnect(stack.paths.socketPath);
    missing.socket.write(encodeLine({ v: 1, id: 2, type: 'auth', token: stack.token }));
    const missingReply = JSON.parse(await missing.nextLine()) as { ok: boolean; error: { code: string; message: string } };
    expect(missingReply.ok).toBe(false);
    expect(missingReply.error.code).toBe('bad_token');
    expect(missingReply.error.message).toContain('cannot read token file');
    await missing.closed;
  });

  it('fails to start when the socket mode cannot be set, and closes the listener', async () => {
    const home = freshHome();
    const paths = getPaths(home);
    ensureHome(paths);
    const store = Store.open(paths.dbPath);
    const core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} });
    fsState.failChmodSuffix = 'bus.sock';
    await expect(Broker.start({ paths, store, core })).rejects.toThrow(
      'murmur: cannot set mode 0600 on socket ' + paths.socketPath,
    );
    fsState.failChmodSuffix = null;
    // The listener was closed, so a fresh broker can bind the same path.
    const broker = await Broker.start({ paths, store, core });
    stacks.push({ home, paths, token: readToken(paths), store, broker });
    expect(modeOf(paths.socketPath)).toBe(0o600);
  });

  it('closes a connection that completes a line over 1 MiB', async () => {
    const stack = await buildStack();
    const raw = await rawConnect(stack.paths.socketPath);
    // Exactly 1 MiB of pending bytes is still allowed; the newline that
    // completes a longer line trips the guard.
    raw.socket.write(Buffer.alloc(MIB, 0x78));
    raw.socket.write('x\n');
    await raw.closed;
    expect(raw.lines.length).toBeGreaterThanOrEqual(1);
    const reply = JSON.parse(raw.lines[0] ?? '{}') as { id: number; ok: boolean; error: { code: string; message: string } };
    expect(reply).toMatchObject({ id: 0, ok: false, error: { code: 'internal', message: 'line exceeds the 1 MiB limit' } });
    expect(existsSync(stack.paths.socketPath)).toBe(true);
  });

  it('closes a connection whose unfinished line passes 1 MiB before any newline', async () => {
    const stack = await buildStack();
    const raw = await rawConnect(stack.paths.socketPath);
    raw.socket.write(Buffer.alloc(MIB + 1, 0x78));
    await raw.closed;
    expect(raw.lines.length).toBeGreaterThanOrEqual(1);
    const reply = JSON.parse(raw.lines[0] ?? '{}') as { id: number; ok: boolean; error: { code: string; message: string } };
    expect(reply).toMatchObject({ id: 0, ok: false, error: { code: 'internal', message: 'line exceeds the 1 MiB limit' } });
  });

  it('applies the line guard after authentication too, and keeps the broker serving', async () => {
    const stack = await buildStack();
    const raw = await rawConnect(stack.paths.socketPath);
    raw.socket.write(encodeLine({ v: 1, id: 1, type: 'auth', token: stack.token }));
    const authReply = JSON.parse(await raw.nextLine()) as { ok: boolean };
    expect(authReply.ok).toBe(true);
    const oversized = JSON.stringify({ v: 1, id: 2, type: 'register', handle: 'x', clientKind: 'cli', display: 'y'.repeat(MIB) });
    raw.socket.write(oversized + '\n');
    await raw.closed;
    expect(raw.lines[1]).toBeDefined();
    expect(JSON.parse(raw.lines[1] ?? '{}')).toMatchObject({ ok: false, error: { code: 'internal' } });
    // Other connections are unaffected.
    const client = await MurmurClient.connect({ socketPath: stack.paths.socketPath, token: stack.token });
    clients.push(client);
    await expect(client.request<unknown>({ type: 'status' })).resolves.toBeDefined();
    // A line just under the limit is accepted and handled normally.
    const underLimit = JSON.stringify({
      v: 1,
      id: 3,
      type: 'register',
      handle: 'big-display',
      clientKind: 'cli',
      display: 'y'.repeat(MIB - 200),
    });
    expect(Buffer.byteLength(underLimit)).toBeLessThan(MIB);
    const raw2 = await rawConnect(stack.paths.socketPath);
    raw2.socket.write(encodeLine({ v: 1, id: 1, type: 'auth', token: stack.token }));
    await raw2.nextLine();
    raw2.socket.write(underLimit + '\n');
    const regReply = JSON.parse(await raw2.nextLine()) as { ok: boolean; id: number };
    expect(regReply.id).toBe(3);
    expect(regReply.ok).toBe(true);
  });
});

describe('home directory hygiene', () => {
  it('keeps the home private even when it already existed with loose modes', () => {
    const home = join(freshHome(), 'nested');
    mkdirSync(home, { recursive: true, mode: 0o755 });
    chmodSync(home, 0o755);
    const paths = getPaths(home);
    ensureHome(paths);
    expect(modeOf(home)).toBe(0o700);
    expect(modeOf(paths.tokenPath)).toBe(0o600);
  });
});
