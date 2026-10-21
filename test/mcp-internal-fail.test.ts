import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Broker } from '../src/broker.js';
import { MurmurClient } from '../src/client.js';
import { MurmurError } from '../src/protocol.js';
import type { StatusResult } from '../src/protocol.js';
import { ensureHome, getPaths, readToken } from '../src/config.js';
import type { MurmurPaths } from '../src/config.js';
import { MurmurCore } from '../src/core.js';
import type { CoreEvents } from '../src/core.js';
import { runMcpServer } from '../src/mcp.js';
import { Store } from '../src/store.js';

// Fault paths of runMcpServer that the conformance suite cannot reach: a
// token store that throws a non-error (the wire only ever produces
// MurmurError), a register request that fails the same way, and the end of
// the wait when the broker connection drops.

// StdioServerTransport must not bind the real process streams; the fake
// keeps the same start/send/close surface the SDK assigns.
const fakeStdio = vi.hoisted(() => ({
  constructed: 0,
  started: 0,
  transport: null as null | { onmessage: ((message: unknown) => void) | undefined },
  sent: [] as Array<{ id?: number; result?: { content?: Array<{ text: string }>; isError?: boolean } }>,
}));

vi.mock('@modelcontextprotocol/sdk/server/stdio.js', () => ({
  StdioServerTransport: class FakeStdioServerTransport {
    onclose: (() => void) | undefined;
    onerror: ((error: Error) => void) | undefined;
    onmessage: ((message: unknown) => void) | undefined;
    constructor() {
      fakeStdio.constructed += 1;
      fakeStdio.transport = this;
    }
    async start(): Promise<void> {
      fakeStdio.started += 1;
    }
    async send(message: (typeof fakeStdio.sent)[number]): Promise<void> {
      fakeStdio.sent.push(message);
    }
    async close(): Promise<void> {}
  },
}));

// runMcpServer's host identity comes from currentHost; each case sets it.
const hostState = vi.hoisted(() => ({ host: null as unknown }));

vi.mock('../src/proc.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/proc.js')>();
  return { ...actual, currentHost: () => hostState.host };
});

// readToken only ever throws MurmurError for real, so the non-murmur branch
// of the connect guard is reached through this pass-through fault switch.
const configState = vi.hoisted(() => ({ tokenFailure: null as unknown }));

vi.mock('../src/config.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/config.js')>();
  return {
    ...actual,
    readToken: (paths: Parameters<typeof actual.readToken>[0]) => {
      if (configState.tokenFailure !== null) {
        const failure = configState.tokenFailure;
        configState.tokenFailure = null;
        throw failure;
      }
      return actual.readToken(paths);
    },
  };
});

interface Stack {
  home: string;
  paths: MurmurPaths;
  store: Store;
  broker: Broker;
}

const stacks: Stack[] = [];

async function buildStack(): Promise<Stack> {
  const home = mkdtempSync(join(tmpdir(), 'murmur-mcp-fail-'));
  const paths = getPaths(home);
  ensureHome(paths);
  const store = Store.open(paths.dbPath);
  const events: CoreEvents = { onAppend: () => {}, onStateChange: () => {} };
  const core = new MurmurCore(store, events);
  const broker = await Broker.start({ paths, store, core });
  const stack: Stack = { home, paths, store, broker };
  stacks.push(stack);
  return stack;
}

function exitThrows(): { exits: number[]; stderr: string[] } {
  const exits: number[] = [];
  const stderr: string[] = [];
  vi.spyOn(process, 'exit').mockImplementation(((code?: number | string | null) => {
    exits.push(Number(code));
    throw new Error('process.exit(' + String(code) + ')');
  }) as (code?: number | string | null) => never);
  vi.spyOn(process.stderr, 'write').mockImplementation(((chunk: string | Uint8Array) => {
    stderr.push(String(chunk));
    return true;
  }) as typeof process.stderr.write);
  return { exits, stderr };
}

afterEach(async () => {
  hostState.host = null;
  configState.tokenFailure = null;
  for (const stack of stacks.splice(0)) {
    await stack.broker.stop();
    stack.store.close();
    rmSync(stack.home, { recursive: true, force: true });
  }
});

describe('runMcpServer fault tolerance', () => {
  it('exits 1 with an internal diagnostic when the token store throws a non-error', async () => {
    const home = mkdtempSync(join(tmpdir(), 'murmur-mcp-fail-env-'));
    try {
      process.env.MURMUR_HOME = home;
      process.env.MURMUR_HANDLE = 'mcp-token-fault';
      configState.tokenFailure = new Error('token vault sealed');
      const { exits, stderr } = exitThrows();
      await expect(runMcpServer()).rejects.toThrow('process.exit(1)');
      expect(exits).toEqual([1]);
      expect(stderr.join('')).toContain(
        'murmur mcp: cannot connect to broker at ' + getPaths(home).socketPath + ' (internal): token vault sealed',
      );
      expect(fakeStdio.constructed).toBe(0);
    } finally {
      delete process.env.MURMUR_HOME;
      delete process.env.MURMUR_HANDLE;
      rmSync(home, { recursive: true, force: true });
    }
  });

  // A register request that throws a non-MurmurError, under every identity
  // shape: no retry applies without a host or without a wanted handle, and
  // with both the retry without the handle fails the same way.
  it.each([
    ['no host, no handle', null, undefined],
    ['no host, wanted handle', null, 'mcp-register-fault'],
    ['host, no handle', { hostPid: 9001, hostStart: 'S1', ancestors: [9001] }, undefined],
    ['host, wanted handle (retry fails too)', { hostPid: 9001, hostStart: 'S1', ancestors: [9001] }, 'mcp-register-fault'],
  ])('exits 1 when the broker rejects the registration with a non-error: %s', async (_label, host, handle) => {
    const stack = await buildStack();
    process.env.MURMUR_HOME = stack.home;
    if (handle === undefined) {
      delete process.env.MURMUR_HANDLE;
    } else {
      process.env.MURMUR_HANDLE = handle;
    }
    hostState.host = host;
    const { exits, stderr } = exitThrows();
    const proto = MurmurClient.prototype as unknown as { request: unknown };
    const original = proto.request;
    let calls = 0;
    proto.request = function () {
      calls += 1;
      throw new Error('register exploded');
    };
    try {
      await expect(runMcpServer()).rejects.toThrow('process.exit(1)');
      expect(exits).toEqual([1]);
      expect(stderr.join('')).toContain('murmur mcp: broker rejected registration (internal): register exploded');
      expect(calls).toBe(host !== null && handle !== undefined ? 2 : 1);
      expect(fakeStdio.constructed).toBe(0);
    } finally {
      proto.request = original;
      delete process.env.MURMUR_HOME;
      delete process.env.MURMUR_HANDLE;
    }
  });

  it('reports the wire code when the broker rejects the registration with a MurmurError', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HOME = stack.home;
    delete process.env.MURMUR_HANDLE;
    const { exits, stderr } = exitThrows();
    const proto = MurmurClient.prototype as unknown as { request: unknown };
    const original = proto.request;
    proto.request = function () {
      throw new MurmurError('bad_request', 'refused by broker');
    };
    try {
      await expect(runMcpServer()).rejects.toThrow('process.exit(1)');
      expect(exits).toEqual([1]);
      expect(stderr.join('')).toContain('murmur mcp: broker rejected registration (bad_request): refused by broker');
    } finally {
      proto.request = original;
      delete process.env.MURMUR_HOME;
    }
  });

  it('ends its wait when the broker connection drops', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HOME = stack.home;
    process.env.MURMUR_HANDLE = 'mcp-drops';
    const exitSpy = vi.spyOn(process, 'exit');
    vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
    try {
      const up = runMcpServer(new PassThrough());
      // The server registered and parked on the broker connection.
      const deadline = Date.now() + 5000;
      while (fakeStdio.started === 0 && Date.now() < deadline) {
        await new Promise((resolve) => {
          setImmediate(resolve);
        });
      }
      expect(fakeStdio.started).toBe(1);
      let outcome: 'resolved' | 'pending' = 'pending';
      const raced = up.then(
        () => {
          outcome = 'resolved';
        },
        () => {
          outcome = 'resolved';
        },
      );
      await Promise.race([raced, new Promise((resolve) => {
        setTimeout(resolve, 300);
      })]);
      expect(outcome).toBe('pending');
      // With no broker to reconnect to (autostart is off here), dropping the
      // connection ends the wait after the reconnect attempts.
      await stack.broker.stop();
      await up;
      expect(exitSpy).not.toHaveBeenCalled();
      // Fire the exit teardown the way process exit would.
      process.emit('exit', 0);
    } finally {
      exitSpy.mockRestore();
      delete process.env.MURMUR_HOME;
      delete process.env.MURMUR_HANDLE;
    }
  });

  it('stops reconnecting as soon as its stdin closes', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HOME = stack.home;
    process.env.MURMUR_HANDLE = 'mcp-gone';
    const exitSpy = vi.spyOn(process, 'exit');
    const stdin = new PassThrough();
    const before = fakeStdio.started;
    try {
      const up = runMcpServer(stdin);
      await until(() => fakeStdio.started > before, 'server started');
      // No broker comes back, so the server is in its reconnect attempts.
      await stack.broker.stop();
      await new Promise((resolve) => {
        setTimeout(resolve, 600);
      });
      const closedAt = Date.now();
      stdin.destroy();
      await up;
      // All 20 attempts would take about 5 s; it ends at the next one instead.
      expect(Date.now() - closedAt).toBeLessThan(2000);
      expect(exitSpy).not.toHaveBeenCalled();
    } finally {
      exitSpy.mockRestore();
      delete process.env.MURMUR_HOME;
      delete process.env.MURMUR_HANDLE;
    }
  });

  it('reconnects and registers again when the broker restarts, and its tools use the new connection', async () => {
    const stack = await buildStack();
    process.env.MURMUR_HOME = stack.home;
    process.env.MURMUR_HANDLE = 'mcp-reconnects';
    const exitSpy = vi.spyOn(process, 'exit');
    const stdin = new PassThrough();
    const before = fakeStdio.started;
    try {
      const up = runMcpServer(stdin);
      await until(() => fakeStdio.started > before, 'server started');
      await stack.broker.stop();
      stack.broker = await Broker.start({
        paths: stack.paths,
        store: stack.store,
        core: new MurmurCore(stack.store, { onAppend: () => {}, onStateChange: () => {} }),
      });
      const viewer = await MurmurClient.connect({ socketPath: stack.paths.socketPath, token: readToken(stack.paths) });
      await viewer.request({ type: 'register', handle: 'viewer', clientKind: 'mcp' });
      await until(async () => {
        const status = await viewer.request<StatusResult>({ type: 'status' });
        return status.peers.find((p) => p.handle === 'mcp-reconnects')?.presence === 'online';
      }, 'mcp-reconnects online on the new broker');
      viewer.close();
      fakeStdio.transport?.onmessage?.({ jsonrpc: '2.0', id: 77, method: 'tools/call', params: { name: 'murmur_status', arguments: {} } });
      await until(() => fakeStdio.sent.some((m) => m.id === 77), 'murmur_status answered');
      const answer = fakeStdio.sent.find((m) => m.id === 77);
      expect(answer?.result?.isError).toBeUndefined();
      expect(answer?.result?.content?.[0]?.text).toContain('"handle":"mcp-reconnects"');
      // The CLI closing stdin ends the server.
      stdin.destroy();
      await up;
      expect(exitSpy).not.toHaveBeenCalled();
    } finally {
      exitSpy.mockRestore();
      delete process.env.MURMUR_HOME;
      delete process.env.MURMUR_HANDLE;
    }
  });
});

async function until(pred: () => boolean | Promise<boolean>, label: string): Promise<void> {
  const deadline = Date.now() + 10000;
  while (!(await pred())) {
    if (Date.now() > deadline) {
      throw new Error('timed out waiting for ' + label);
    }
    await new Promise((resolve) => {
      setTimeout(resolve, 20);
    });
  }
}
