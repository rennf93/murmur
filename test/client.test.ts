import * as net from 'node:net';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { LineReader, MurmurError, encodeLine } from '../src/protocol.js';
import type { ServerEvent } from '../src/protocol.js';
import { MurmurClient, isBrokerDown, isMurmurError } from '../src/client.js';

type LineHandler = (conn: net.Socket, line: string) => void;

interface Broker {
  socketPath: string;
  connections: net.Socket[];
  setHandler(handler: LineHandler): void;
  sendTo(conn: net.Socket, msg: unknown): void;
  lastConnection(): net.Socket;
  destroyConnections(): void;
  close(): Promise<void>;
}

const brokers: Broker[] = [];

async function startBroker(): Promise<Broker> {
  const dir = mkdtempSync(join(tmpdir(), 'murmur-client-'));
  const socketPath = join(dir, 'bus.sock');
  const connections: net.Socket[] = [];
  let closed = false;
  let lineHandler: LineHandler = () => undefined;
  const server = net.createServer((conn) => {
    const reader = new LineReader();
    connections.push(conn);
    conn.on('error', () => undefined);
    conn.on('data', (chunk: Buffer) => {
      for (const line of reader.feed(chunk.toString('utf8'))) {
        lineHandler(conn, line);
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, () => resolve());
  });
  const broker: Broker = {
    socketPath,
    connections,
    setHandler(handler: LineHandler) {
      lineHandler = handler;
    },
    sendTo(conn: net.Socket, msg: unknown) {
      conn.write(encodeLine(msg));
    },
    lastConnection() {
      return connections[connections.length - 1] as net.Socket;
    },
    destroyConnections() {
      for (const conn of connections) {
        conn.destroy();
      }
    },
    close() {
      return new Promise<void>((resolve) => {
        if (closed) {
          resolve();
          return;
        }
        closed = true;
        for (const conn of connections.splice(0)) {
          conn.destroy();
        }
        server.close(() => {
          rmSync(dir, { recursive: true, force: true });
          resolve();
        });
      });
    },
  };
  brokers.push(broker);
  return broker;
}

afterEach(async () => {
  while (brokers.length > 0) {
    await brokers.pop()?.close();
  }
});

// Auth on every connection, then the given handler for everything else.
function authThen(handler: (conn: net.Socket, msg: { id: number; type: string }) => void): LineHandler {
  return (conn, line) => {
    const msg = JSON.parse(line) as { id: number; type: string };
    if (msg.type === 'auth') {
      conn.write(encodeLine({ v: 1, id: msg.id, ok: true, result: null }));
      return;
    }
    handler(conn, msg);
  };
}

function echoBroker(): LineHandler {
  return authThen((conn, msg) => {
    conn.write(encodeLine({ v: 1, id: msg.id, ok: true, result: { echoed: msg.type } }));
  });
}

interface ConnectOpts {
  token?: string;
  onEvent?: (ev: ServerEvent) => void;
  connectTimeoutMs?: number;
}

async function connect(broker: Broker, opts: ConnectOpts = {}): Promise<MurmurClient> {
  const options: {
    socketPath: string;
    token: string;
    onEvent?: (ev: ServerEvent) => void;
    connectTimeoutMs?: number;
  } = {
    socketPath: broker.socketPath,
    token: opts.token ?? 'tok',
  };
  if (opts.onEvent !== undefined) {
    options.onEvent = opts.onEvent;
  }
  if (opts.connectTimeoutMs !== undefined) {
    options.connectTimeoutMs = opts.connectTimeoutMs;
  }
  return MurmurClient.connect(options);
}

describe('MurmurClient', () => {
  it('authenticates and completes request round trips', async () => {
    const broker = await startBroker();
    broker.setHandler(echoBroker());
    const client = await connect(broker);
    expect(client.closed).toBe(false);
    await expect(client.request({ type: 'status' })).resolves.toEqual({ echoed: 'status' });
    await expect(client.request({ type: 'inbox', unreadOnly: true })).resolves.toEqual({ echoed: 'inbox' });
    client.close();
  });

  it('rejects with a MurmurError carrying the wire error code', async () => {
    const broker = await startBroker();
    broker.setHandler(
      authThen((conn, msg) => {
        conn.write(encodeLine({ v: 1, id: msg.id, ok: false, error: { code: 'unknown_handle', message: 'no such handle' } }));
      }),
    );
    const client = await connect(broker);
    let caught: unknown;
    try {
      await client.request({ type: 'send', threadId: null, to: ['nobody'], body: 'x' });
    } catch (err) {
      caught = err;
    }
    expect(isMurmurError(caught)).toBe(true);
    expect(caught).toBeInstanceOf(MurmurError);
    expect((caught as MurmurError).code).toBe('unknown_handle');
    expect((caught as MurmurError).message).toBe('no such handle');
    client.close();
  });

  it('rejects connect when auth fails', async () => {
    const broker = await startBroker();
    broker.setHandler((conn, line) => {
      const msg = JSON.parse(line) as { id: number };
      conn.write(encodeLine({ v: 1, id: msg.id, ok: false, error: { code: 'bad_token', message: 'token mismatch' } }));
    });
    let caught: unknown;
    try {
      await connect(broker, { token: 'wrong' });
    } catch (err) {
      caught = err;
    }
    expect(isMurmurError(caught)).toBe(true);
    expect((caught as MurmurError).code).toBe('bad_token');
  });

  it('reports broker_down when the socket does not exist', async () => {
    const broker = await startBroker();
    const missing = join(broker.socketPath, '..', 'absent.sock');
    let caught: unknown;
    try {
      await MurmurClient.connect({ socketPath: missing, token: 't', connectTimeoutMs: 2000 });
    } catch (err) {
      caught = err;
    }
    expect(isMurmurError(caught)).toBe(true);
    expect((caught as MurmurError).code).toBe('broker_down');
    expect((caught as MurmurError).message).toContain('failed');
    expect(isBrokerDown(caught)).toBe(true);
    expect(isBrokerDown(new Error('plain'))).toBe(false);
    expect(isBrokerDown(new MurmurError('broker_closed', 'x'))).toBe(true);
    expect(isBrokerDown(new MurmurError('bad_token', 'x'))).toBe(false);
  });

  it('times out when the broker never answers auth', async () => {
    const broker = await startBroker();
    broker.setHandler(() => undefined);
    let caught: unknown;
    try {
      await connect(broker, { connectTimeoutMs: 100 });
    } catch (err) {
      caught = err;
    }
    expect(isMurmurError(caught)).toBe(true);
    expect((caught as MurmurError).code).toBe('broker_down');
    expect((caught as MurmurError).message).toContain('timed out');
  });

  it('rejects pending requests with broker_closed when the server closes', async () => {
    const broker = await startBroker();
    broker.setHandler(authThen(() => undefined));
    const client = await connect(broker);
    const first = client.request({ type: 'status' });
    const second = client.request({ type: 'thread', threadId: 1, sinceSeq: null, full: false });
    broker.destroyConnections();
    // Issued in the same tick, this write reaches the dying socket and
    // exercises the client side socket error handler before the close lands.
    const third = client.request({ type: 'status' });
    await expect(first).rejects.toMatchObject({ code: 'broker_closed' });
    await expect(second).rejects.toMatchObject({ code: 'broker_closed' });
    await expect(third).rejects.toMatchObject({ code: 'broker_closed' });
    expect(client.closed).toBe(true);
  });

  it('delivers server events to onEvent', async () => {
    const broker = await startBroker();
    const events: ServerEvent[] = [];
    broker.setHandler((conn, line) => {
      const msg = JSON.parse(line) as { id: number; type: string };
      if (msg.type === 'auth') {
        conn.write(encodeLine({ v: 1, id: msg.id, ok: true, result: null }));
        conn.write(
          encodeLine({
            v: 1,
            type: 'event',
            event: 'notification',
            payload: [{ kind: 'peer_offline', threadId: null, fromHandle: null, detail: 'gone' }],
          }),
        );
        return;
      }
      conn.write(encodeLine({ v: 1, id: msg.id, ok: true, result: null }));
    });
    const client = await connect(broker, { onEvent: (ev) => events.push(ev) });
    await client.request({ type: 'status' });
    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toMatchObject({ type: 'event', event: 'notification' });
    client.close();
  });

  it('ignores events when no listener is installed', async () => {
    const broker = await startBroker();
    broker.setHandler(
      authThen((conn, msg) => {
        conn.write(encodeLine({ v: 1, id: msg.id, ok: true, result: 'ok' }));
      }),
    );
    const client = await connect(broker);
    broker.sendTo(broker.lastConnection(), {
      v: 1,
      type: 'event',
      event: 'push',
      payload: { message: {}, mode: 'push', footer: 'f' },
    });
    await expect(client.request({ type: 'status' })).resolves.toBe('ok');
    expect(client.closed).toBe(false);
    client.close();
  });

  it('closes with internal when the broker sends a malformed line', async () => {
    const broker = await startBroker();
    broker.setHandler(authThen(() => undefined));
    const client = await connect(broker);
    const pending = client.request({ type: 'status' });
    broker.lastConnection().write('this is not json\n');
    await expect(pending).rejects.toMatchObject({ code: 'internal' });
    const err = (await pending.catch((e: MurmurError) => e)) as MurmurError;
    expect(err.message).toContain('failed JSON parse');
    expect(client.closed).toBe(true);
  });

  it.each([
    ['a bare JSON number', '42'],
    ['an ok:false reply without an error object', JSON.stringify({ v: 1, id: 7, ok: false })],
    ['a JSON object without an id', JSON.stringify({ hello: 'world' })],
  ])('treats %s as a protocol violation', async (_label, line) => {
    const broker = await startBroker();
    broker.setHandler(authThen(() => undefined));
    const client = await connect(broker);
    const pending = client.request({ type: 'status' });
    broker.lastConnection().write(line + '\n');
    await expect(pending).rejects.toMatchObject({ code: 'internal' });
    expect(client.closed).toBe(true);
  });

  it('ignores replies for unknown request ids', async () => {
    const broker = await startBroker();
    broker.setHandler(
      authThen((conn, msg) => {
        conn.write(encodeLine({ v: 1, id: 4242, ok: true, result: 'stale' }));
        conn.write(encodeLine({ v: 1, id: msg.id, ok: true, result: 'fresh' }));
      }),
    );
    const client = await connect(broker);
    await expect(client.request({ type: 'status' })).resolves.toBe('fresh');
    client.close();
  });

  it('wraps an unexpected auth handshake failure as an internal error', async () => {
    const broker = await startBroker();
    broker.setHandler(echoBroker());
    const proto = MurmurClient.prototype as unknown as {
      send: (msg: object, id: number) => Promise<unknown>;
    };
    const realSend = proto.send;
    proto.send = function (msg: object, id: number) {
      if ((msg as { type?: string }).type === 'auth') {
        return Promise.reject(new Error('auth transport exploded'));
      }
      return realSend.call(this, msg, id);
    };
    try {
      let caught: unknown;
      try {
        await connect(broker);
      } catch (err) {
        caught = err;
      }
      expect(isMurmurError(caught)).toBe(true);
      expect((caught as MurmurError).code).toBe('internal');
      expect((caught as MurmurError).message).toBe('auth transport exploded');
    } finally {
      proto.send = realSend;
    }
  });

  it('rejects requests issued after the client is closed', async () => {
    const broker = await startBroker();
    broker.setHandler(echoBroker());
    const client = await connect(broker);
    client.close();
    expect(client.closed).toBe(true);
    await expect(client.request({ type: 'status' })).rejects.toMatchObject({ code: 'broker_closed' });
  });

  it('rejects with broker_closed when the socket write throws synchronously', async () => {
    const broker = await startBroker();
    broker.setHandler(echoBroker());
    const client = await connect(broker);
    const internals = client as unknown as { socket: net.Socket; pending: Map<number, unknown> };
    const realWrite = internals.socket.write.bind(internals.socket);
    internals.socket.write = (() => {
      throw new Error('pipe blew up');
    }) as typeof internals.socket.write;
    await expect(client.request({ type: 'status' })).rejects.toMatchObject({
      code: 'broker_closed',
      message: expect.stringContaining('closed before write'),
    });
    expect(internals.pending.size).toBe(0);
    internals.socket.write = realWrite as typeof internals.socket.write;
    await expect(client.request({ type: 'status' })).resolves.toEqual({ echoed: 'status' });
    client.close();
  });
});
