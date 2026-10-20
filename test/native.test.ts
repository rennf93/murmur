import { mkdtempSync, rmSync } from 'node:fs';
import { createServer, Socket } from 'node:net';
import type { Server } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { buildAuthLine, buildMessageLine, nativeSend } from '../src/native.js';
import { LineReader } from '../src/protocol.js';

// nativeSend is exercised against a scripted Unix socket server that plays
// the Claude native messaging peer: it records what arrived and answers
// according to the script under test.

interface Scripted {
  socketPath: string;
  received: string[];
  connections: number;
  close: () => Promise<void>;
}

type Script = (ctx: { socket: Socket; lines: string[] }) => void;

const homes: string[] = [];
const servers: Server[] = [];
const peers: Socket[] = [];

// Starts a server that collects complete lines per connection and invokes
// the script once both the auth and the message line have arrived. A script
// of null keeps the server silent (the timeout scenario).
function scriptedServer(script: Script | null): Promise<Scripted> {
  const dir = mkdtempSync(join(tmpdir(), 'murmur-native-'));
  homes.push(dir);
  const socketPath = join(dir, 'peer.sock');
  const received: string[] = [];
  const state: Scripted = {
    socketPath,
    received,
    connections: 0,
    close: () => Promise.resolve(),
  };
  // A silent peer must keep its half open, or it would hang up on the client's end().
  const server = createServer({ allowHalfOpen: script === null }, (socket: Socket) => {
    state.connections += 1;
    peers.push(socket);
    const reader = new LineReader();
    const lines: string[] = [];
    let fired = false;
    socket.setEncoding('utf8');
    socket.on('error', () => {
      // The client destroys its end once settled; ignore the resulting error.
    });
    socket.on('data', (chunk: string) => {
      for (const line of reader.feed(chunk)) {
        lines.push(line);
        received.push(line);
      }
      if (!fired && lines.length >= 2 && script !== null) {
        fired = true;
        script({ socket, lines });
      }
    });
  });
  servers.push(server);
  state.close = () =>
    new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  return new Promise<Scripted>((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, () => resolve(state));
  });
}

afterEach(async () => {
  for (const peer of peers.splice(0)) {
    peer.destroy();
  }
  for (const server of servers.splice(0)) {
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  }
  for (const dir of homes.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

describe('framing builders', () => {
  it('buildAuthLine frames a newline-delimited auth object', () => {
    const line = buildAuthLine('secret-token');
    expect(line).toBe('{"type":"auth","token":"secret-token"}\n');
    expect(line.endsWith('\n')).toBe(true);
    expect(JSON.parse(line)).toEqual({ type: 'auth', token: 'secret-token' });
  });

  it('buildMessageLine frames a newline-delimited message object and escapes newlines', () => {
    const line = buildMessageLine('hello\nworld');
    expect(line).toBe('{"type":"user","message":{"role":"user","content":"hello\\nworld"}}\n');
    expect(line.split('\n')).toHaveLength(2);
    expect(JSON.parse(line)).toEqual({ type: 'user', message: { role: 'user', content: 'hello\nworld' } });
  });
});

describe('nativeSend against a scripted peer', () => {
  it('writes exactly the auth line then the message line and accepts a non-error reply', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('{"type":"ack","ok":true}\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok-1', 'body text\n\nfooter');
    expect(result).toEqual({ ok: true, error: null });
    expect(peer.received).toEqual([buildAuthLine('tok-1').trim(), buildMessageLine('body text\n\nfooter').trim()]);
    expect(peer.connections).toBe(1);
  });

  it('accepts a clean close with no reply line', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.end();
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: true, error: null });
  });

  it('accepts a first reply line that is not JSON', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('OK thanks\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: true, error: null });
  });

  it('accepts a JSON reply that is not an object', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('null\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: true, error: null });
  });

  it('accepts an ok:false reply whose error is null (not the error shape)', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('{"ok":false,"error":null}\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: true, error: null });
  });

  it('reports a murmur-shaped error reply with its message', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('{"ok":false,"error":{"code":"bad_token","message":"denied"}}\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: false, error: 'denied' });
  });

  it('reports a murmur-shaped error reply without a string message as its JSON', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('{"ok":false,"error":{"code":"weird"}}\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: false, error: '{"ok":false,"error":{"code":"weird"}}' });
  });

  it('reports a bare type:error reply using its top-level message', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('{"type":"error","message":"boom"}\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: false, error: 'boom' });
  });

  it('reports a bare type:error reply without a message as its JSON', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('{"type":"error"}\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: false, error: '{"type":"error"}' });
  });

  it('settles on the first reply line only', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('{"type":"ack"}\n{"type":"error","message":"late"}\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: true, error: null });
  });

  it('accepts an ok:false reply with no error object', async () => {
    const peer = await scriptedServer(({ socket }) => {
      socket.write('{"ok":false}\n');
    });
    const result = await nativeSend(peer.socketPath, 'tok', 'text');
    expect(result).toEqual({ ok: true, error: null });
  });

  it('reports a synchronous write failure after connect', async () => {
    const peer = await scriptedServer(null);
    const writeSpy = vi
      .spyOn(Socket.prototype, 'end')
      .mockImplementationOnce(() => {
        throw new Error('pipe broke');
      });
    try {
      const result = await nativeSend(peer.socketPath, 'tok', 'text', 2000);
      expect(result).toEqual({ ok: false, error: 'write to ' + peer.socketPath + ' failed: pipe broke' });
      expect(peer.received).toEqual([]);
    } finally {
      writeSpy.mockRestore();
    }
  });

  it('fails on connect when the socket path does not exist', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'murmur-native-'));
    homes.push(dir);
    const missing = join(dir, 'nobody.sock');
    const result = await nativeSend(missing, 'tok', 'text');
    expect(result.ok).toBe(false);
    expect(result.error).toContain('socket error on ' + missing);
  });

  it('times out when the peer stays silent and keeps the connection open', async () => {
    const peer = await scriptedServer(null);
    const started = Date.now();
    const result = await nativeSend(peer.socketPath, 'tok', 'text', 120);
    expect(Date.now() - started).toBeGreaterThanOrEqual(100);
    expect(result).toEqual({
      ok: false,
      error: 'no reply or close from ' + peer.socketPath + ' within 120ms',
    });
    expect(peer.received).toHaveLength(2);
  });
});
