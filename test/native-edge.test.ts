import { describe, expect, it, vi } from 'vitest';

// nativeSend owns its socket, so node:net's createConnection is replaced with
// a scripted fake. This drives two event sequences a loopback server cannot
// be coerced into producing: a close-with-error that never delivered an
// error event, and a data chunk that carries no complete line. native.js is
// imported dynamically so it loads after the net mock is in place.
const fakeState = vi.hoisted(() => ({ sockets: [] as EventEmitter[] }));

vi.mock('node:net', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:net')>();
  // The factory runs before the module imports, so EventEmitter is pulled in
  // here rather than captured from the file scope.
  const { EventEmitter } = await import('node:events');
  class FakeSocket extends EventEmitter {
    writes: string[] = [];
    destroyed = false;
    write(chunk: string | Uint8Array): boolean {
      this.writes.push(String(chunk));
      return true;
    }
    destroy(): void {
      this.destroyed = true;
    }
  }
  return {
    ...actual,
    createConnection: () => {
      const socket = new FakeSocket();
      fakeState.sockets.push(socket);
      return socket as never;
    },
  };
});

function lastFake(): EventEmitter & { writes: string[]; destroy(): void } {
  const socket = fakeState.sockets[fakeState.sockets.length - 1];
  if (socket === undefined) {
    throw new Error('no fake socket was created');
  }
  return socket as EventEmitter & { writes: string[]; destroy(): void };
}

describe('nativeSend edge frames', () => {
  it('counts a chunk with no complete line as silence, then a clean close as accepted', async () => {
    const { nativeSend } = await import('../src/native.js');
    const pending = nativeSend('/tmp/murmur-native-partial.sock', 't', 'hello');
    const socket = lastFake();
    socket.emit('connect');
    expect(socket.writes).toHaveLength(2);
    // No newline anywhere: the reply loop never runs, the close decides.
    socket.emit('data', Buffer.from('a peer line that never terminates'));
    socket.emit('close', false);
    await expect(pending).resolves.toEqual({ ok: true, error: null });
    expect(socket.destroyed).toBe(true);
  });

  it('reports a generic failure for a close with an error flag but no error event', async () => {
    const { nativeSend } = await import('../src/native.js');
    const pending = nativeSend('/tmp/murmur-native-abort.sock', 't', 'hello');
    const socket = lastFake();
    socket.emit('connect');
    // close(hadError=true) with the error event never firing: the fallback
    // text is all the caller gets.
    socket.emit('close', true);
    await expect(pending).resolves.toEqual({
      ok: false,
      error: 'socket to /tmp/murmur-native-abort.sock closed with an error',
    });
  });
});
