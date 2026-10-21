import { spawn } from 'node:child_process';
import { Socket, createConnection } from 'node:net';
import { StringDecoder } from 'node:string_decoder';
import { LineReader, MurmurError, PROTOCOL_VERSION, encodeLine } from './protocol.js';
import type { ClientMessage, ServerEvent, ServerReply } from './protocol.js';

export interface MurmurClientOptions {
  socketPath: string;
  token: string;
  onEvent?: (ev: ServerEvent) => void;
  connectTimeoutMs?: number;
}

interface Pending {
  resolve: (reply: ServerReply) => void;
  reject: (err: Error) => void;
}

export class MurmurClient {
  private readonly socket: Socket;
  private readonly decoder: StringDecoder;
  private readonly reader: LineReader;
  private readonly pending: Map<number, Pending> = new Map();
  private readonly onEvent: ((ev: ServerEvent) => void) | undefined;
  // Set by callers that must react when the broker connection drops (the MCP
  // server ends its wait on it). Invoked once, after pending requests fail.
  onClosed: (() => void) | undefined;
  // Auth occupies id 1, so requests start at 2.
  private nextId = 2;
  private closedState = false;

  private constructor(socket: Socket, onEvent: ((ev: ServerEvent) => void) | undefined) {
    this.socket = socket;
    this.decoder = new StringDecoder('utf8');
    this.reader = new LineReader();
    this.onEvent = onEvent;
    socket.on('data', (chunk: Buffer) => {
      this.onData(this.decoder.write(chunk));
    });
    socket.on('close', () => {
      this.onClose();
    });
    socket.on('error', () => {
      // Socket errors surface through the connect rejection or the close
      // handler, which rejects all pending requests.
    });
  }

  static connect(opts: MurmurClientOptions): Promise<MurmurClient> {
    const timeoutMs = opts.connectTimeoutMs ?? 3000;
    const socket: Socket = createConnection(opts.socketPath);
    return new Promise<MurmurClient>((resolve, reject) => {
      let settled = false;
      const finish = (err: MurmurError | null, client: MurmurClient | null): void => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        if (err !== null) {
          socket.destroy();
          reject(err);
        } else {
          // Every caller passes exactly one of err and client.
          resolve(client as MurmurClient);
        }
      };
      const timer = setTimeout(() => {
        finish(new MurmurError('broker_down', 'connect to ' + opts.socketPath + ' timed out after ' + String(timeoutMs) + 'ms'), null);
      }, timeoutMs);
      socket.once('error', (err: NodeJS.ErrnoException) => {
        // A CLI sandbox (codex) denies Unix sockets while the broker may be
        // up: not broker_down, so autostart never starts a second broker.
        if (err.code === 'EPERM' || err.code === 'EACCES') {
          finish(new MurmurError('socket_denied', 'connect to ' + opts.socketPath + ' denied (' + err.code +
            '): a sandbox may block this process from the murmur socket; use the murmur tools (murmur_reply, murmur_send) instead'), null);
          return;
        }
        finish(new MurmurError('broker_down', 'connect to ' + opts.socketPath + ' failed: ' + err.message), null);
      });
      socket.once('connect', () => {
        const client = new MurmurClient(socket, opts.onEvent);
        client
          .send<void>({ type: 'auth', token: opts.token }, 1)
          .then(() => {
            finish(null, client);
          })
          .catch((err: Error) => {
            if (err instanceof MurmurError) {
              finish(err, null);
            } else {
              finish(new MurmurError('internal', err.message), null);
            }
          });
      });
    });
  }

  request<T>(msg: Omit<ClientMessage, 'v' | 'id'>): Promise<T> {
    if (this.closed) {
      return Promise.reject(new MurmurError('broker_closed', 'connection to the broker is closed'));
    }
    const id = this.nextId;
    this.nextId += 1;
    return this.send<T>(msg, id);
  }

  close(): void {
    this.socket.destroy();
  }

  get closed(): boolean {
    return this.closedState || this.socket.destroyed;
  }

  // Writes one request line with the given id and stores its resolver. The
  // ok reply resolves with result cast to T; an error reply rejects with a
  // MurmurError carrying the wire error code. The message body is typed as
  // object because Omit over the ClientMessage union keeps only the common
  // keys, which would reject concrete message literals here.
  private send<T>(msg: object, id: number): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, {
        resolve: (reply: ServerReply) => {
          if (reply.ok) {
            resolve(reply.result as T);
          } else {
            reject(new MurmurError(reply.error.code, reply.error.message));
          }
        },
        reject,
      });
      try {
        this.socket.write(encodeLine({ v: PROTOCOL_VERSION, id, ...msg }));
      } catch (err) {
        this.pending.delete(id);
        reject(new MurmurError('broker_closed', 'connection to the broker closed before write: ' + (err as Error).message));
      }
    });
  }

  private onData(chunk: string): void {
    for (const line of this.reader.feed(chunk)) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch (err) {
        this.failAll(new MurmurError('internal', 'broker sent a line that failed JSON parse: ' + (err as Error).message));
        this.socket.destroy();
        return;
      }
      if (this.isEvent(parsed)) {
        if (this.onEvent !== undefined) {
          this.onEvent(parsed);
        }
        continue;
      }
      if (!this.isReply(parsed)) {
        // Valid JSON but neither an event nor a reply: treat as a protocol
        // violation, same policy as a parse failure.
        this.failAll(new MurmurError('internal', 'broker sent a line that is neither a reply nor an event'));
        this.socket.destroy();
        return;
      }
      const waiting = this.pending.get(parsed.id);
      if (waiting !== undefined) {
        this.pending.delete(parsed.id);
        waiting.resolve(parsed);
      }
    }
  }

  private isEvent(msg: unknown): msg is ServerEvent {
    return typeof msg === 'object' && msg !== null && (msg as { type?: unknown }).type === 'event';
  }

  private isReply(msg: unknown): msg is ServerReply {
    if (typeof msg !== 'object' || msg === null) {
      return false;
    }
    const rec = msg as Record<string, unknown>;
    if (typeof rec.id !== 'number' || (rec.ok !== true && rec.ok !== false)) {
      return false;
    }
    if (rec.ok === false && (typeof rec.error !== 'object' || rec.error === null)) {
      return false;
    }
    return true;
  }

  private onClose(): void {
    this.closedState = true;
    this.decoder.end();
    this.failAll(new MurmurError('broker_closed', 'connection to the broker closed'));
    this.onClosed?.();
  }

  private failAll(err: MurmurError): void {
    const waiting = Array.from(this.pending.values());
    this.pending.clear();
    for (const p of waiting) {
      p.reject(err);
    }
  }
}

export function isMurmurError(e: unknown): e is MurmurError {
  return e instanceof MurmurError;
}

export function isBrokerDown(e: unknown): boolean {
  return e instanceof MurmurError && (e.code === 'broker_down' || e.code === 'broker_closed');
}

// Connects, and when the broker is down and autostart is on (the murmur bin
// sets MURMUR_AUTOSTART=1; tests and library callers never do), starts one
// detached broker and retries for up to three seconds.
export async function connectBroker(opts: MurmurClientOptions): Promise<MurmurClient> {
  try {
    return await MurmurClient.connect(opts);
  } catch (err) {
    if (process.env.MURMUR_AUTOSTART !== '1' || !(err instanceof MurmurError) || err.code !== 'broker_down') {
      throw err;
    }
    spawn(process.execPath, [process.argv[1] ?? '', 'serve'], { detached: true, stdio: 'ignore' }).unref();
    for (let attempt = 0; attempt < 30; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      try {
        return await MurmurClient.connect(opts);
      } catch {
        // Still starting.
      }
    }
    throw err;
  }
}
