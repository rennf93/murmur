import { Socket, createConnection } from 'node:net';
import { StringDecoder } from 'node:string_decoder';
import { LineReader, encodeLine } from './protocol.js';

export interface NativeSendResult {
  ok: boolean;
  error: string | null;
}

// Framing for the Claude native messaging socket. The two builders below
// are the ONLY framing code in this file: nativeSend writes exactly the
// lines they return and frames nothing itself, so a spike outcome can be
// applied by editing these functions alone. The shapes are a documented
// guess from the official docs (newline-delimited JSON). Spike outcome:
// the peer never emitted a single byte in any probe (no banner, no ack,
// no error, against a wrong token, the real token, newline-delimited,
// NUL-delimited, and delimiter-free writes), so the reply framing is
// unobservable and the evidence does NOT justify changing the schema.
// Documented default stands.
export function buildAuthLine(token: string): string {
  return encodeLine({ type: 'auth', token });
}

export function buildMessageLine(text: string): string {
  return encodeLine({ type: 'message', message: text });
}

const DEFAULT_TIMEOUT_MS = 5000;

// Sends one message to a live Claude session over its native messaging
// socket (the /tmp/cc-socks/<pid>.sock socket the session exposes).
// Connects, writes the auth line then the message line, then waits for a
// reply line, a close, or the timeout. Resolves ok true when the peer
// accepted the send: a reply line arrived that is not an error object, or
// the socket closed cleanly. Resolves ok false with the error text on
// connect failure, write failure, an error-object reply, or timeout.
// Never throws.
export async function nativeSend(
  socketPath: string,
  token: string,
  text: string,
  timeoutMs?: number,
): Promise<NativeSendResult> {
  const limit = timeoutMs ?? DEFAULT_TIMEOUT_MS;
  return new Promise<NativeSendResult>((resolve) => {
    const socket: Socket = createConnection(socketPath);
    const decoder = new StringDecoder('utf8');
    const reader = new LineReader();
    let settled = false;
    let lastSocketError: string | null = null;
    let timer: NodeJS.Timeout | null = null;

    const finish = (result: NativeSendResult): void => {
      if (settled) {
        return;
      }
      settled = true;
      if (timer !== null) {
        clearTimeout(timer);
      }
      socket.destroy();
      resolve(result);
    };

    timer = setTimeout(() => {
      finish({ ok: false, error: 'no reply or close from ' + socketPath + ' within ' + String(limit) + 'ms' });
    }, limit);

    socket.once('error', (err: Error) => {
      lastSocketError = err.message;
      finish({ ok: false, error: 'socket error on ' + socketPath + ': ' + err.message });
    });

    socket.once('connect', () => {
      try {
        socket.write(buildAuthLine(token));
        socket.write(buildMessageLine(text));
      } catch (err) {
        finish({ ok: false, error: 'write to ' + socketPath + ' failed: ' + (err as Error).message });
      }
    });

    socket.on('data', (chunk: Buffer) => {
      const lines = reader.feed(decoder.write(chunk));
      for (const line of lines) {
        let parsed: unknown;
        try {
          parsed = JSON.parse(line);
        } catch {
          // The peer spoke first with a line that is not JSON. It is still
          // a reply line, and not an error object, so count the send as
          // accepted.
          finish({ ok: true, error: null });
          return;
        }
        if (isErrorReply(parsed)) {
          finish({ ok: false, error: errorReplyText(parsed) });
        } else {
          finish({ ok: true, error: null });
        }
        return;
      }
    });

    socket.once('close', (hadError: boolean) => {
      if (hadError || lastSocketError !== null) {
        finish({
          ok: false,
          error: lastSocketError ?? 'socket to ' + socketPath + ' closed with an error',
        });
        return;
      }
      // Clean close with no reply line: the peer read the writes and hung
      // up without complaining, count that as accepted.
      finish({ ok: true, error: null });
    });
  });
}

// Recognizes the plausible error shapes for a reply line: the murmur
// ErrReply shape ({ ok: false, error: { code, message } }) and a bare
// { type: 'error' }. Anything else counts as acceptance.
function isErrorReply(parsed: unknown): boolean {
  if (typeof parsed !== 'object' || parsed === null) {
    return false;
  }
  const rec = parsed as Record<string, unknown>;
  if (rec.ok === false && typeof rec.error === 'object' && rec.error !== null) {
    return true;
  }
  return rec.type === 'error';
}

function errorReplyText(parsed: unknown): string {
  const rec = parsed as Record<string, unknown>;
  if (typeof rec.error === 'object' && rec.error !== null) {
    const message = (rec.error as Record<string, unknown>).message;
    if (typeof message === 'string') {
      return message;
    }
  }
  if (typeof rec.message === 'string') {
    return rec.message;
  }
  return JSON.stringify(parsed);
}
