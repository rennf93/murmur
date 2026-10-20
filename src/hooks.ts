import { MurmurClient } from './client.js';
import { getPaths, readToken } from './config.js';
import { truncateForDelivery } from './protocol.js';
import type {
  FetchPushMessage,
  FetchPushResult,
  HookEvent,
  HookMessage,
  MessageFull,
  NativeRegisterMessage,
} from './protocol.js';

export interface HookOutcome {
  exitCode: number;
  stdout: string;
}

// Hard cap on the broker interaction. A hook runs inline inside a Claude
// session and must never block it. MurmurClient.connect is already bounded
// (3s default), but a broker that accepts the connection and then stops
// replying could stall a request forever, so this watchdog destroys the
// socket, which rejects every pending request and lets the hook return.
const HOOK_DEADLINE_MS = 10000;

const FETCH_PUSH_LIMIT = 10;

// Entry point used by every client's hook config. It must never block or
// fail a session: any broker error, broker down, unreadable stdin, or
// unexpected failure resolves with exit 0 and empty stdout. On success it
// also resolves with exit 0; only the prompt event may carry stdout (the
// UserPromptSubmit additionalContext JSON line).
export async function runHook(event: HookEvent, stdinText: string, env: NodeJS.ProcessEnv): Promise<HookOutcome> {
  try {
    const sessionId = parseSessionId(stdinText);
    const handle = resolveHandle(env, sessionId);
    const home = env.MURMUR_HOME;
    const paths = typeof home === 'string' ? getPaths(home) : getPaths();
    const token = readToken(paths);
    const client = await MurmurClient.connect({ socketPath: paths.socketPath, token });
    const watchdog = setTimeout(() => {
      client.close();
    }, HOOK_DEADLINE_MS);
    try {
      await sendHookEvent(client, event, handle, sessionId);
      if (event === 'session-start') {
        await registerNative(client, env, handle);
        return { exitCode: 0, stdout: '' };
      }
      if (event === 'prompt') {
        return await fetchAndRender(client, handle);
      }
      // stop: hook event only.
      return { exitCode: 0, stdout: '' };
    } finally {
      clearTimeout(watchdog);
      client.close();
    }
  } catch {
    // Never fail or block a session because of murmur. This catches broker
    // down, broker closed, bad token, the watchdog firing, and anything
    // unexpected.
    return { exitCode: 0, stdout: '' };
  }
}

async function sendHookEvent(
  client: MurmurClient,
  event: HookEvent,
  handle: string,
  sessionId: string | null,
): Promise<void> {
  const msg: Omit<HookMessage, 'v' | 'id'> = { type: 'hook', event, handle };
  if (sessionId !== null) {
    msg.sessionHint = sessionId;
  }
  await client.request<void>(msg);
}

// session-start only: when this Claude process exposed its native messaging
// socket and token to us (both env vars present), tell the broker where to
// reach this session for native sends.
async function registerNative(client: MurmurClient, env: NodeJS.ProcessEnv, handle: string): Promise<void> {
  const socketPath = env.CLAUDE_CODE_MESSAGING_SOCKET;
  const token = env.CLAUDE_CODE_MESSAGING_TOKEN;
  if (typeof socketPath !== 'string' || socketPath.length === 0) {
    return;
  }
  if (typeof token !== 'string' || token.length === 0) {
    return;
  }
  const msg: Omit<NativeRegisterMessage, 'v' | 'id'> = {
    type: 'native-register',
    handle,
    socketPath,
    token,
  };
  await client.request<void>(msg);
}

// prompt only: drain up to FETCH_PUSH_LIMIT pushed messages for the handle
// and render them as one UserPromptSubmit additionalContext JSON line. No
// messages means empty stdout.
async function fetchAndRender(client: MurmurClient, handle: string): Promise<HookOutcome> {
  const msg: Omit<FetchPushMessage, 'v' | 'id'> = { type: 'fetch-push', handle, limit: FETCH_PUSH_LIMIT };
  const result = await client.request<FetchPushResult>(msg);
  if (result.messages.length === 0) {
    return { exitCode: 0, stdout: '' };
  }
  const joined = result.messages.map(renderMessage).join('\n\n');
  return {
    exitCode: 0,
    stdout: JSON.stringify({
      hookSpecificOutput: {
        hookEventName: 'UserPromptSubmit',
        additionalContext: joined,
      },
    }),
  };
}

function renderMessage(m: MessageFull): string {
  const header =
    'murmur message ' +
    String(m.id) +
    ' in thread ' +
    String(m.threadId) +
    ' seq ' +
    String(m.seq) +
    ' from ' +
    m.sender +
    ' (reply with murmur_reply in_reply_to ' +
    String(m.id) +
    '):';
  return header + '\n\n' + truncateForDelivery(m.body).delivered;
}

// Reads the Claude hook payload from stdin. It carries session_id among
// other fields; a parse failure or a missing id is ignored and the caller
// falls back to the unknown handle.
function parseSessionId(stdinText: string): string | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(stdinText);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) {
    return null;
  }
  const sessionId = (parsed as Record<string, unknown>).session_id;
  if (typeof sessionId === 'string' && sessionId.length > 0) {
    return sessionId;
  }
  return null;
}

// MURMUR_HANDLE wins when set, else claude- plus the first 8 chars of the
// session id, else claude-unknown.
function resolveHandle(env: NodeJS.ProcessEnv, sessionId: string | null): string {
  const explicit = env.MURMUR_HANDLE;
  if (typeof explicit === 'string' && explicit.length > 0) {
    return explicit;
  }
  if (sessionId !== null) {
    return 'claude-' + sessionId.slice(0, 8);
  }
  return 'claude-unknown';
}
