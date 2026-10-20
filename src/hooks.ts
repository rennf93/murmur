import { connectBroker } from './client.js';
import { getPaths, readToken } from './config.js';
import { currentHost } from './proc.js';
import type { HostInfo } from './proc.js';
import type { FetchPushMessage, FetchPushResult, HookEvent, SessionMessage, SessionResult } from './protocol.js';

export interface HookOutcome {
  exitCode: number;
  stdout: string;
}

// The CLIs that drive murmur through command hooks. All four send the same
// stdin fields (session_id, transcript_path, cwd); they differ in event
// names (mapped by the installer onto murmur's four events) and in how a
// hook hands text back to the model.
export type HookClient = 'claude' | 'codex' | 'gemini' | 'zcode';
export const HOOK_CLIENTS: readonly HookClient[] = ['claude', 'codex', 'gemini', 'zcode'];

// Hard cap on the broker interaction. A hook runs inline inside a session
// and must never block it. MurmurClient.connect is already bounded (3s), but
// a broker that accepts and then stops replying could stall a request
// forever, so this watchdog destroys the socket, which rejects every
// pending request and lets the hook return.
const HOOK_DEADLINE_MS = 10000;

const FETCH_PUSH_LIMIT = 10;

interface HookInput {
  sessionId: string | null;
  transcriptPath: string | null;
  cwd: string | null;
}

// Entry point used by every client's hook config. It must never block or
// fail a session: any broker error, broker down, unreadable stdin, unknown
// host or unexpected failure resolves with exit 0 and empty stdout.
export async function runHook(
  event: HookEvent,
  stdinText: string,
  env: NodeJS.ProcessEnv,
  opts?: { client?: HookClient; host?: HostInfo | null },
): Promise<HookOutcome> {
  try {
    const client = opts?.client ?? 'claude';
    const host = opts?.host === undefined ? currentHost(undefined, false) : opts.host;
    if (host === null) {
      return { exitCode: 0, stdout: '' };
    }
    const input = parseInput(stdinText);
    const home = env.MURMUR_HOME;
    const paths = typeof home === 'string' ? getPaths(home) : getPaths();
    const token = readToken(paths);
    const conn = await connectBroker({ socketPath: paths.socketPath, token });
    const watchdog = setTimeout(() => {
      conn.close();
    }, HOOK_DEADLINE_MS);
    try {
      const { handle } = await conn.request<SessionResult>(sessionMessage(event, client, input, host, env));
      if (event === 'session-end') {
        return { exitCode: 0, stdout: '' };
      }
      const msg: Omit<FetchPushMessage, 'v' | 'id'> = { type: 'fetch-push', handle, limit: FETCH_PUSH_LIMIT };
      const result = await conn.request<FetchPushResult>(msg);
      const texts = result.texts ?? [];
      if (texts.length === 0) {
        return { exitCode: 0, stdout: '' };
      }
      const text = 'murmur: ' + String(texts.length) + ' message(s) for ' + handle + '\n\n' + texts.join('\n\n');
      return { exitCode: 0, stdout: JSON.stringify(outputFor(client, event, text)) };
    } finally {
      clearTimeout(watchdog);
      conn.close();
    }
  } catch {
    // Never fail or block a session because of murmur.
    return { exitCode: 0, stdout: '' };
  }
}

function sessionMessage(
  event: HookEvent,
  client: HookClient,
  input: HookInput,
  host: HostInfo,
  env: NodeJS.ProcessEnv,
): Omit<SessionMessage, 'v' | 'id'> {
  const msg: Omit<SessionMessage, 'v' | 'id'> = {
    type: 'session',
    event,
    client,
    cliSessionId: input.sessionId,
    host: { hostPid: host.hostPid, hostStart: host.hostStart },
  };
  // zcode hands hooks a synthetic transcript it deletes afterwards, so it
  // cannot serve as delivery proof.
  if (input.transcriptPath !== null && client !== 'zcode') {
    msg.transcriptPath = input.transcriptPath;
  }
  if (input.cwd !== null) {
    msg.cwd = input.cwd;
  }
  const explicit = env.MURMUR_HANDLE;
  if (typeof explicit === 'string' && explicit.length > 0) {
    msg.handle = explicit;
  }
  // A Claude session exposes its own messaging socket to the processes it
  // spawns; registering it lets the broker push straight into the session.
  const socketPath = env.CLAUDE_CODE_MESSAGING_SOCKET;
  const nativeToken = env.CLAUDE_CODE_MESSAGING_TOKEN;
  if (client === 'claude' && event === 'session-start' && nonEmpty(socketPath) && nonEmpty(nativeToken)) {
    msg.native = { socketPath, token: nativeToken };
  }
  return msg;
}

function nonEmpty(value: string | undefined): value is string {
  return typeof value === 'string' && value.length > 0;
}

// How each CLI takes text back from a hook. Start and prompt events add it
// to the model's context; stop asks the CLI to keep going with the text as
// the next instruction, so messages that arrived mid-turn are handled
// without waiting for the user.
function outputFor(client: HookClient, event: HookEvent, text: string): Record<string, unknown> {
  if (event === 'stop') {
    return { decision: client === 'gemini' ? 'deny' : 'block', reason: text };
  }
  let hookEventName = event === 'session-start' ? 'SessionStart' : 'UserPromptSubmit';
  if (client === 'gemini' && event === 'prompt') {
    hookEventName = 'BeforeAgent';
  }
  return { hookSpecificOutput: { hookEventName, additionalContext: text } };
}

// Reads the hook payload from stdin. A parse failure or missing field reads
// as null.
function parseInput(stdinText: string): HookInput {
  let parsed: unknown;
  try {
    parsed = JSON.parse(stdinText);
  } catch {
    parsed = null;
  }
  const rec = typeof parsed === 'object' && parsed !== null ? (parsed as Record<string, unknown>) : {};
  return {
    sessionId: stringField(rec, 'session_id'),
    transcriptPath: stringField(rec, 'transcript_path'),
    cwd: stringField(rec, 'cwd'),
  };
}

function stringField(rec: Record<string, unknown>, key: string): string | null {
  const value = rec[key];
  return typeof value === 'string' && value.length > 0 ? value : null;
}
