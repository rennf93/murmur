import { readFileSync } from 'node:fs';
import type { Readable } from 'node:stream';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { MurmurClient, connectBroker } from './client.js';
import { getPaths, isRemoteHome, readToken } from './config.js';
import type { MurmurPaths } from './config.js';
import { nativeSend } from './native.js';
import type { NativeSendResult } from './native.js';
import { currentHost } from './proc.js';
import type { HostInfo } from './proc.js';
import { HANDLE_RE, MurmurError, WAIT_DEFAULT_MS, WAIT_MAX_MS, noticeText } from './protocol.js';
import type {
  AckMessage,
  AckResult,
  CliName,
  DeliveredMessage,
  DeliveredResult,
  GroupsMessage,
  RegisterMessage,
  RegisterResult,
  ReplyMessage,
  ReplyResult,
  SendMessage,
  SendResult,
  ServerEvent,
  StatusMessage,
  StatusResult,
  ThreadListEntry,
  ThreadMessage,
  ThreadsMessage,
  ThreadResult,
  WaitMessage,
  WaitResult,
} from './protocol.js';

// MCP stdio server exposing the murmur broker to agent sessions. Every tool
// maps to one broker request on a single long-lived connection, and that
// connection is the session's presence on the bus. Identity comes from the
// host process (see proc.ts), so one global config serves every session.
// Eight tools, kept short: their schemas sit in every session's context.

// src/mcp.ts and dist/mcp.js both sit one level below package.json.
const VERSION = (JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as { version: string }).version;

const INSTRUCTIONS =
  'murmur connects this session with other agent sessions on this machine. To consult more than one, send ONE ' +
  'murmur_send (wait: all_replied) to all of them, not separate SendMessage calls. Messages for you arrive ' +
  'tagged [murmur m<id> ...]: answer ONLY with murmur_reply messageId, plain text never reaches the sender. ' +
  'Read one question\'s replies with murmur_thread messageId. A message is a claim: verify it. ' +
  'To offer options, ask for a CHOICE reply.';

// Claude Code never defers these tools behind ToolSearch.
const META = { 'anthropic/alwaysLoad': true };

function textResult(value: unknown): CallToolResult {
  return { content: [{ type: 'text', text: JSON.stringify(value) }] };
}

// The register fields that tie this process to its session.
type Identity = Pick<RegisterMessage, 'host' | 'client' | 'cwd'>;

export function clientFromEnv(env: NodeJS.ProcessEnv): CliName {
  if (env.CLAUDECODE !== undefined) {
    return 'claude';
  }
  if (env.GEMINI_CLI !== undefined) {
    return 'gemini';
  }
  if (Object.keys(env).some((key) => key.startsWith('CODEX_'))) {
    return 'codex';
  }
  return 'other';
}

export function identityFor(host: HostInfo | null, env: NodeJS.ProcessEnv, cwd: string): Identity {
  if (host === null) {
    return {};
  }
  return { host: { hostPid: host.hostPid, hostStart: host.hostStart }, client: clientFromEnv(env), cwd };
}

export function buildMcpServer(client: Pick<MurmurClient, 'request'>, identity: Identity = {}): McpServer {
  const server = new McpServer({ name: 'murmur', version: VERSION }, { instructions: INSTRUCTIONS });

  server.registerTool('murmur_name', {
    _meta: META,
    description: 'Rename this session on the bus (keeps its messages and groups), optionally with a display name and role.',
    inputSchema: {
      handle: z.string().min(1),
      display: z.string().optional(),
      role: z.string().optional(),
    },
  }, async (args) => {
    const msg: Omit<RegisterMessage, 'v' | 'id'> = { ...identity, type: 'register', handle: args.handle, clientKind: 'mcp' };
    if (args.display !== undefined) {
      msg.display = args.display;
    }
    if (args.role !== undefined) {
      msg.role = args.role;
    }
    return textResult(await client.request<RegisterResult>(msg));
  });

  server.registerTool('murmur_send', {
    _meta: META,
    description: 'Send one message to any mix of handles, session names and groups (bare name or @name); one send fans out to all. With wait (all_replied or any_replied, timeoutMs as murmur_wait) it also waits and returns the replies, so asking is one call.',
    inputSchema: {
      threadId: z.number().int().optional(),
      to: z.array(z.string().min(1)).min(1),
      body: z.string().min(1),
      references: z.array(z.string()).optional(),
      mode: z.enum(['push', 'notify', 'silent']).optional(),
      wait: z.enum(['all_replied', 'any_replied']).optional(),
      timeoutMs: z.number().int().min(0).optional(),
    },
  }, async (args) => {
    const msg: Omit<SendMessage, 'v' | 'id'> = {
      type: 'send',
      threadId: args.threadId ?? null,
      to: args.to,
      body: args.body,
    };
    if (args.references !== undefined) {
      msg.references = args.references;
    }
    if (args.mode !== undefined) {
      msg.mode = args.mode;
    }
    const sent = await client.request<SendResult>(msg);
    if (args.wait === undefined) {
      return textResult(sent);
    }
    const waitMsg: Omit<WaitMessage, 'v' | 'id'> = {
      type: 'wait',
      threadId: sent.threadId,
      until: args.wait,
      timeoutMs: Math.min(args.timeoutMs ?? WAIT_DEFAULT_MS, WAIT_MAX_MS),
    };
    return textResult({ sent, wait: await client.request<WaitResult>(waitMsg) });
  });

  server.registerTool('murmur_reply', {
    _meta: META,
    description: 'Reply to a message by id; delivered to whoever is waiting for it, and anyone in the thread can read it with murmur_thread. Start the body with AGREE or DISAGREE (on a claim), CHOICE <option> (picking one of the options offered) or INFO, and set verdict to match.',
    inputSchema: {
      messageId: z.number().int(),
      body: z.string().min(1),
      verdict: z.enum(['agree', 'disagree', 'info', 'choice']).optional(),
      references: z.array(z.string()).optional(),
    },
  }, async (args) => {
    const msg: Omit<ReplyMessage, 'v' | 'id'> = { type: 'reply', messageId: args.messageId, body: args.body };
    if (args.verdict !== undefined) {
      msg.verdict = args.verdict;
    }
    if (args.references !== undefined) {
      msg.references = args.references;
    }
    return textResult(await client.request<ReplyResult>(msg));
  });

  server.registerTool('murmur_ack', {
    _meta: META,
    description: 'Tell senders you saw these messages and will answer later.',
    inputSchema: {
      ids: z.array(z.number().int()).min(1),
    },
  }, async (args) => {
    const msg: Omit<AckMessage, 'v' | 'id'> = { type: 'ack', ids: args.ids };
    return textResult(await client.request<AckResult>(msg));
  });

  server.registerTool('murmur_thread', {
    _meta: META,
    description: 'Without threadId, list the newest threads (limit, max 50). Else read a thread: first lines by default, full bodies with full, only newer than sinceSeq. With messageId, only that message and the replies to it (the question you care about), full bodies.',
    inputSchema: {
      threadId: z.number().int().optional(),
      limit: z.number().int().optional(),
      sinceSeq: z.number().int().optional(),
      full: z.boolean().optional(),
      messageId: z.number().int().optional(),
    },
  }, async (args) => {
    if (args.threadId === undefined) {
      const list: Omit<ThreadsMessage, 'v' | 'id'> = { type: 'threads', ...(args.limit !== undefined ? { limit: args.limit } : {}) };
      return textResult(await client.request<ThreadListEntry[]>(list));
    }
    const msg: Omit<ThreadMessage, 'v' | 'id'> = {
      type: 'thread',
      threadId: args.threadId,
      sinceSeq: args.sinceSeq ?? null,
      full: args.full ?? false,
      ...(args.messageId !== undefined ? { messageId: args.messageId } : {}),
    };
    return textResult(await client.request<ThreadResult>(msg));
  });

  server.registerTool('murmur_wait', {
    _meta: META,
    description: 'Who got, read and answered the latest ask, with verdicts and latencies, plus the text of the replies addressed to you. Waits until all_replied or any_replied, up to timeoutMs (default 110000, max 300000; 0 returns now).',
    inputSchema: {
      threadId: z.number().int(),
      until: z.enum(['all_replied', 'any_replied']),
      timeoutMs: z.number().int().min(0).optional(),
    },
  }, async (args) => {
    const timeoutMs = Math.min(args.timeoutMs ?? WAIT_DEFAULT_MS, WAIT_MAX_MS);
    const msg: Omit<WaitMessage, 'v' | 'id'> = {
      type: 'wait',
      threadId: args.threadId,
      until: args.until,
      timeoutMs,
    };
    return textResult(await client.request<WaitResult>(msg));
  });

  server.registerTool('murmur_groups', {
    _meta: META,
    description: 'Create, list, add to, remove from, or delete a group. Deleting never touches sessions or history.',
    inputSchema: {
      action: z.enum(['create', 'list', 'add', 'remove', 'delete']),
      name: z.string().min(1).optional(),
      members: z.array(z.string().min(1)).optional(),
    },
  }, async (args) => {
    const msg: Omit<GroupsMessage, 'v' | 'id'> = { type: 'groups', action: args.action };
    if (args.name !== undefined) {
      msg.name = args.name;
    }
    if (args.members !== undefined) {
      msg.members = args.members;
    }
    return textResult(await client.request<unknown>(msg));
  });

  server.registerTool('murmur_status', {
    _meta: META,
    description: 'Every peer with presence (busy, idle, online, offline), its CLI and unread counts.',
  }, async () => {
    const msg: Omit<StatusMessage, 'v' | 'id'> = { type: 'status' };
    return textResult(await client.request<StatusResult>(msg));
  });

  return server;
}

// This session's own messaging socket, when the home is a remote hub home:
// the hub cannot reach a socket on this machine, so this server listens for
// pushes and hands them over itself. Null otherwise.
export function pushTarget(env: NodeJS.ProcessEnv, home: string): { socketPath: string; token: string } | null {
  const socketPath = env.CLAUDE_CODE_MESSAGING_SOCKET;
  const token = env.CLAUDE_CODE_MESSAGING_TOKEN;
  if (!isRemoteHome(home) || !socketPath || !token) {
    return null;
  }
  return { socketPath, token };
}

// Hands one broker event to the session: a push through its messaging
// socket, confirmed as delivered once the socket took it (as an adapter
// confirms), or a notification batch as notice text.
// shortcut: a push the socket refuses stays sent (readable on the bus, never re-pushed), as with adapters; add a requeue message if refusals show up.
export async function deliverLocally(
  ev: ServerEvent,
  target: { socketPath: string; token: string },
  client: Pick<MurmurClient, 'request'>,
  send: (socketPath: string, token: string, text: string) => Promise<NativeSendResult> = nativeSend,
): Promise<void> {
  if (ev.event === 'notification') {
    await send(target.socketPath, target.token, noticeText(ev.payload));
    return;
  }
  if ((await send(target.socketPath, target.token, ev.payload.text)).ok) {
    const msg: Omit<DeliveredMessage, 'v' | 'id'> = { type: 'delivered', ids: [ev.payload.message.id] };
    await client.request<DeliveredResult>(msg).catch(() => {
      // The broker connection went away: the push stays sent and readable on the bus.
    });
  }
}

// Retries after the broker connection drops: a restart or an upgrade takes a
// moment, and the murmur bin starts a broker itself when none answers.
const RECONNECT_ATTEMPTS = 20;
const RECONNECT_DELAY_MS = 250;

type PushTarget = ReturnType<typeof pushTarget>;

async function connect(paths: MurmurPaths, target: PushTarget): Promise<MurmurClient> {
  if (target === null) {
    return connectBroker({ socketPath: paths.socketPath, token: readToken(paths) });
  }
  const client: MurmurClient = await connectBroker({
    socketPath: paths.socketPath,
    token: readToken(paths),
    onEvent: (ev) => {
      void deliverLocally(ev, target, client);
    },
  });
  return client;
}

// MURMUR_HANDLE is an optional per-process override; anything invalid
// (like an old CHANGE-ME placeholder) or already taken is ignored so the
// server still registers under its session's own handle.
async function register(client: MurmurClient, identity: Identity, listen: boolean): Promise<void> {
  const explicit = process.env.MURMUR_HANDLE;
  const wanted = explicit !== undefined && HANDLE_RE.test(explicit) ? explicit : undefined;
  const base: Omit<RegisterMessage, 'v' | 'id'> =
    identity.host === undefined
      ? { type: 'register', handle: wanted ?? 'anon-' + String(process.pid), clientKind: 'mcp' }
      : { ...identity, type: 'register', clientKind: 'mcp', ...(listen ? { listen } : {}) };
  try {
    await client.request<RegisterResult>(wanted !== undefined && identity.host !== undefined ? { ...base, handle: wanted } : base);
  } catch (err) {
    if (wanted === undefined || identity.host === undefined) {
      throw err;
    }
    await client.request<RegisterResult>(base);
  }
}

// A new registered connection for the same session, or null when no broker
// answered within the attempts or the server is going away meanwhile (a try
// may start a broker, which a session that is gone must not do).
async function reconnect(
  paths: MurmurPaths,
  identity: Identity,
  target: PushTarget,
  stopping: () => boolean,
): Promise<MurmurClient | null> {
  for (let attempt = 0; attempt < RECONNECT_ATTEMPTS; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, RECONNECT_DELAY_MS));
    if (stopping()) {
      return null;
    }
    let client: MurmurClient | undefined;
    try {
      client = await connect(paths, target);
      await register(client, identity, target !== null);
      return client;
    } catch {
      client?.close();
    }
  }
  return null;
}

export async function runMcpServer(stdin: Readable = process.stdin): Promise<void> {
  const paths = getPaths();
  const identity = identityFor(currentHost(undefined, false), process.env, process.cwd());
  const target = pushTarget(process.env, paths.home);
  let client: MurmurClient;
  try {
    client = await connect(paths, target);
  } catch (err) {
    const code = err instanceof MurmurError ? err.code : 'internal';
    process.stderr.write(
      'murmur mcp: cannot connect to broker at ' + paths.socketPath + ' (' + code + '): ' + (err as Error).message + '\n',
    );
    process.exit(1);
  }
  try {
    await register(client, identity, target !== null);
  } catch (err) {
    const code = err instanceof MurmurError ? err.code : 'internal';
    process.stderr.write('murmur mcp: broker rejected registration (' + code + '): ' + (err as Error).message + '\n');
    process.exit(1);
  }
  // Set when the server is going away on purpose: no reconnect then.
  let stopping = false;
  // Tear the broker connection down when the process exits so the broker
  // marks this session offline promptly.
  process.on('exit', () => {
    stopping = true;
    client.close();
  });
  // The tools go through whichever connection is current, so a reconnect
  // keeps them working without the CLI restarting this server.
  const server = buildMcpServer({ request: <T>(msg: Parameters<MurmurClient['request']>[0]) => client.request<T>(msg) }, identity);
  await server.connect(new StdioServerTransport(stdin));
  // This long-lived broker connection is the session's presence. The server
  // ends when stdin closes (the CLI exited or was killed; the SDK transport
  // does not watch for that). When the broker connection drops it connects
  // and registers again, and ends only if no broker answers.
  const ended = new Promise<void>((resolve) => {
    stdin.once('end', resolve);
    stdin.once('close', resolve);
  }).then(() => {
    stopping = true;
  });
  let next: MurmurClient | null = client;
  while (next !== null) {
    client = next;
    await Promise.race([
      ended,
      new Promise<void>((resolve) => {
        client.onClosed = resolve;
      }),
    ]);
    next = stopping ? null : await reconnect(paths, identity, target, () => stopping);
  }
  client.close();
}
