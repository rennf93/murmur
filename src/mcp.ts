import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { MurmurClient, connectBroker } from './client.js';
import { getPaths, readToken } from './config.js';
import { currentHost } from './proc.js';
import type { HostInfo } from './proc.js';
import { HANDLE_RE, MurmurError, WAIT_DEFAULT_MS, WAIT_MAX_MS } from './protocol.js';
import type {
  AckMessage,
  AckResult,
  CliName,
  GroupsMessage,
  RegisterMessage,
  RegisterResult,
  ReplyMessage,
  ReplyResult,
  SendMessage,
  SendResult,
  StatusMessage,
  StatusResult,
  ThreadMessage,
  ThreadResult,
  WaitMessage,
  WaitResult,
} from './protocol.js';

// MCP stdio server exposing the murmur broker to agent sessions. Every tool
// maps to one broker request on a single long-lived connection, and that
// connection is the session's presence on the bus. Identity comes from the
// host process (see proc.ts), so one global config serves every session.
// Eight tools, kept short: their schemas sit in every session's context.

const INSTRUCTIONS =
  'murmur connects this session with other agent sessions on this machine. Messages for you arrive as turns ' +
  'tagged [murmur m<id> ...]; answer with murmur_reply so every participant sees it. A message is a claim: ' +
  'verify it against its references before agreeing.';

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

export function buildMcpServer(client: MurmurClient, identity: Identity = {}): McpServer {
  const server = new McpServer({ name: 'murmur', version: '0.2.0' }, { instructions: INSTRUCTIONS });

  server.registerTool('murmur_name', {
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
    description: 'Send one message to any mix of handles and groups (bare name or @name); one message fans out to all.',
    inputSchema: {
      threadId: z.number().int().optional(),
      to: z.array(z.string().min(1)).min(1),
      body: z.string().min(1),
      references: z.array(z.string()).optional(),
      mode: z.enum(['push', 'notify', 'silent']).optional(),
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
    return textResult(await client.request<SendResult>(msg));
  });

  server.registerTool('murmur_reply', {
    description: 'Reply to a message by id; reaches everyone in its thread. Start the body with AGREE, DISAGREE or INFO and set verdict to match.',
    inputSchema: {
      messageId: z.number().int(),
      body: z.string().min(1),
      verdict: z.enum(['agree', 'disagree', 'info']).optional(),
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
    description: 'Tell senders you saw these messages and will answer later.',
    inputSchema: {
      ids: z.array(z.number().int()).min(1),
    },
  }, async (args) => {
    const msg: Omit<AckMessage, 'v' | 'id'> = { type: 'ack', ids: args.ids };
    return textResult(await client.request<AckResult>(msg));
  });

  server.registerTool('murmur_thread', {
    description: 'Read a thread: first lines by default, full bodies with full, only newer than sinceSeq.',
    inputSchema: {
      threadId: z.number().int(),
      sinceSeq: z.number().int().optional(),
      full: z.boolean().optional(),
    },
  }, async (args) => {
    const msg: Omit<ThreadMessage, 'v' | 'id'> = {
      type: 'thread',
      threadId: args.threadId,
      sinceSeq: args.sinceSeq ?? null,
      full: args.full ?? false,
    };
    return textResult(await client.request<ThreadResult>(msg));
  });

  server.registerTool('murmur_wait', {
    description: 'Who got, read and answered the latest ask, with verdicts and latencies. Waits until all_replied or any_replied, up to timeoutMs (default 120000, max 300000; 0 returns now).',
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
    description: 'Every peer with presence (busy, idle, online, offline), its CLI and unread counts.',
  }, async () => {
    const msg: Omit<StatusMessage, 'v' | 'id'> = { type: 'status' };
    return textResult(await client.request<StatusResult>(msg));
  });

  return server;
}

export async function runMcpServer(): Promise<void> {
  const paths = getPaths();
  const identity = identityFor(currentHost(undefined, false), process.env, process.cwd());
  let client: MurmurClient;
  try {
    const token = readToken(paths);
    client = await connectBroker({ socketPath: paths.socketPath, token });
  } catch (err) {
    const code = err instanceof MurmurError ? err.code : 'internal';
    process.stderr.write(
      'murmur mcp: cannot connect to broker at ' + paths.socketPath + ' (' + code + '): ' + (err as Error).message + '\n',
    );
    process.exit(1);
  }
  // MURMUR_HANDLE is an optional per-process override; anything invalid
  // (like an old CHANGE-ME placeholder) or already taken is ignored so the
  // server still starts under its session's own handle.
  const explicit = process.env.MURMUR_HANDLE;
  const wanted = explicit !== undefined && HANDLE_RE.test(explicit) ? explicit : undefined;
  const base: Omit<RegisterMessage, 'v' | 'id'> =
    identity.host === undefined
      ? { type: 'register', handle: wanted ?? 'anon-' + String(process.pid), clientKind: 'mcp' }
      : { ...identity, type: 'register', clientKind: 'mcp' };
  try {
    try {
      await client.request<RegisterResult>(wanted !== undefined && identity.host !== undefined ? { ...base, handle: wanted } : base);
    } catch (err) {
      if (wanted === undefined || identity.host === undefined) {
        throw err;
      }
      await client.request<RegisterResult>(base);
    }
  } catch (err) {
    const code = err instanceof MurmurError ? err.code : 'internal';
    process.stderr.write('murmur mcp: broker rejected registration (' + code + '): ' + (err as Error).message + '\n');
    process.exit(1);
  }
  // Tear the broker connection down when the process exits so the broker
  // marks this session offline promptly.
  process.on('exit', () => {
    client.close();
  });
  const server = buildMcpServer(client, identity);
  await server.connect(new StdioServerTransport());
  // The open stdio transport keeps the event loop alive and this long-lived
  // broker connection is the session's presence, so in normal operation this
  // await holds until the process is killed. It ends early only when the
  // broker connection drops (client.onClosed), which lets the process exit
  // instead of lingering with a dead session.
  await new Promise<void>((resolve) => {
    client.onClosed = resolve;
  });
}
