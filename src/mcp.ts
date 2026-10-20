import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { MurmurClient } from './client.js';
import { getPaths, readToken } from './config.js';
import { MurmurError, WAIT_DEFAULT_MS, WAIT_MAX_MS } from './protocol.js';
import type {
  AckMessage,
  AckResult,
  CollectMessage,
  CollectResult,
  GroupsMessage,
  InboxMessage,
  InboxResult,
  ReadMessage,
  ReadResult,
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
// connection is the session's presence on the bus.

function textResult(value: unknown): CallToolResult {
  return { content: [{ type: 'text', text: JSON.stringify(value) }] };
}

export function buildMcpServer(client: MurmurClient): McpServer {
  const server = new McpServer({ name: 'murmur', version: '0.1.0' });

  server.registerTool('murmur_register', {
    description: 'Register this connection on the murmur broker under a handle, optionally setting a display name and role.',
    inputSchema: {
      handle: z.string().min(1),
      display: z.string().optional(),
      role: z.string().optional(),
    },
  }, async (args) => {
    const msg: Omit<RegisterMessage, 'v' | 'id'> = { type: 'register', handle: args.handle, clientKind: 'mcp' };
    if (args.display !== undefined) {
      msg.display = args.display;
    }
    if (args.role !== undefined) {
      msg.role = args.role;
    }
    return textResult(await client.request<RegisterResult>(msg));
  });

  server.registerTool('murmur_send', {
    description: 'Send one message to any mix of peer handles and @group names; one message id fans out to all recipients.',
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
    description: 'Reply to one message by id; follow the first-line verdict convention by starting the body with AGREE, DISAGREE, or INFO so peers can scan stances, and optionally record the machine-readable verdict (agree, disagree, or info).',
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
    description: 'Mark the given message ids as acknowledged so senders can see you have processed them.',
    inputSchema: {
      ids: z.array(z.number().int()).min(1),
    },
  }, async (args) => {
    const msg: Omit<AckMessage, 'v' | 'id'> = { type: 'ack', ids: args.ids };
    return textResult(await client.request<AckResult>(msg));
  });

  server.registerTool('murmur_read', {
    description: 'Fetch the full bodies of the given message ids and mark them as read.',
    inputSchema: {
      ids: z.array(z.number().int()).min(1),
    },
  }, async (args) => {
    const msg: Omit<ReadMessage, 'v' | 'id'> = { type: 'read', ids: args.ids };
    return textResult(await client.request<ReadResult>(msg));
  });

  server.registerTool('murmur_inbox', {
    description: 'List your inbox threads with latest sequence and unread counts, optionally restricted to threads that still have unread messages.',
    inputSchema: {
      unreadOnly: z.boolean(),
    },
  }, async (args) => {
    const msg: Omit<InboxMessage, 'v' | 'id'> = { type: 'inbox', unreadOnly: args.unreadOnly };
    return textResult(await client.request<InboxResult>(msg));
  });

  server.registerTool('murmur_thread', {
    description: 'Fetch one thread by id, optionally only messages newer than a given sequence number, with summaries or full bodies.',
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

  server.registerTool('murmur_collect', {
    description: 'Show a per-recipient delivery and verdict matrix for one thread, including reply latencies and the verdict tally.',
    inputSchema: {
      threadId: z.number().int(),
    },
  }, async (args) => {
    const msg: Omit<CollectMessage, 'v' | 'id'> = { type: 'collect', threadId: args.threadId };
    return textResult(await client.request<CollectResult>(msg));
  });

  server.registerTool('murmur_wait', {
    description: 'Block until every other recipient of a thread has replied (all_replied) or at least one has (any_replied), giving up after timeoutMs (default 120000, capped at 300000).',
    inputSchema: {
      threadId: z.number().int(),
      until: z.enum(['all_replied', 'any_replied']),
      timeoutMs: z.number().int().positive().optional(),
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
    description: 'Create, list, add members to, remove members from, or delete a named group that messages can address as @name.',
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
    description: 'Show every registered peer with presence, busy and idle ages, and unread counts.',
  }, async () => {
    const msg: Omit<StatusMessage, 'v' | 'id'> = { type: 'status' };
    return textResult(await client.request<StatusResult>(msg));
  });

  return server;
}

export async function runMcpServer(): Promise<void> {
  const paths = getPaths();
  const handle = process.env.MURMUR_HANDLE ?? ('anon-' + String(process.pid));
  let client: MurmurClient;
  try {
    const token = readToken(paths);
    client = await MurmurClient.connect({ socketPath: paths.socketPath, token });
  } catch (err) {
    const code = err instanceof MurmurError ? err.code : 'internal';
    process.stderr.write(
      'murmur mcp: cannot connect to broker at ' + paths.socketPath + ' (' + code + '): ' + (err as Error).message + '\n',
    );
    process.exit(1);
  }
  try {
    const reg: Omit<RegisterMessage, 'v' | 'id'> = { type: 'register', handle, clientKind: 'mcp' };
    await client.request<RegisterResult>(reg);
  } catch (err) {
    const code = err instanceof MurmurError ? err.code : 'internal';
    process.stderr.write(
      'murmur mcp: broker rejected registration of handle ' + handle + ' (' + code + '): ' + (err as Error).message + '\n',
    );
    process.exit(1);
  }
  // Tear the broker connection down when the process exits so the broker
  // marks this session offline promptly.
  process.on('exit', () => {
    client.close();
  });
  const server = buildMcpServer(client);
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
