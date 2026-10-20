// murmur adapter for opencode: joins each opencode session to the murmur bus.
// One long-lived socket per session, push events become promptAsync calls,
// and the murmur_* tools talk over the calling session's connection.
import { tool, type Plugin } from '@opencode-ai/plugin';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { connect, type Socket } from 'node:net';
import { homedir } from 'node:os';
import { join } from 'node:path';

const HOME = process.env.MURMUR_HOME || join(homedir(), '.murmur');
const RETRY_MS = 5000;

let hostStart = '';
try {
  hostStart = execFileSync('ps', ['-o', 'lstart=', '-p', String(process.pid)], {
    env: { ...process.env, LC_ALL: 'C' },
    encoding: 'utf8',
  }).trim();
} catch {
  // leave empty; the broker will refuse the announce and we retry quietly
}
const host = { hostPid: process.pid, hostStart };

type Pending = { resolve: (v: any) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> };

interface Conn {
  sid: string;
  cwd: string;
  alive: boolean;
  busy: boolean;
  handle: string | null;
  sock: Socket | null;
  buf: string;
  nextId: number;
  pending: Map<number, Pending>;
  retry: ReturnType<typeof setTimeout> | null;
  ready: Promise<void>;
}

const plugin: Plugin = async ({ client, directory }) => {
  const conns = new Map<string, Conn>();

  const fail = (p: Pending, msg: string) => {
    clearTimeout(p.timer);
    p.reject(new Error(msg));
  };

  const drop = (c: Conn) => {
    for (const p of c.pending.values()) fail(p, 'murmur connection closed');
    c.pending.clear();
    c.sock?.destroy();
    c.sock = null;
    if (c.alive && !c.retry) {
      c.retry = setTimeout(() => {
        c.retry = null;
        if (c.alive) c.ready = open(c);
      }, RETRY_MS);
    }
  };

  const rpc = (c: Conn, msg: Record<string, unknown>, timeoutMs = 15000): Promise<any> =>
    new Promise((resolve, reject) => {
      if (!c.sock) return reject(new Error('murmur broker not connected'));
      const id = c.nextId++;
      const timer = setTimeout(() => {
        c.pending.delete(id);
        reject(new Error('murmur request timed out'));
      }, timeoutMs);
      c.pending.set(id, { resolve, reject, timer });
      c.sock.write(JSON.stringify({ v: 2, id, ...msg }) + '\n');
    });

  const lifecycle = (c: Conn, event: string) =>
    rpc(c, {
      type: 'session',
      event,
      client: 'opencode',
      cliSessionId: c.sid,
      host,
      cwd: c.cwd,
      listen: true,
    });

  const post = async (c: Conn, text: string) => {
    const r: any = await client.session.promptAsync({
      path: { id: c.sid },
      body: { parts: [{ type: 'text', text }] },
    });
    if (r && r.error) throw new Error('promptAsync failed');
  };

  const onEvent = async (c: Conn, m: any) => {
    if (m.event === 'push') {
      const text = m.payload?.text;
      const mid = m.payload?.message?.id;
      if (typeof text !== 'string') return;
      await post(c, text);
      if (typeof mid === 'number') await rpc(c, { type: 'delivered', ids: [mid] });
    } else if (m.event === 'notification' && !c.busy && Array.isArray(m.payload)) {
      const details = m.payload.map((n: any) => n?.detail).filter(Boolean);
      if (details.length) await post(c, '[murmur notice] ' + details.join('; '));
    }
  };

  const onLine = (c: Conn, line: string) => {
    let m: any;
    try {
      m = JSON.parse(line);
    } catch {
      return;
    }
    if (m.type === 'event') {
      onEvent(c, m).catch(() => {});
      return;
    }
    const p = c.pending.get(m.id);
    if (!p) return;
    c.pending.delete(m.id);
    clearTimeout(p.timer);
    if (m.ok) p.resolve(m.result);
    else p.reject(new Error(m.error?.message ?? 'murmur error'));
  };

  const open = async (c: Conn): Promise<void> => {
    try {
      const token = readFileSync(join(HOME, 'token'), 'utf8').trim();
      const sock = await new Promise<Socket>((resolve, reject) => {
        const s = connect(join(HOME, 'bus.sock'));
        s.once('connect', () => resolve(s));
        s.once('error', reject);
      });
      c.sock = sock;
      c.buf = '';
      c.nextId = 1;
      sock.setEncoding('utf8');
      sock.on('data', (d: string) => {
        c.buf += d;
        let i;
        while ((i = c.buf.indexOf('\n')) >= 0) {
          const line = c.buf.slice(0, i);
          c.buf = c.buf.slice(i + 1);
          if (line) onLine(c, line);
        }
      });
      sock.on('error', () => {});
      sock.on('close', () => {
        if (c.sock === sock) drop(c);
      });
      await rpc(c, { type: 'auth', token });
      const r = await lifecycle(c, 'session-start');
      c.handle = r?.handle ?? c.handle;
    } catch {
      drop(c);
    }
  };

  const ensure = (sid: string, cwd?: string): Conn => {
    let c = conns.get(sid);
    if (!c) {
      c = {
        sid, cwd: cwd || directory, alive: true, busy: false, handle: null, sock: null,
        buf: '', nextId: 1, pending: new Map(), retry: null, ready: Promise.resolve(),
      };
      conns.set(sid, c);
      c.ready = open(c);
    }
    return c;
  };

  const send = (sid: string, event: string) => {
    const c = conns.get(sid);
    if (c?.sock) lifecycle(c, event).catch(() => {});
  };

  const setBusy = (sid: string, cwd?: string) => {
    const c = ensure(sid, cwd);
    if (c.busy) return;
    c.busy = true;
    send(sid, 'prompt');
  };

  const call = async (sid: string, cwd: string, msg: Record<string, unknown>, timeoutMs?: number) => {
    try {
      const c = ensure(sid, cwd);
      await c.ready;
      return JSON.stringify(await rpc(c, msg, timeoutMs));
    } catch (e) {
      return JSON.stringify({ error: e instanceof Error ? e.message : String(e) });
    }
  };

  const s = tool.schema;
  const refs = s.array(s.string()).optional();
  const ctxCall = (msg: Record<string, unknown>, ctx: { sessionID: string; directory: string }, t?: number) =>
    call(ctx.sessionID, ctx.directory, msg, t);

  return {
    event: async ({ event }) => {
      try {
        const e: any = event;
        const p = e.properties ?? {};
        switch (e.type) {
          case 'session.created':
            ensure(p.info.id, p.info.directory);
            break;
          case 'session.status':
            if (p.status?.type === 'busy') setBusy(p.sessionID);
            break;
          case 'session.idle': {
            const c = ensure(p.sessionID);
            c.busy = false;
            send(p.sessionID, 'stop');
            break;
          }
          case 'session.deleted': {
            const c = conns.get(p.info.id);
            if (!c) break;
            c.alive = false;
            if (c.retry) clearTimeout(c.retry);
            const end = c.sock ? lifecycle(c, 'session-end').catch(() => {}) : Promise.resolve();
            end.finally(() => drop(c));
            conns.delete(p.info.id);
            break;
          }
        }
      } catch {
        // never throw out of a hook
      }
    },
    'chat.message': async ({ sessionID }) => {
      try {
        setBusy(sessionID);
      } catch {
        // never throw out of a hook
      }
    },
    tool: {
      murmur_send: tool({
        description: 'Send a message to murmur peers or groups, optionally into an existing thread.',
        args: {
          to: s.array(s.string()),
          body: s.string(),
          threadId: s.number().optional(),
          references: refs,
          mode: s.enum(['push', 'notify', 'silent']).optional(),
        },
        execute: (a, ctx) =>
          ctxCall({ type: 'send', threadId: a.threadId ?? null, to: a.to, body: a.body, references: a.references, mode: a.mode }, ctx),
      }),
      murmur_reply: tool({
        description: 'Reply to a murmur message; the reply reaches everyone in the thread, so start the body with AGREE, DISAGREE or INFO.',
        args: {
          messageId: s.number(),
          body: s.string(),
          verdict: s.enum(['agree', 'disagree', 'info']).optional(),
          references: refs,
        },
        execute: (a, ctx) =>
          ctxCall({ type: 'reply', messageId: a.messageId, body: a.body, verdict: a.verdict, references: a.references }, ctx),
      }),
      murmur_thread: tool({
        description: 'Read a murmur thread, optionally only messages after a sequence number.',
        args: { threadId: s.number(), sinceSeq: s.number().optional(), full: s.boolean().optional() },
        execute: (a, ctx) =>
          ctxCall({ type: 'thread', threadId: a.threadId, sinceSeq: a.sinceSeq ?? null, full: a.full ?? false }, ctx),
      }),
      murmur_wait: tool({
        description: 'Wait until all or any recipients of a murmur thread have replied.',
        args: {
          threadId: s.number(),
          until: s.enum(['all_replied', 'any_replied']),
          timeoutMs: s.number().optional(),
        },
        execute: (a, ctx) => {
          const timeoutMs = Math.min(Math.max(a.timeoutMs ?? 120000, 0), 300000);
          return ctxCall({ type: 'wait', threadId: a.threadId, until: a.until, timeoutMs }, ctx, timeoutMs + 10000);
        },
      }),
      murmur_status: tool({
        description: 'Show murmur peers, presence and delivery status.',
        args: {},
        execute: (_a, ctx) => ctxCall({ type: 'status' }, ctx),
      }),
      murmur_groups: tool({
        description: 'Create, list, change or delete murmur groups.',
        args: {
          action: s.enum(['create', 'list', 'add', 'remove', 'delete']),
          name: s.string().optional(),
          members: s.array(s.string()).optional(),
        },
        execute: (a, ctx) => ctxCall({ type: 'groups', action: a.action, name: a.name, members: a.members }, ctx),
      }),
      murmur_name: tool({
        description: 'Rename this session on the murmur bus.',
        args: { handle: s.string() },
        execute: (a, ctx) =>
          ctxCall({ type: 'register', clientKind: 'adapter', host, handle: a.handle }, ctx),
      }),
    },
  };
};

export default plugin;
