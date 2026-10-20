import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { Broker } from './broker.js';
import { MurmurClient, isBrokerDown } from './client.js';
import { ensureHome, getPaths, readToken } from './config.js';
import { HANDLE_RE, MurmurCore } from './core.js';
import type { CoreEvents } from './core.js';
import { runHook } from './hooks.js';
import { installClient } from './install.js';
import { runMcpServer } from './mcp.js';
import { MurmurError } from './protocol.js';
import type {
  DeliveryMode,
  GroupAction,
  GroupsMessage,
  GroupsResult,
  HookEvent,
  RegisterMessage,
  RegisterResult,
  SendMessage,
  SendResult,
  StatusResult,
  ThreadMessage,
  ThreadResult,
} from './protocol.js';
import { Store } from './store.js';

// The murmur command line. Every subcommand is a thin driver over the broker
// protocol: parse argv, connect, request, print. runCli takes an injectable
// io pair so tests can capture output without touching real streams.

export interface CliIo {
  out?: (s: string) => void;
  err?: (s: string) => void;
}

type Sink = (s: string) => void;

const OWNER_HANDLE = 'renn';
const OWNER_DISPLAY = 'renn (owner)';

const SEND_USAGE =
  'usage: murmur send <targets> <body...> [--as <handle>] [--mode <mode>] [--refs <a,b>] [--thread <id>]';
const THREAD_USAGE = 'usage: murmur thread <id> [--since <n>] [--full]';
const GROUPS_USAGE =
  'usage: murmur groups [list] | groups create <name> | groups add <name> <members...> | groups remove <name> <members...> | groups delete <name>';
const SLASH_USAGE = '/murmur <@group | handle[,more]> <message>   (run bare like this for peers and groups)';

export async function runCli(argv: string[], io?: CliIo): Promise<number> {
  // The default io is captured at call time and touches only the real
  // process streams, so injected io replaces them completely.
  const out: Sink = io?.out ?? ((s: string) => { process.stdout.write(s); });
  const err: Sink = io?.err ?? ((s: string) => { process.stderr.write(s); });
  try {
    return await dispatch(argv, out, err);
  } catch (e) {
    if (isBrokerDown(e)) {
      err('murmur broker is down; start it with: murmur serve\n');
      return 2;
    }
    if (e instanceof MurmurError) {
      err(e.message + '\n');
      return 1;
    }
    if (e instanceof Error) {
      err(e.message + '\n');
      return 1;
    }
    throw e;
  }
}

async function dispatch(argv: string[], out: Sink, err: Sink): Promise<number> {
  const command = argv[0];
  if (command === undefined) {
    printUsage(err);
    return 1;
  }
  switch (command) {
    case 'init':
      return cmdInit(out);
    case 'serve':
      return cmdServe(argv.slice(1), out);
    case 'mcp':
      await runMcpServer();
      return 0;
    case 'hook':
      return cmdHook(argv.slice(1), out, err);
    case 'send':
      return cmdSend(argv.slice(1), out, err);
    case 'slash':
      return cmdSlash(argv.slice(1), out, err);
    case 'status':
      return cmdStatus(out);
    case 'thread':
      return cmdThread(argv.slice(1), out, err);
    case 'groups':
      return cmdGroups(argv.slice(1), out, err);
    case 'install':
      return cmdInstall(argv.slice(1), out, err);
    default:
      err('murmur: unknown command: ' + command + '\n');
      printUsage(err);
      return 1;
  }
}

function printUsage(err: Sink): void {
  const lines = [
    'usage: murmur <command> [args]',
    '',
    'commands:',
    '  init                     create the murmur home and the owner peer',
    '  serve [--daemon]         start the broker and print the socket path',
    '  mcp                      serve the murmur MCP server over stdio',
    '  hook <event>             run one client hook (session-start, prompt, stop)',
    '  send <targets> <body...> send a message; flags: --as, --mode, --refs, --thread',
    '  slash [args...]          the /murmur backing: bare prints usage, peers and groups',
    '  status                   list peers with presence and unread counts',
    '  thread <id> [--since <n>] [--full]',
    '  groups [list] | groups create <name> | groups add <name> <members...>',
    '         | groups remove <name> <members...> | groups delete <name>',
    '  install <claude|zcode|hummin>',
  ];
  err(lines.join('\n') + '\n');
}

async function cmdInit(out: Sink): Promise<number> {
  const paths = getPaths();
  ensureHome(paths);
  const store = Store.open(paths.dbPath);
  try {
    store.upsertPeer(OWNER_HANDLE, null, null, true, Date.now());
  } finally {
    store.close();
  }
  out(paths.home + '\n');
  return 0;
}

async function cmdServe(args: string[], out: Sink): Promise<number> {
  if (args.includes('--daemon')) {
    // Re-spawn this exact entry file without the flag, detached from the
    // current terminal, then report success immediately.
    const self = process.argv[1] ?? '';
    const child = spawn(process.execPath, [self, 'serve'], { detached: true, stdio: 'ignore' });
    child.unref();
    return 0;
  }
  const paths = getPaths();
  ensureHome(paths);
  const store = Store.open(paths.dbPath);
  const events: CoreEvents = {
    onAppend: () => {},
    onStateChange: () => {},
  };
  const core = new MurmurCore(store, events);
  const broker = await Broker.start({ paths, store, core });
  out(paths.socketPath + '\n');
  let stopping = false;
  await new Promise<void>((resolve) => {
    const stop = (): void => {
      if (stopping) {
        return;
      }
      stopping = true;
      void broker
        .stop()
        .catch(() => undefined)
        .then(() => {
          store.close();
          resolve();
        });
    };
    process.on('SIGINT', stop);
    process.on('SIGTERM', stop);
  });
  return 0;
}

function isHookEvent(value: string): value is HookEvent {
  return value === 'session-start' || value === 'prompt' || value === 'stop';
}

async function cmdHook(args: string[], out: Sink, err: Sink): Promise<number> {
  const event = args[0];
  if (event === undefined || !isHookEvent(event)) {
    err('usage: murmur hook <session-start|prompt|stop>\n');
    return 1;
  }
  const stdinText = await readAllStdin();
  const outcome = await runHook(event, stdinText, process.env);
  out(withNewline(outcome.stdout));
  return outcome.exitCode;
}

function withNewline(text: string): string {
  if (text.length === 0 || text.endsWith('\n')) {
    return text;
  }
  return text + '\n';
}

function readAllStdin(): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    let text = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk: string) => {
      text += chunk;
    });
    process.stdin.once('end', () => {
      resolve(text);
    });
    process.stdin.once('error', (ioErr: Error) => {
      reject(ioErr);
    });
  });
}

async function cmdSend(args: string[], out: Sink, err: Sink): Promise<number> {
  let asHandle: string | null = null;
  let mode: DeliveryMode | undefined;
  let refs: string[] | undefined;
  let threadId: number | null = null;
  let flagsOk = true;
  const positionals: string[] = [];
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === undefined) {
      break;
    }
    if (arg === '--as' || arg === '--mode' || arg === '--refs' || arg === '--thread') {
      const value = args[i + 1];
      if (value === undefined) {
        err('murmur send: flag ' + arg + ' needs a value\n' + SEND_USAGE + '\n');
        return 1;
      }
      i += 1;
      if (arg === '--as') {
        asHandle = value;
      } else if (arg === '--mode') {
        // The broker validates the mode value against the wire enum.
        mode = value as DeliveryMode;
      } else if (arg === '--refs') {
        refs = value
          .split(',')
          .map((part) => part.trim())
          .filter((part) => part.length > 0);
      } else {
        const parsed = Number(value);
        if (!Number.isInteger(parsed) || parsed < 0) {
          err('murmur send: --thread needs a non-negative integer, got: ' + value + '\n');
          flagsOk = false;
        } else {
          threadId = parsed;
        }
      }
      continue;
    }
    positionals.push(arg);
  }
  if (!flagsOk) {
    return 1;
  }
  if (asHandle !== null && !HANDLE_RE.test(asHandle)) {
    // Rejected locally with the same message the broker's register
    // validation would produce, before any connection is made.
    err('invalid handle: ' + asHandle + '\n');
    return 1;
  }
  const targetsArg = positionals[0];
  if (targetsArg === undefined) {
    err('murmur send: missing targets\n' + SEND_USAGE + '\n');
    return 1;
  }
  const body = positionals.slice(1).join(' ');
  if (body.length === 0) {
    err('murmur send: missing body\n' + SEND_USAGE + '\n');
    return 1;
  }
  const to = targetsArg
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  const client = await connectCli();
  try {
    // Default identity is the owner. With --as the session registers under
    // its own handle with no display name, so its replies never speak as the
    // owner.
    const reg: Omit<RegisterMessage, 'v' | 'id'> =
      asHandle === null
        ? { type: 'register', handle: OWNER_HANDLE, display: OWNER_DISPLAY, clientKind: 'cli' }
        : { type: 'register', handle: asHandle, clientKind: 'cli' };
    await client.request<RegisterResult>(reg);
    const msg: Omit<SendMessage, 'v' | 'id'> = { type: 'send', threadId, to, body };
    if (mode !== undefined) {
      msg.mode = mode;
    }
    if (refs !== undefined) {
      msg.references = refs;
    }
    const result = await client.request<SendResult>(msg);
    for (const delivery of result.deliveries) {
      out(delivery.recipient + ': ' + delivery.state + '\n');
    }
    out(
      'message ' + String(result.messageId) + ' thread ' + String(result.threadId) + ' seq ' + String(result.seq) + '\n',
    );
    return 0;
  } finally {
    client.close();
  }
}

// Auth-only connection for commands that never need a bound handle. Only
// send registers, because registration refreshes the peer row.
async function connectCli(): Promise<MurmurClient> {
  const paths = getPaths();
  const token = readToken(paths);
  return MurmurClient.connect({ socketPath: paths.socketPath, token });
}

// One formatted line per peer, exactly as the status view prints them. The
// slash discovery view reuses these so the two can never drift apart.
async function statusLines(): Promise<string[]> {
  const client = await connectCli();
  try {
    const result = await client.request<StatusResult>({ type: 'status' });
    return result.peers.map((peer) => {
      let line = peer.handle + ' ' + peer.presence + ' unread ' + String(peer.unread);
      if (peer.display !== null) {
        line += ' ' + peer.display;
      }
      return line;
    });
  } finally {
    client.close();
  }
}

async function cmdStatus(out: Sink): Promise<number> {
  const lines = await statusLines();
  for (const line of lines) {
    out(line + '\n');
  }
  return 0;
}

async function cmdThread(args: string[], out: Sink, err: Sink): Promise<number> {
  let full = false;
  let sinceSeq: number | null = null;
  const positionals: string[] = [];
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === undefined) {
      break;
    }
    if (arg === '--full') {
      full = true;
      continue;
    }
    if (arg === '--since') {
      const value = args[i + 1];
      if (value === undefined) {
        err('murmur thread: --since needs a value\n' + THREAD_USAGE + '\n');
        return 1;
      }
      i += 1;
      const parsed = Number(value);
      if (!Number.isInteger(parsed) || parsed < 0) {
        err('murmur thread: --since needs a non-negative integer, got: ' + value + '\n');
        return 1;
      }
      sinceSeq = parsed;
      continue;
    }
    positionals.push(arg);
  }
  const idArg = positionals[0];
  if (idArg === undefined) {
    err('murmur thread: missing thread id\n' + THREAD_USAGE + '\n');
    return 1;
  }
  const threadId = Number(idArg);
  if (!Number.isInteger(threadId) || threadId < 0) {
    err('murmur thread: thread id must be a non-negative integer, got: ' + idArg + '\n');
    return 1;
  }
  const client = await connectCli();
  try {
    const msg: Omit<ThreadMessage, 'v' | 'id'> = { type: 'thread', threadId, sinceSeq, full };
    const result = await client.request<ThreadResult>(msg);
    for (const message of result.messages) {
      const text = full ? message.body : message.firstLine;
      out(String(message.seq) + ' ' + message.sender + ' ' + text + '\n');
    }
    return 0;
  } finally {
    client.close();
  }
}

// Runs one groups action and returns one formatted line per group, exactly as
// the groups view prints them; the slash discovery view reuses the list form.
async function groupsLines(action: GroupAction, name?: string, members?: string[]): Promise<string[]> {
  const client = await connectCli();
  try {
    const msg: Omit<GroupsMessage, 'v' | 'id'> = { type: 'groups', action };
    if (name !== undefined) {
      msg.name = name;
    }
    if (members !== undefined) {
      msg.members = members;
    }
    const groups = await client.request<GroupsResult[]>(msg);
    return groups.map((group) => {
      const memberText = group.members.length > 0 ? ' ' + group.members.join(', ') : '';
      return group.name + ':' + memberText;
    });
  } finally {
    client.close();
  }
}

async function cmdGroups(args: string[], out: Sink, err: Sink): Promise<number> {
  const sub = args[0];
  let action: GroupAction = 'list';
  let name: string | undefined;
  let members: string[] | undefined;
  if (sub === undefined || sub === 'list') {
    action = 'list';
  } else if (sub === 'create' || sub === 'delete') {
    const value = args[1];
    if (value === undefined) {
      err('murmur groups ' + sub + ': missing group name\n' + GROUPS_USAGE + '\n');
      return 1;
    }
    action = sub;
    name = value;
  } else if (sub === 'add' || sub === 'remove') {
    const value = args[1];
    const rest = args.slice(2);
    if (value === undefined || rest.length === 0) {
      err('murmur groups ' + sub + ': need a group name and at least one member\n' + GROUPS_USAGE + '\n');
      return 1;
    }
    action = sub;
    name = value;
    members = rest;
  } else {
    err('murmur groups: unknown action: ' + sub + '\n' + GROUPS_USAGE + '\n');
    return 1;
  }
  const lines = await groupsLines(action, name, members);
  for (const line of lines) {
    out(line + '\n');
  }
  return 0;
}

// The backing of the /murmur command. Bare, it is the discovery surface: the
// usage line, then the peers exactly as status prints them, then the groups
// exactly as the groups list prints them. 'status' and 'groups' delegate to
// those views; anything else is a plain send with the same parsing.
async function cmdSlash(args: string[], out: Sink, err: Sink): Promise<number> {
  const sub = args[0];
  if (sub === 'status') {
    return cmdStatus(out);
  }
  if (sub === 'groups') {
    return cmdGroups(args.slice(1), out, err);
  }
  if (sub !== undefined) {
    return cmdSend(args, out, err);
  }
  const peers = await statusLines();
  const groups = await groupsLines('list');
  out(SLASH_USAGE + '\n');
  out('\npeers:\n');
  for (const line of peers) {
    out(line + '\n');
  }
  out('\ngroups:\n');
  for (const line of groups) {
    out(line + '\n');
  }
  return 0;
}

function isInstallClient(value: string): value is 'claude' | 'zcode' | 'hummin' {
  return value === 'claude' || value === 'zcode' || value === 'hummin';
}

async function cmdInstall(args: string[], out: Sink, err: Sink): Promise<number> {
  const name = args[0];
  if (name === undefined || !isInstallClient(name)) {
    err('usage: murmur install <claude|zcode|hummin>\n');
    return 1;
  }
  const files = await installClient(name);
  for (const file of files) {
    out(file + '\n');
  }
  return 0;
}

// The entry guard lives in one function so every branch of it is attributed
// to a function body, which the v8 coverage provider credits reliably even
// when a second module instance (a dynamic re-import) runs the entry path.
function runWhenEntry(): boolean {
  const isEntry: boolean = import.meta.url === pathToFileURL(process.argv[1] ?? '').href;
  if (isEntry) {
    void runCli(process.argv.slice(2)).then((code: number) => {
      process.exit(code);
    });
  }
  return isEntry;
}
runWhenEntry();
