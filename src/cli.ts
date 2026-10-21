import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { MurmurClient, connectBroker, isBrokerDown } from './client.js';
import { ensureHome, getPaths, readToken } from './config.js';
import type { CoreEvents } from './core.js';
import { HOOK_CLIENTS, runHook } from './hooks.js';
import type { HookClient } from './hooks.js';
import { INSTALL_CLIENTS, installClient } from './install.js';
import type { InstallClient } from './install.js';
import { currentHost } from './proc.js';
import { HANDLE_RE, MurmurError } from './protocol.js';
import type {
  DeliveryMode,
  GroupAction,
  GroupsMessage,
  GroupsResult,
  HookEvent,
  PruneResult,
  RegisterMessage,
  RegisterResult,
  ReplyMessage,
  ReplyResult,
  SendMessage,
  SendResult,
  StatusResult,
  ThreadListEntry,
  ThreadMessage,
  ThreadsMessage,
  ThreadResult,
} from './protocol.js';

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
const REPLY_USAGE = 'usage: murmur reply <message id> <body...> [--as <handle>] [--verdict agree|disagree|info|choice] [--refs <a,b>]';
const NAME_USAGE = 'usage: murmur name <handle>   (renames the agent session this runs in)';
const THREAD_USAGE = 'usage: murmur thread [<id> [--since <n>] [--full] [--message <id>]] (no id lists recent threads)';
const GROUPS_USAGE =
  'usage: murmur groups [list] | groups create <name> | groups add <name> <members...> | groups remove <name> <members...> | groups delete <name>';
const SLASH_USAGE =
  '/murmur <group | handle[,more]> <message>   (sent as renn, via the session you type it in; run bare like this for peers and groups)';

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
      // The MCP SDK and zod cost more to load than everything else here, and
      // only this command needs them; hooks and sends run on every prompt.
      await (await import('./mcp.js')).runMcpServer();
      return 0;
    case 'hook':
      return cmdHook(argv.slice(1), out, err);
    case 'send':
      return cmdSend(argv.slice(1), out, err, 'session');
    case 'reply':
      return cmdReply(argv.slice(1), out, err);
    case 'name':
      return cmdName(argv.slice(1), out, err);
    case 'prune':
      return cmdPrune(out);
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
    '  hook <event> [--client <cli>]  run one hook (session-start, prompt, stop, session-end)',
    '  send <targets> <body...> send a message; flags: --as, --mode, --refs, --thread',
    '  reply <id> <body...>     reply into a thread; flags: --as, --verdict, --refs',
    '  name <handle>            rename the agent session this runs in',
    '  slash [args...]          the /murmur backing, sent as the owner via your session; bare prints peers and groups',
    '  prune                    remove peers with no session, message, group or connection',
    '  status                   list peers with presence and unread counts',
    '  thread [<id> [--since <n>] [--full] [--message <id>]]  no id lists recent threads',
    '  groups [list] | groups create <name> | groups add <name> <members...>',
    '         | groups remove <name> <members...> | groups delete <name>',
    '  install <' + INSTALL_CLIENTS.join('|') + '>',
  ];
  err(lines.join('\n') + '\n');
}

async function cmdInit(out: Sink): Promise<number> {
  const { Store } = await import('./store.js');
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
  const [{ Store }, { MurmurCore }, { Broker }] = await Promise.all([
    import('./store.js'),
    import('./core.js'),
    import('./broker.js'),
  ]);
  const paths = getPaths();
  ensureHome(paths);
  const store = Store.open(paths.dbPath);
  // An autostarted broker may run before murmur init ever did.
  if (!store.listPeers().some((peer) => peer.isOwner)) {
    store.upsertPeer(OWNER_HANDLE, OWNER_DISPLAY, null, true, Date.now());
  }
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
  return value === 'session-start' || value === 'prompt' || value === 'stop' || value === 'session-end';
}

async function cmdHook(args: string[], out: Sink, err: Sink): Promise<number> {
  const event = args[0];
  const clientFlag = args.indexOf('--client');
  const client = clientFlag === -1 ? 'claude' : args[clientFlag + 1];
  if (event === undefined || !isHookEvent(event) || !HOOK_CLIENTS.includes(client as HookClient)) {
    err('usage: murmur hook <session-start|prompt|stop|session-end> [--client ' + HOOK_CLIENTS.join('|') + ']\n');
    return 1;
  }
  const stdinText = await readAllStdin();
  const outcome = await runHook(event, stdinText, process.env, { client: client as HookClient });
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

// Who a CLI command speaks as. --as wins. Otherwise a command run inside an
// agent session (any ancestor is a live session's host) speaks as that
// session, and one run from a plain terminal speaks as the owner. The
// /murmur surface is the human typing, so it always speaks as the owner, and
// the session it was typed in (an ancestor host) rides along as the via.
type Speaker = 'session' | 'owner';

function identityMessage(asHandle: string | null, speaker: Speaker): Omit<RegisterMessage, 'v' | 'id'> {
  if (asHandle !== null) {
    return { type: 'register', handle: asHandle, clientKind: 'cli' };
  }
  if (speaker === 'owner') {
    return {
      type: 'register',
      owner: true,
      display: OWNER_DISPLAY,
      clientKind: 'cli',
      ancestors: currentHost()?.ancestors ?? [],
    };
  }
  return { type: 'register', clientKind: 'cli', ancestors: currentHost()?.ancestors ?? [] };
}

interface ParsedFlags {
  values: Map<string, string>;
  positionals: string[];
}

// Splits --flag value pairs from positionals. Null (after printing why)
// when a flag is missing its value.
function parseFlags(args: string[], flags: string[], usage: string, name: string, err: Sink): ParsedFlags | null {
  const values = new Map<string, string>();
  const positionals: string[] = [];
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i] as string;
    if (flags.includes(arg)) {
      const value = args[i + 1];
      if (value === undefined) {
        err('murmur ' + name + ': flag ' + arg + ' needs a value\n' + usage + '\n');
        return null;
      }
      values.set(arg, value);
      i += 1;
      continue;
    }
    positionals.push(arg);
  }
  return { values, positionals };
}

function splitRefs(value: string | undefined): string[] | undefined {
  return value
    ?.split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function validAs(value: string | undefined, err: Sink): string | null | false {
  if (value === undefined) {
    return null;
  }
  if (!HANDLE_RE.test(value)) {
    // Rejected locally with the same message the broker would produce,
    // before any connection is made.
    err('invalid handle: ' + value + '\n');
    return false;
  }
  return value;
}

async function cmdSend(args: string[], out: Sink, err: Sink, speaker: Speaker): Promise<number> {
  const parsed = parseFlags(args, ['--as', '--mode', '--refs', '--thread'], SEND_USAGE, 'send', err);
  if (parsed === null) {
    return 1;
  }
  let threadId: number | null = null;
  const threadValue = parsed.values.get('--thread');
  if (threadValue !== undefined) {
    const value = Number(threadValue);
    if (!Number.isInteger(value) || value < 0) {
      err('murmur send: --thread needs a non-negative integer, got: ' + threadValue + '\n');
      return 1;
    }
    threadId = value;
  }
  const asHandle = validAs(parsed.values.get('--as'), err);
  if (asHandle === false) {
    return 1;
  }
  const targetsArg = parsed.positionals[0];
  if (targetsArg === undefined) {
    err('murmur send: missing targets\n' + SEND_USAGE + '\n');
    return 1;
  }
  const body = parsed.positionals.slice(1).join(' ');
  if (body.length === 0) {
    err('murmur send: missing body\n' + SEND_USAGE + '\n');
    return 1;
  }
  return sendAs(identityMessage(asHandle, speaker), targetsArg, body, threadId, parsed.values, out);
}

async function sendAs(
  identity: Omit<RegisterMessage, 'v' | 'id'>,
  targetsArg: string,
  body: string,
  threadId: number | null,
  flags: Map<string, string>,
  out: Sink,
): Promise<number> {
  const to = targetsArg
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  const client = await connectCli();
  try {
    await client.request<RegisterResult>(identity);
    const msg: Omit<SendMessage, 'v' | 'id'> = { type: 'send', threadId, to, body };
    const mode = flags.get('--mode');
    if (mode !== undefined) {
      // The broker validates the mode value against the wire enum.
      msg.mode = mode as DeliveryMode;
    }
    const refs = splitRefs(flags.get('--refs'));
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

async function cmdReply(args: string[], out: Sink, err: Sink): Promise<number> {
  const parsed = parseFlags(args, ['--as', '--verdict', '--refs'], REPLY_USAGE, 'reply', err);
  if (parsed === null) {
    return 1;
  }
  const asHandle = validAs(parsed.values.get('--as'), err);
  if (asHandle === false) {
    return 1;
  }
  const idArg = parsed.positionals[0];
  const messageId = Number(idArg);
  const body = parsed.positionals.slice(1).join(' ');
  if (idArg === undefined || !Number.isInteger(messageId) || messageId < 0 || body.length === 0) {
    err('murmur reply: need a message id and a body\n' + REPLY_USAGE + '\n');
    return 1;
  }
  const client = await connectCli();
  try {
    await client.request<RegisterResult>(identityMessage(asHandle, 'session'));
    const msg: Omit<ReplyMessage, 'v' | 'id'> = { type: 'reply', messageId, body };
    const verdict = parsed.values.get('--verdict');
    if (verdict !== undefined) {
      // The broker validates the verdict against the wire enum.
      msg.verdict = verdict as ReplyMessage['verdict'] & string;
    }
    const refs = splitRefs(parsed.values.get('--refs'));
    if (refs !== undefined) {
      msg.references = refs;
    }
    const result = await client.request<ReplyResult>(msg);
    out('message ' + String(result.messageId) + ' thread ' + String(result.threadId) + ' seq ' + String(result.seq) + '\n');
    return 0;
  } finally {
    client.close();
  }
}

// Renames the agent session this command runs in (its host process).
async function cmdName(args: string[], out: Sink, err: Sink): Promise<number> {
  const handle = args[0];
  const host = currentHost(undefined, false);
  if (handle === undefined || !HANDLE_RE.test(handle)) {
    err('murmur name: need a valid handle (a-z, 0-9, -)\n' + NAME_USAGE + '\n');
    return 1;
  }
  if (host === null) {
    err('murmur name: cannot find the agent session this runs in\n');
    return 1;
  }
  const client = await connectCli();
  try {
    const msg: Omit<RegisterMessage, 'v' | 'id'> = {
      type: 'register',
      clientKind: 'cli',
      handle,
      host: { hostPid: host.hostPid, hostStart: host.hostStart },
    };
    const result = await client.request<RegisterResult>(msg);
    out(result.handle + '\n');
    return 0;
  } finally {
    client.close();
  }
}

// Auth-only connection; each command registers the identity it needs.
async function connectCli(): Promise<MurmurClient> {
  const paths = getPaths();
  const token = readToken(paths);
  return connectBroker({ socketPath: paths.socketPath, token });
}

// One formatted line per peer, exactly as the status view prints them. The
// slash discovery view reuses these so the two can never drift apart.
async function statusLines(): Promise<string[]> {
  const client = await connectCli();
  try {
    const result = await client.request<StatusResult>({ type: 'status' });
    return result.peers.map((peer) => {
      let line = peer.handle + ' ' + peer.presence + (peer.client === null ? '' : ' ' + peer.client) + ' unread ' + String(peer.unread);
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

async function cmdPrune(out: Sink): Promise<number> {
  const client = await connectCli();
  try {
    const result = await client.request<PruneResult>({ type: 'prune' });
    for (const handle of result.removed) {
      out(handle + '\n');
    }
    if (result.removed.length === 0) {
      out('nothing to prune\n');
    }
    return 0;
  } finally {
    client.close();
  }
}

async function cmdThread(args: string[], out: Sink, err: Sink): Promise<number> {
  let full = false;
  let sinceSeq: number | null = null;
  let messageId: number | undefined;
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
    if (arg === '--message') {
      const parsed = Number(args[i + 1]);
      if (args[i + 1] === undefined || !Number.isInteger(parsed) || parsed < 0) {
        err('murmur thread: --message needs a non-negative integer\n' + THREAD_USAGE + '\n');
        return 1;
      }
      i += 1;
      messageId = parsed;
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
  if (idArg === undefined && !full && sinceSeq === null && messageId === undefined) {
    const client = await connectCli();
    try {
      const list: Omit<ThreadsMessage, 'v' | 'id'> = { type: 'threads' };
      for (const t of await client.request<ThreadListEntry[]>(list)) {
        out('t' + String(t.threadId) + ' s' + String(t.latestSeq) + ' ' + new Date(t.lastAt).toISOString() + ' ' +
          t.participants.join(',') + ' ' + String(t.messages) + ' ' + t.first + '\n');
      }
      return 0;
    } finally {
      client.close();
    }
  }
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
    const msg: Omit<ThreadMessage, 'v' | 'id'> = { type: 'thread', threadId, sinceSeq, full, ...(messageId !== undefined ? { messageId } : {}) };
    const result = await client.request<ThreadResult>(msg);
    for (const message of result.messages) {
      const text = full || messageId !== undefined ? (message.body as string) : message.firstLine;
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

// The backing of the /murmur command, always speaking as the owner. The
// whole argument text arrives either on stdin (--stdin: the command file
// feeds it through a quoted heredoc so no shell ever parses the message)
// or as one shell-escaped word (gemini): the first word is the targets,
// the rest is the body, verbatim. Bare, it is the discovery surface:
// usage, then peers as status prints them, then groups as the groups list
// prints them.
async function cmdSlash(args: string[], out: Sink, err: Sink): Promise<number> {
  let words = args;
  const oneWord = args.length === 1 && /\s/.test(args[0] as string);
  if (args[0] === '--stdin' || oneWord) {
    let text = (oneWord ? (args[0] as string) : await readAllStdin()).trim();
    // Claude Code rewrites a "!" that starts a word in $ARGUMENTS to "\!"
    // (its guard against injected shell blocks); undo exactly that.
    if (args[1] === '--claude-escaped') {
      text = text.replace(/(^|\s)\\!/g, '$1!');
    }
    const match = /^(\S+)\s+([\s\S]+)$/.exec(text);
    if (match !== null) {
      return sendAs(identityMessage(null, 'owner'), match[1] as string, match[2] as string, null, new Map(), out);
    }
    words = text.length === 0 ? [] : [text];
  }
  const sub = words[0];
  if (sub === 'status') {
    return cmdStatus(out);
  }
  if (sub === 'groups') {
    return cmdGroups(words.slice(1), out, err);
  }
  if (sub !== undefined) {
    return cmdSend(words, out, err, 'owner');
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

async function cmdInstall(args: string[], out: Sink, err: Sink): Promise<number> {
  const name = args[0];
  if (name === undefined || !INSTALL_CLIENTS.includes(name as InstallClient)) {
    err('usage: murmur install <' + INSTALL_CLIENTS.join('|') + '>\n');
    return 1;
  }
  for (const line of await installClient(name as InstallClient)) {
    out(line + '\n');
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
