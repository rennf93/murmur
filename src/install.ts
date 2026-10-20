import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';

// Wiring for agent clients. Every write is an idempotent merge: existing
// content is preserved and murmur entries are only added when missing, and
// every touched file gets a one-time backup copy with the .bak-murmur
// suffix before the first murmur write touches it.

const BACKUP_SUFFIX = '.bak-murmur';

const CLAUDE_COMMANDS_MD = `---
description: Send to a murmur group or peer; with no arguments it lists peers, groups and usage
argument-hint: <@group | handle> <message>
---

!murmur slash $ARGUMENTS

<!-- The bang (!) pre-exec form is a command-file feature (a pre-executed shell
line), distinct from typing !command in the input box, and it must be verified
against the installed Claude version. The fallback block below covers versions
where it does not run. -->

Fallback when the pre-exec line above produced no output: use the Bash tool
with exactly this command.

    murmur slash $ARGUMENTS

Then report the printed lines verbatim: when a message was sent, the delivery
states, one line per recipient, plus the final line with the message id,
thread id and seq; when no arguments were given, the bus status as printed,
the usage line, the peers and the groups.
`;

const ZCODE_SKILL_MD = `---
name: murmur
description: Send, read and reply to messages on the murmur local message bus from Bash.
---

# murmur

Murmur is a local message bus between AI sessions. Use the murmur CLI through
the Bash tool for everything. CLI-first: no MCP tool calls and no node -e
snippets are needed.

## Send

    murmur send <targets> <body>

targets is a comma separated list of handles and @groups, for example
renn,claude-a12b,@dev. Useful flags:

    --thread <id>     send into an existing thread (use this to reply)
    --mode <mode>     push (default), notify or silent
    --refs <a,b>      reference other handles

Output is one "recipient: state" line per delivery plus a final line with the
message id, thread id and seq.

## Read

    murmur status                      peers, presence and unread counts
    murmur thread <id>                 digest of a thread
    murmur thread <id> --full          full bodies
    murmur thread <id> --since <seq>   only entries after a known seq

## Poll for replies at turn starts

At the start of every turn, poll the threads this session participates in:
run murmur thread <id> --since <last seq seen> and read any new lines before
answering. Reply with murmur send --as <your-handle> --thread <id> <targets> <body>.
`;

// The Claude hooks we install, one murmur command per settings event.
const CLAUDE_HOOKS: Array<{ event: string; command: string }> = [
  { event: 'SessionStart', command: 'murmur hook session-start' },
  { event: 'UserPromptSubmit', command: 'murmur hook prompt' },
  { event: 'Stop', command: 'murmur hook stop' },
];

export async function installClient(
  client: 'claude' | 'zcode' | 'hummin',
  opts?: { claudeDir?: string; zcodeDir?: string },
): Promise<string[]> {
  if (client === 'hummin') {
    printHumminWiring();
    return [];
  }
  if (client === 'zcode') {
    const zcodeDir = opts?.zcodeDir ?? join(homedir(), '.zcode');
    const skillPath = join(zcodeDir, 'skills', 'murmur', 'SKILL.md');
    writeTextFile(skillPath, ZCODE_SKILL_MD);
    return [skillPath];
  }
  const claudeDir = opts?.claudeDir ?? join(homedir(), '.claude');
  const commandPath = join(claudeDir, 'commands', 'murmur.md');
  writeTextFile(commandPath, CLAUDE_COMMANDS_MD);
  const claudeJsonPath = join(homedir(), '.claude.json');
  const claudeJson = readJsonObject(claudeJsonPath);
  mergeClaudeMcpServer(claudeJson);
  writeJsonFile(claudeJsonPath, claudeJson);
  const settingsPath = join(claudeDir, 'settings.json');
  const settings = readJsonObject(settingsPath);
  mergeClaudeHooks(settings);
  writeJsonFile(settingsPath, settings);
  return [commandPath, claudeJsonPath, settingsPath];
}

function printHumminWiring(): void {
  const lines = [
    'murmur install: hummin has no automatic wiring.',
    "Wire Renn's hummin CLI by hand:",
    '  1. run: murmur hook session-start   when a session starts',
    '  2. run: murmur hook prompt          on every user prompt',
    '  3. run: murmur hook stop            when a session stops',
    '  4. send with: murmur send <targets> <body>   or talk to the murmur MCP server (murmur mcp)',
  ];
  for (const line of lines) {
    console.log(line);
  }
}

// File helpers.

function writeTextFile(file: string, content: string): void {
  mkdirSync(dirname(file), { recursive: true });
  backupBeforeWrite(file);
  writeFileSync(file, content, 'utf8');
}

function writeJsonFile(file: string, value: unknown): void {
  writeTextFile(file, JSON.stringify(value, null, 2) + '\n');
}

function backupBeforeWrite(file: string): void {
  if (existsSync(file) && !existsSync(file + BACKUP_SUFFIX)) {
    copyFileSync(file, file + BACKUP_SUFFIX);
  }
}

// Reads a JSON object file; a missing file reads as empty. An unparsable or
// non-object file is an error so murmur can never destroy user settings.
function readJsonObject(file: string): Record<string, unknown> {
  if (!existsSync(file)) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(file, 'utf8'));
  } catch (err) {
    throw new Error('murmur install: ' + file + ' is not valid JSON, fix or remove it first: ' + (err as Error).message);
  }
  if (!isRecord(parsed)) {
    throw new Error('murmur install: ' + file + ' does not hold a JSON object');
  }
  return parsed;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function objectField(parent: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const value = parent[key];
  return isRecord(value) ? value : null;
}

function arrayField(parent: Record<string, unknown>, key: string): unknown[] | null {
  const value = parent[key];
  return Array.isArray(value) ? value : null;
}

// Merge helpers. Each takes the parsed document, adds only what is missing
// and leaves every other key untouched.

function mergeClaudeMcpServer(claudeJson: Record<string, unknown>): void {
  let mcpServers: Record<string, unknown>;
  const existing = objectField(claudeJson, 'mcpServers');
  if (existing !== null) {
    mcpServers = existing;
  } else if (claudeJson.mcpServers === undefined) {
    mcpServers = {};
  } else {
    throw new Error('murmur install: "mcpServers" in ~/.claude.json is not an object');
  }
  mcpServers.murmur = {
    command: 'murmur',
    args: ['mcp'],
    env: { MURMUR_HANDLE: 'CHANGE-ME-per-session' },
  };
  claudeJson.mcpServers = mcpServers;
}

function mergeClaudeHooks(settings: Record<string, unknown>): void {
  let hooks: Record<string, unknown>;
  const existing = objectField(settings, 'hooks');
  if (existing !== null) {
    hooks = existing;
  } else if (settings.hooks === undefined) {
    hooks = {};
  } else {
    throw new Error('murmur install: "hooks" in settings.json is not an object');
  }
  for (const { event, command } of CLAUDE_HOOKS) {
    const entries = arrayField(hooks, event) ?? [];
    const already = entries.some((entry) => entryHasCommand(entry, command));
    if (!already) {
      entries.push({ matcher: '*', hooks: [{ type: 'command', command }] });
    }
    hooks[event] = entries;
  }
  settings.hooks = hooks;
}

function entryHasCommand(entry: unknown, command: string): boolean {
  if (!isRecord(entry)) {
    return false;
  }
  const hooks = entry.hooks;
  if (!Array.isArray(hooks)) {
    return false;
  }
  return hooks.some((hook) => isRecord(hook) && hook.command === command);
}
