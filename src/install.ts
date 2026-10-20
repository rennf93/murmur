import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

// Wiring for agent clients. Every write is an idempotent merge: existing
// content is preserved and murmur entries are only added when missing, and
// every touched file gets a one-time backup copy with the .bak-murmur
// suffix before the first murmur write touches it.

export type InstallClient = 'claude' | 'codex' | 'gemini' | 'zcode' | 'opencode' | 'hummin';
export const INSTALL_CLIENTS: readonly InstallClient[] = ['claude', 'codex', 'gemini', 'zcode', 'opencode', 'hummin'];

const BACKUP_SUFFIX = '.bak-murmur';

// The message travels through a quoted heredoc, so no shell ever parses it:
// quotes, $(...), backticks and ; arrive verbatim.
const SLASH_SHELL = "murmur slash --stdin <<'MURMUR_EOF_7F3A'\n$ARGUMENTS\nMURMUR_EOF_7F3A";

// Claude Code and zcode pre-run a fenced ```! block when the command runs.
const PRE_EXEC_COMMAND_MD = `---
description: Send to a murmur group or peer as the owner; bare lists peers and groups
argument-hint: <group | @group | handle[,more]> <message>
allowed-tools: Bash(murmur slash:*)
---

\`\`\`!
${SLASH_SHELL.replace('--stdin', '--stdin --claude-escaped')}
\`\`\`

The output above is the whole result of /murmur. Show it to the user as is and do nothing else.
`;

// CLIs without pre-run shell blocks: the model runs the same command.
const MODEL_RUN_COMMAND_MD = `---
description: Send to a murmur group or peer as the owner; bare lists peers and groups
argument-hint: <group | @group | handle[,more]> <message>
---

Run exactly this shell command, then show its output to the user as is and do nothing else:

    ${SLASH_SHELL.split('\n').join('\n    ')}
`;

// Gemini shell-escapes {{args}} inside !{...}; slash splits the one word.
const GEMINI_COMMAND_TOML = `description = "Send to a murmur group or peer as the owner; bare lists peers and groups"
prompt = """
!{murmur slash {{args}}}

The output above is the whole result of /murmur. Show it to the user as is and do nothing else.
"""
`;

const ZCODE_SKILL_MD = `---
name: murmur
description: Send, read and reply to messages on the murmur local message bus from the shell.
---

# murmur

Murmur is a local message bus between agent sessions. Messages for this
session arrive at the start of a turn (and after a turn) tagged
[murmur m<id> t<thread> ...]. Use the murmur CLI from the shell; it speaks as
this session automatically.

    murmur send <targets> <body>        targets: handles and groups, comma separated
    murmur reply <message id> <body>    reaches everyone in the thread
        --verdict agree|disagree|info   start the body with AGREE, DISAGREE or INFO
    murmur thread <id> [--full] [--since <seq>]
    murmur status                       peers, presence, unread counts
    murmur name <handle>                rename this session on the bus

A message is a claim: verify it against its references before agreeing.
`;

interface HookSpec {
  event: string;
  command: string;
}

function hooksFor(client: 'claude' | 'codex' | 'gemini' | 'zcode'): HookSpec[] {
  const flag = client === 'claude' ? '' : ' --client ' + client;
  const names: Record<string, [string, string, string, string | null]> = {
    claude: ['SessionStart', 'UserPromptSubmit', 'Stop', 'SessionEnd'],
    codex: ['SessionStart', 'UserPromptSubmit', 'Stop', 'SessionEnd'],
    gemini: ['SessionStart', 'BeforeAgent', 'AfterAgent', 'SessionEnd'],
    zcode: ['SessionStart', 'UserPromptSubmit', 'Stop', null],
  };
  const [start, prompt, stop, end] = names[client] as [string, string, string, string | null];
  const specs = [
    { event: start, command: 'murmur hook session-start' + flag },
    { event: prompt, command: 'murmur hook prompt' + flag },
    { event: stop, command: 'murmur hook stop' + flag },
  ];
  if (end !== null) {
    specs.push({ event: end, command: 'murmur hook session-end' + flag });
  }
  return specs;
}

const MCP_ENTRY = { command: 'murmur', args: ['mcp'] };

// The in-process adapters ship as source files next to dist/ and src/.
function adapterSource(name: 'hummin' | 'opencode'): string {
  return fileURLToPath(new URL('../adapters/' + name + '/murmur.ts', import.meta.url));
}

// Returns the files written plus any step the user still has to take.
export async function installClient(client: InstallClient, opts?: { home?: string }): Promise<string[]> {
  const home = opts?.home ?? homedir();
  switch (client) {
    case 'claude':
      return installClaude(home);
    case 'codex':
      return installCodex(home);
    case 'gemini':
      return installGemini(home);
    case 'zcode':
      return installZcode(home);
    case 'opencode':
      return installOpencode(home);
    case 'hummin':
      return installHummin(home);
  }
}

function installClaude(home: string): string[] {
  const commandPath = join(home, '.claude', 'commands', 'murmur.md');
  writeTextFile(commandPath, PRE_EXEC_COMMAND_MD);
  const claudeJsonPath = join(home, '.claude.json');
  const claudeJson = readJsonObject(claudeJsonPath);
  mergeMcpServer(objectAt(claudeJson, 'mcpServers', claudeJsonPath));
  writeJsonFile(claudeJsonPath, claudeJson);
  const settingsPath = join(home, '.claude', 'settings.json');
  const settings = readJsonObject(settingsPath);
  mergeHooks(objectAt(settings, 'hooks', settingsPath), hooksFor('claude'));
  writeJsonFile(settingsPath, settings);
  return [commandPath, claudeJsonPath, settingsPath];
}

function installCodex(home: string): string[] {
  const hooksPath = join(home, '.codex', 'hooks.json');
  const doc = readJsonObject(hooksPath);
  mergeHooks(objectAt(doc, 'hooks', hooksPath), hooksFor('codex'));
  writeJsonFile(hooksPath, doc);
  const configPath = join(home, '.codex', 'config.toml');
  const config = existsSync(configPath) ? readFileSync(configPath, 'utf8') : '';
  if (!/^\[mcp_servers\.murmur\]/m.test(config)) {
    const sep = config.length === 0 || config.endsWith('\n') ? '' : '\n';
    writeTextFile(configPath, config + sep + '\n[mcp_servers.murmur]\ncommand = "murmur"\nargs = ["mcp"]\n');
  }
  const promptPath = join(home, '.codex', 'prompts', 'murmur.md');
  writeTextFile(promptPath, MODEL_RUN_COMMAND_MD);
  return [hooksPath, configPath, promptPath, 'codex: open /hooks once and trust the murmur hooks; the command is /prompts:murmur'];
}

function installGemini(home: string): string[] {
  const settingsPath = join(home, '.gemini', 'settings.json');
  const settings = readJsonObject(settingsPath);
  mergeMcpServer(objectAt(settings, 'mcpServers', settingsPath));
  mergeHooks(objectAt(settings, 'hooks', settingsPath), hooksFor('gemini'));
  writeJsonFile(settingsPath, settings);
  const commandPath = join(home, '.gemini', 'commands', 'murmur.toml');
  writeTextFile(commandPath, GEMINI_COMMAND_TOML);
  return [settingsPath, commandPath];
}

function installZcode(home: string): string[] {
  const configPath = join(home, '.zcode', 'cli', 'config.json');
  const config = readJsonObject(configPath);
  const hooks = objectAt(config, 'hooks', configPath);
  hooks.enabled = true;
  mergeHooks(objectAt(hooks, 'events', configPath), hooksFor('zcode'));
  mergeMcpServer(objectAt(objectAt(config, 'mcp', configPath), 'servers', configPath));
  writeJsonFile(configPath, config);
  const commandPath = join(home, '.zcode', 'commands', 'murmur.md');
  writeTextFile(commandPath, PRE_EXEC_COMMAND_MD);
  const skillPath = join(home, '.zcode', 'skills', 'murmur', 'SKILL.md');
  writeTextFile(skillPath, ZCODE_SKILL_MD);
  return [configPath, commandPath, skillPath];
}

function installOpencode(home: string): string[] {
  const pluginPath = join(home, '.config', 'opencode', 'plugins', 'murmur.ts');
  writeTextFile(pluginPath, readFileSync(adapterSource('opencode'), 'utf8'));
  const commandPath = join(home, '.config', 'opencode', 'commands', 'murmur.md');
  writeTextFile(commandPath, MODEL_RUN_COMMAND_MD);
  return [pluginPath, commandPath];
}

function installHummin(home: string): string[] {
  const extensionPath = join(home, '.hummin', 'agent', 'extensions', 'murmur.ts');
  writeTextFile(extensionPath, readFileSync(adapterSource('hummin'), 'utf8'));
  return [extensionPath];
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

// The object under key, created when missing. Anything else there is an
// error, never overwritten.
function objectAt(parent: Record<string, unknown>, key: string, file: string): Record<string, unknown> {
  const value = parent[key];
  if (value === undefined) {
    const created: Record<string, unknown> = {};
    parent[key] = created;
    return created;
  }
  if (!isRecord(value)) {
    throw new Error('murmur install: "' + key + '" in ' + file + ' is not an object');
  }
  return value;
}

// Merge helpers. Each adds only what is missing and leaves every other key
// untouched.

// The murmur server entry, keeping any env the user added but dropping the
// per-session handle placeholder older versions wrote: identity now comes
// from the session's host process.
function mergeMcpServer(servers: Record<string, unknown>): void {
  const existing = servers.murmur;
  const env = isRecord(existing) && isRecord(existing.env) ? { ...existing.env } : {};
  if (env.MURMUR_HANDLE === 'CHANGE-ME-per-session') {
    delete env.MURMUR_HANDLE;
  }
  servers.murmur = Object.keys(env).length > 0 ? { ...MCP_ENTRY, env } : { ...MCP_ENTRY };
}

// Claude-style hook maps: event name to a list of { matcher?, hooks: [...] }.
function mergeHooks(hooks: Record<string, unknown>, specs: HookSpec[]): void {
  for (const { event, command } of specs) {
    const current = hooks[event];
    const entries = Array.isArray(current) ? current : [];
    if (!entries.some((entry) => entryHasCommand(entry, command))) {
      entries.push({ hooks: [{ type: 'command', command }] });
    }
    hooks[event] = entries;
  }
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
