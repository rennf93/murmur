import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { INSTALL_CLIENTS, installClient } from '../src/install.js';
import type { InstallClient } from '../src/install.js';

// installClient takes an explicit home, so every test writes into its own
// temp directory. Only the default-home test goes through os.homedir, which
// is stubbed to a temp directory for this file.

const osState = vi.hoisted(() => ({ home: '' }));

vi.mock('node:os', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:os')>();
  const fs = await import('node:fs');
  const path = await import('node:path');
  osState.home = fs.mkdtempSync(path.join(actual.tmpdir(), 'murmur-install-default-'));
  return { ...actual, homedir: () => osState.home };
});

const REPO = join(dirname(fileURLToPath(import.meta.url)), '..');

const SLASH_SHELL = "murmur slash --stdin <<'MURMUR_EOF_7F3A'\n$ARGUMENTS\nMURMUR_EOF_7F3A";
const PRE_EXEC_COMMAND_MD = [
  '---',
  'description: Send to a murmur group or peer as the owner; bare lists peers and groups',
  'argument-hint: <group | @group | handle[,more]> <message>',
  'allowed-tools: Bash(murmur slash:*)',
  '---',
  '',
  '```!',
  SLASH_SHELL.replace('--stdin', '--stdin --claude-escaped'),
  '```',
  '',
  'The output above is the whole result of /murmur. Show it to the user as is and do nothing else.',
  '',
].join('\n');
const MODEL_RUN_COMMAND_MD = [
  '---',
  'description: Send to a murmur group or peer as the owner; bare lists peers and groups',
  'argument-hint: <group | @group | handle[,more]> <message>',
  '---',
  '',
  'Run exactly this shell command, then show its output to the user as is and do nothing else:',
  '',
  '    ' + SLASH_SHELL.split('\n').join('\n    '),
  '',
].join('\n');
const GEMINI_COMMAND_TOML = [
  'description = "Send to a murmur group or peer as the owner; bare lists peers and groups"',
  'prompt = """',
  '!{murmur slash {{args}}}',
  '',
  'The output above is the whole result of /murmur. Show it to the user as is and do nothing else.',
  '"""',
  '',
].join('\n');

const MCP = { command: 'murmur', args: ['mcp'] };

interface HookCfg {
  event: string;
  command: string;
}

const hooksOf = (client: 'claude' | 'codex' | 'gemini' | 'zcode'): HookCfg[] => {
  const flag = client === 'claude' ? '' : ' --client ' + client;
  const names =
    client === 'gemini'
      ? ['SessionStart', 'BeforeAgent', 'AfterAgent', 'SessionEnd']
      : ['SessionStart', 'UserPromptSubmit', 'Stop', 'SessionEnd'];
  const cmds = ['session-start', 'prompt', 'stop', 'session-end'];
  return names
    .map((event, i) => ({ event, command: 'murmur hook ' + (cmds[i] as string) + flag }))
    .filter((h) => !(client === 'zcode' && h.event === 'SessionEnd'));
};

const hookMap = (specs: HookCfg[]): Record<string, unknown[]> =>
  Object.fromEntries(specs.map((h) => [h.event, [{ hooks: [{ type: 'command', command: h.command }] }]]));

const homes: string[] = [];
let home = '';

const readJson = (file: string): Record<string, unknown> => JSON.parse(readFileSync(file, 'utf8')) as Record<string, unknown>;
const readText = (file: string): string => readFileSync(file, 'utf8');
const put = (file: string, content: string): void => {
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, content, 'utf8');
};

beforeEach(() => {
  home = mkdtempSync(join(tmpdir(), 'mm-install-'));
  homes.push(home);
});

afterEach(() => {
  for (const dir of homes.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

afterAll(() => {
  rmSync(osState.home, { recursive: true, force: true });
});

describe('client list', () => {
  it('names all six clients', () => {
    expect([...INSTALL_CLIENTS]).toEqual(['claude', 'codex', 'gemini', 'zcode', 'opencode', 'hummin']);
  });

  it('defaults to os.homedir when no home is given', async () => {
    const files = await installClient('hummin');
    expect(files).toEqual([join(osState.home, '.hummin', 'agent', 'extensions', 'murmur.ts')]);
    expect(existsSync(files[0] as string)).toBe(true);
  });
});

describe('claude', () => {
  it('writes the command, the MCP server and the four hooks into a fresh home', async () => {
    const files = await installClient('claude', { home });
    const commandPath = join(home, '.claude', 'commands', 'murmur.md');
    const jsonPath = join(home, '.claude.json');
    const settingsPath = join(home, '.claude', 'settings.json');
    expect(files).toEqual([commandPath, jsonPath, settingsPath]);
    expect(readText(commandPath)).toBe(PRE_EXEC_COMMAND_MD);
    expect(readJson(jsonPath)).toEqual({ mcpServers: { murmur: MCP } });
    expect(readJson(settingsPath)).toEqual({ hooks: hookMap(hooksOf('claude')) });
    expect(hooksOf('claude').map((h) => h.command)).toEqual([
      'murmur hook session-start',
      'murmur hook prompt',
      'murmur hook stop',
      'murmur hook session-end',
    ]);
    // Nothing existed, so nothing was backed up.
    expect(existsSync(commandPath + '.bak-murmur')).toBe(false);
    expect(existsSync(jsonPath + '.bak-murmur')).toBe(false);
  });

  it('merges into existing files, keeping every other key and backing each up once', async () => {
    const jsonPath = join(home, '.claude.json');
    const settingsPath = join(home, '.claude', 'settings.json');
    const commandPath = join(home, '.claude', 'commands', 'murmur.md');
    const userJson = JSON.stringify({ theme: 'dark', mcpServers: { other: { command: 'other' } } });
    const userSettings = JSON.stringify({
      model: 'opus',
      hooks: { Stop: [{ matcher: 'x', hooks: [{ type: 'command', command: 'echo user' }] }], Notification: [{ hooks: [] }] },
    });
    put(jsonPath, userJson);
    put(settingsPath, userSettings);
    put(commandPath, 'my own command');
    await installClient('claude', { home });
    const json = readJson(jsonPath);
    expect(json.theme).toBe('dark');
    expect(json.mcpServers).toEqual({ other: { command: 'other' }, murmur: MCP });
    const settings = readJson(settingsPath) as { model: string; hooks: Record<string, unknown[]> };
    expect(settings.model).toBe('opus');
    expect(settings.hooks.Notification).toEqual([{ hooks: [] }]);
    expect(settings.hooks.Stop).toEqual([
      { matcher: 'x', hooks: [{ type: 'command', command: 'echo user' }] },
      { hooks: [{ type: 'command', command: 'murmur hook stop' }] },
    ]);
    expect(readText(jsonPath + '.bak-murmur')).toBe(userJson);
    expect(readText(settingsPath + '.bak-murmur')).toBe(userSettings);
    expect(readText(commandPath + '.bak-murmur')).toBe('my own command');
    expect(readText(commandPath)).toBe(PRE_EXEC_COMMAND_MD);
  });

  it('is idempotent: no duplicate hooks and the first backup stands', async () => {
    const settingsPath = join(home, '.claude', 'settings.json');
    put(settingsPath, '{"keep":1}');
    await installClient('claude', { home });
    const first = readText(settingsPath);
    await installClient('claude', { home });
    expect(readText(settingsPath)).toBe(first);
    const hooks = (readJson(settingsPath) as { hooks: Record<string, unknown[]> }).hooks;
    for (const entries of Object.values(hooks)) {
      expect(entries).toHaveLength(1);
    }
    expect(readText(settingsPath + '.bak-murmur')).toBe('{"keep":1}');
  });

  it('drops the CHANGE-ME placeholder env and keeps the user env keys', async () => {
    const jsonPath = join(home, '.claude.json');
    put(
      jsonPath,
      JSON.stringify({ mcpServers: { murmur: { command: 'old', args: [], env: { MURMUR_HANDLE: 'CHANGE-ME-per-session', MURMUR_HOME: '/mine' } } } }),
    );
    await installClient('claude', { home });
    expect(readJson(jsonPath)).toEqual({ mcpServers: { murmur: { ...MCP, env: { MURMUR_HOME: '/mine' } } } });
    // A real handle the user chose is kept; a placeholder-only env disappears.
    put(jsonPath, JSON.stringify({ mcpServers: { murmur: { env: { MURMUR_HANDLE: 'mine' } } } }));
    await installClient('claude', { home });
    expect(readJson(jsonPath)).toEqual({ mcpServers: { murmur: { ...MCP, env: { MURMUR_HANDLE: 'mine' } } } });
    put(jsonPath, JSON.stringify({ mcpServers: { murmur: { env: { MURMUR_HANDLE: 'CHANGE-ME-per-session' } } } }));
    await installClient('claude', { home });
    expect(readJson(jsonPath)).toEqual({ mcpServers: { murmur: MCP } });
    // A murmur entry whose env is not an object is replaced by the plain entry.
    put(jsonPath, JSON.stringify({ mcpServers: { murmur: { env: 'junk' } } }));
    await installClient('claude', { home });
    expect(readJson(jsonPath)).toEqual({ mcpServers: { murmur: MCP } });
  });

  it('tolerates hook values that are not lists or entries without hooks', async () => {
    const settingsPath = join(home, '.claude', 'settings.json');
    put(settingsPath, JSON.stringify({ hooks: { Stop: 'nope', SessionStart: ['text', { hooks: 'bad' }, { hooks: [null, 3] }] } }));
    await installClient('claude', { home });
    const hooks = (readJson(settingsPath) as { hooks: Record<string, unknown[]> }).hooks;
    expect(hooks.Stop).toEqual([{ hooks: [{ type: 'command', command: 'murmur hook stop' }] }]);
    expect(hooks.SessionStart).toHaveLength(4);
    expect(hooks.SessionStart?.[3]).toEqual({ hooks: [{ type: 'command', command: 'murmur hook session-start' }] });
  });

  it('refuses invalid JSON and non-object containers without touching the file', async () => {
    const jsonPath = join(home, '.claude.json');
    put(jsonPath, '{ nope');
    await expect(installClient('claude', { home })).rejects.toThrow('murmur install: ' + jsonPath + ' is not valid JSON, fix or remove it first');
    expect(readText(jsonPath)).toBe('{ nope');
    put(jsonPath, '[1,2]');
    await expect(installClient('claude', { home })).rejects.toThrow('murmur install: ' + jsonPath + ' does not hold a JSON object');
    put(jsonPath, '{"mcpServers":[]}');
    await expect(installClient('claude', { home })).rejects.toThrow('murmur install: "mcpServers" in ' + jsonPath + ' is not an object');
    put(jsonPath, '{}');
    const settingsPath = join(home, '.claude', 'settings.json');
    put(settingsPath, '{"hooks":"x"}');
    await expect(installClient('claude', { home })).rejects.toThrow('murmur install: "hooks" in ' + settingsPath + ' is not an object');
    expect(readText(settingsPath)).toBe('{"hooks":"x"}');
  });
});

describe('codex', () => {
  it('writes hooks.json, config.toml and the prompt, and tells the user to trust the hooks', async () => {
    const files = await installClient('codex', { home });
    const hooksPath = join(home, '.codex', 'hooks.json');
    const configPath = join(home, '.codex', 'config.toml');
    const promptPath = join(home, '.codex', 'prompts', 'murmur.md');
    expect(files).toEqual([hooksPath, configPath, promptPath, 'codex: open /hooks once and trust the murmur hooks; the command is /prompts:murmur']);
    expect(readJson(hooksPath)).toEqual({ hooks: hookMap(hooksOf('codex')) });
    expect(hooksOf('codex')[0]?.command).toBe('murmur hook session-start --client codex');
    expect(readText(configPath)).toBe('\n[mcp_servers.murmur]\ncommand = "murmur"\nargs = ["mcp"]\n');
    expect(readText(promptPath)).toBe(MODEL_RUN_COMMAND_MD);
  });

  it('appends to an existing config.toml once, with a newline when it lacks one', async () => {
    const configPath = join(home, '.codex', 'config.toml');
    put(configPath, 'model = "x"');
    await installClient('codex', { home });
    expect(readText(configPath)).toBe('model = "x"\n\n[mcp_servers.murmur]\ncommand = "murmur"\nargs = ["mcp"]\n');
    expect(readText(configPath + '.bak-murmur')).toBe('model = "x"');
    const after = readText(configPath);
    await installClient('codex', { home });
    expect(readText(configPath)).toBe(after);
    put(configPath, 'model = "y"\n');
    rmSync(configPath + '.bak-murmur');
    await installClient('codex', { home });
    expect(readText(configPath)).toBe('model = "y"\n\n[mcp_servers.murmur]\ncommand = "murmur"\nargs = ["mcp"]\n');
  });

  it('leaves a config.toml that already has the murmur table alone and does not duplicate hooks', async () => {
    const configPath = join(home, '.codex', 'config.toml');
    put(configPath, '[mcp_servers.murmur]\ncommand = "custom"\n');
    await installClient('codex', { home });
    await installClient('codex', { home });
    expect(readText(configPath)).toBe('[mcp_servers.murmur]\ncommand = "custom"\n');
    const hooks = (readJson(join(home, '.codex', 'hooks.json')) as { hooks: Record<string, unknown[]> }).hooks;
    expect(Object.values(hooks).every((entries) => entries.length === 1)).toBe(true);
  });
});

describe('gemini', () => {
  it('writes the MCP server and the Gemini event names into settings.json, plus the toml command', async () => {
    const files = await installClient('gemini', { home });
    const settingsPath = join(home, '.gemini', 'settings.json');
    const commandPath = join(home, '.gemini', 'commands', 'murmur.toml');
    expect(files).toEqual([settingsPath, commandPath]);
    expect(readJson(settingsPath)).toEqual({ mcpServers: { murmur: MCP }, hooks: hookMap(hooksOf('gemini')) });
    expect(Object.keys(readJson(settingsPath).hooks as object)).toEqual(['SessionStart', 'BeforeAgent', 'AfterAgent', 'SessionEnd']);
    expect(hooksOf('gemini')[1]?.command).toBe('murmur hook prompt --client gemini');
    expect(readText(commandPath)).toBe(GEMINI_COMMAND_TOML);
  });

  it('is idempotent and backs up once', async () => {
    const settingsPath = join(home, '.gemini', 'settings.json');
    put(settingsPath, '{"general":{"x":1}}');
    await installClient('gemini', { home });
    const first = readText(settingsPath);
    await installClient('gemini', { home });
    expect(readText(settingsPath)).toBe(first);
    expect(readText(settingsPath + '.bak-murmur')).toBe('{"general":{"x":1}}');
    expect(readJson(settingsPath).general).toEqual({ x: 1 });
  });
});

describe('zcode', () => {
  it('enables hooks, writes the three events and the MCP server, the command and the skill', async () => {
    const files = await installClient('zcode', { home });
    const configPath = join(home, '.zcode', 'cli', 'config.json');
    const commandPath = join(home, '.zcode', 'commands', 'murmur.md');
    const skillPath = join(home, '.zcode', 'skills', 'murmur', 'SKILL.md');
    expect(files).toEqual([configPath, commandPath, skillPath]);
    expect(readJson(configPath)).toEqual({
      hooks: { enabled: true, events: hookMap(hooksOf('zcode')) },
      mcp: { servers: { murmur: MCP } },
    });
    expect(Object.keys((readJson(configPath).hooks as { events: object }).events)).toEqual(['SessionStart', 'UserPromptSubmit', 'Stop']);
    expect(readText(commandPath)).toBe(PRE_EXEC_COMMAND_MD);
    const skill = readText(skillPath);
    expect(skill.startsWith('---\nname: murmur\n')).toBe(true);
    expect(skill).toContain('murmur reply <message id> <body>');
    expect(skill).toContain('murmur name <handle>');
  });

  it('keeps existing config keys and refuses a malformed hooks or mcp container', async () => {
    const configPath = join(home, '.zcode', 'cli', 'config.json');
    put(configPath, JSON.stringify({ theme: 'x', hooks: { enabled: false, events: { Stop: [] } }, mcp: { servers: { a: {} } } }));
    await installClient('zcode', { home });
    const config = readJson(configPath) as { theme: string; hooks: { enabled: boolean; events: Record<string, unknown[]> }; mcp: { servers: object } };
    expect(config.theme).toBe('x');
    expect(config.hooks.enabled).toBe(true);
    expect(config.hooks.events.Stop).toHaveLength(1);
    expect(config.mcp.servers).toEqual({ a: {}, murmur: MCP });
    put(configPath, '{"mcp":{"servers":"x"}}');
    await expect(installClient('zcode', { home })).rejects.toThrow('"servers" in ' + configPath + ' is not an object');
  });
});

describe('adapters', () => {
  it.each([
    ['opencode', join('.config', 'opencode', 'plugins', 'murmur.ts')],
    ['hummin', join('.hummin', 'agent', 'extensions', 'murmur.ts')],
  ] as Array<[InstallClient, string]>)('%s copies its adapter verbatim', async (client, rel) => {
    const files = await installClient(client, { home });
    const target = join(home, rel);
    expect(files[0]).toBe(target);
    expect(readText(target)).toBe(readText(join(REPO, 'adapters', client, 'murmur.ts')));
  });

  it('opencode also writes the model-run command; reruns refresh and back up the old file once', async () => {
    const files = await installClient('opencode', { home });
    const pluginPath = join(home, '.config', 'opencode', 'plugins', 'murmur.ts');
    const commandPath = join(home, '.config', 'opencode', 'commands', 'murmur.md');
    expect(files).toEqual([pluginPath, commandPath]);
    expect(readText(commandPath)).toBe(MODEL_RUN_COMMAND_MD);
    writeFileSync(pluginPath, 'stale plugin', 'utf8');
    await installClient('opencode', { home });
    expect(readText(pluginPath)).toBe(readText(join(REPO, 'adapters', 'opencode', 'murmur.ts')));
    expect(readText(pluginPath + '.bak-murmur')).toBe('stale plugin');
    writeFileSync(pluginPath, 'stale again', 'utf8');
    await installClient('opencode', { home });
    expect(readText(pluginPath + '.bak-murmur')).toBe('stale plugin');
  });
});
