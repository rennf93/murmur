import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { installClient } from '../src/install.js';

// installClient writes ~/.claude.json through os.homedir, which is not
// injectable, so homedir is stubbed to a temp directory for this file. Every
// test starts from an empty stubbed home.

const osState = vi.hoisted(() => ({ home: '' }));

vi.mock('node:os', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:os')>();
  const fs = await import('node:fs');
  const path = await import('node:path');
  osState.home = fs.mkdtempSync(path.join(actual.tmpdir(), 'murmur-install-home-'));
  return { ...actual, homedir: () => osState.home };
});

const MURMUR_SERVER = { command: 'murmur', args: ['mcp'], env: { MURMUR_HANDLE: 'CHANGE-ME-per-session' } };
const HOOK_EVENTS: Array<[string, string]> = [
  ['SessionStart', 'murmur hook session-start'],
  ['UserPromptSubmit', 'murmur hook prompt'],
  ['Stop', 'murmur hook stop'],
];

interface HookEntry {
  matcher?: string;
  hooks?: Array<{ type?: string; command?: string }>;
}

function readJson(file: string): Record<string, unknown> {
  return JSON.parse(readFileSync(file, 'utf8')) as Record<string, unknown>;
}

function hooksOf(settings: Record<string, unknown>, event: string): unknown[] {
  const hooks = settings.hooks as Record<string, unknown>;
  return hooks[event] as unknown[];
}

function murmurEntries(entries: unknown[], command: string): HookEntry[] {
  return entries.filter(
    (entry): entry is HookEntry =>
      typeof entry === 'object' &&
      entry !== null &&
      Array.isArray((entry as HookEntry).hooks) &&
      ((entry as HookEntry).hooks ?? []).some((h) => h.command === command),
  );
}

let claudeDir = '';
let claudeJsonPath = '';
let settingsPath = '';
let commandPath = '';

beforeEach(() => {
  rmSync(osState.home, { recursive: true, force: true });
  mkdirSync(osState.home, { recursive: true });
  claudeDir = join(osState.home, 'claude-config');
  claudeJsonPath = join(osState.home, '.claude.json');
  settingsPath = join(claudeDir, 'settings.json');
  commandPath = join(claudeDir, 'commands', 'murmur.md');
});

afterAll(() => {
  rmSync(osState.home, { recursive: true, force: true });
});

describe('installClient claude', () => {
  const existingSettings = {
    model: 'opus',
    permissions: { allow: ['Bash(ls:*)'] },
    hooks: {
      PreToolUse: [{ matcher: 'Bash', hooks: [{ type: 'command', command: 'echo pre' }] }],
      SessionStart: [
        'garbage-string-entry',
        { hooks: 'not-an-array' },
        { hooks: [5] },
        { matcher: '*', hooks: [{ type: 'command', command: 'echo other-session-start' }] },
      ],
      Stop: 'not-an-array-at-all',
    },
  };
  const existingClaudeJson = {
    numStartups: 7,
    mcpServers: {
      other: { command: 'other-server', args: ['--flag'] },
    },
  };

  it('merges into existing settings and ~/.claude.json, preserving everything and writing backups', async () => {
    mkdirSync(claudeDir, { recursive: true });
    const settingsText = JSON.stringify(existingSettings, null, 2) + '\n';
    const claudeText = JSON.stringify(existingClaudeJson);
    writeFileSync(settingsPath, settingsText, 'utf8');
    writeFileSync(claudeJsonPath, claudeText, 'utf8');

    const files = await installClient('claude', { claudeDir });
    expect(files).toEqual([commandPath, claudeJsonPath, settingsPath]);

    // Command file.
    const command = readFileSync(commandPath, 'utf8');
    expect(command).toContain('description: Send a message on the murmur bus');
    expect(command).toContain('!murmur send $ARGUMENTS');
    expect(existsSync(commandPath + '.bak-murmur')).toBe(false);

    // ~/.claude.json: other server and unrelated keys preserved, murmur added.
    const claudeJson = readJson(claudeJsonPath);
    expect(claudeJson.numStartups).toBe(7);
    const servers = claudeJson.mcpServers as Record<string, unknown>;
    expect(servers.other).toEqual({ command: 'other-server', args: ['--flag'] });
    expect(servers.murmur).toEqual(MURMUR_SERVER);
    expect(Object.keys(servers).sort()).toEqual(['murmur', 'other']);
    expect(readFileSync(claudeJsonPath + '.bak-murmur', 'utf8')).toBe(claudeText);

    // settings.json: unrelated keys and foreign hooks preserved, one murmur
    // entry per event, a non-array event value replaced by an array.
    const settings = readJson(settingsPath);
    expect(settings.model).toBe('opus');
    expect(settings.permissions).toEqual({ allow: ['Bash(ls:*)'] });
    expect(hooksOf(settings, 'PreToolUse')).toEqual(existingSettings.hooks.PreToolUse);
    const sessionStart = hooksOf(settings, 'SessionStart');
    expect(sessionStart.slice(0, 4)).toEqual(existingSettings.hooks.SessionStart);
    expect(sessionStart).toHaveLength(5);
    for (const [event, cmd] of HOOK_EVENTS) {
      const entries = hooksOf(settings, event);
      expect(Array.isArray(entries)).toBe(true);
      const mine = murmurEntries(entries, cmd);
      expect(mine).toHaveLength(1);
      expect(mine[0]).toEqual({ matcher: '*', hooks: [{ type: 'command', command: cmd }] });
    }
    expect(hooksOf(settings, 'Stop')).toEqual([{ matcher: '*', hooks: [{ type: 'command', command: 'murmur hook stop' }] }]);
    expect(readFileSync(settingsPath + '.bak-murmur', 'utf8')).toBe(settingsText);
  });

  it('is idempotent on rerun: no duplicate entries and the first backups stand', async () => {
    mkdirSync(claudeDir, { recursive: true });
    const settingsText = JSON.stringify(existingSettings);
    const claudeText = JSON.stringify(existingClaudeJson);
    writeFileSync(settingsPath, settingsText, 'utf8');
    writeFileSync(claudeJsonPath, claudeText, 'utf8');
    await installClient('claude', { claudeDir });
    const afterFirstSettings = readFileSync(settingsPath, 'utf8');
    const afterFirstClaude = readFileSync(claudeJsonPath, 'utf8');
    const afterFirstCommand = readFileSync(commandPath, 'utf8');

    const files = await installClient('claude', { claudeDir });
    expect(files).toEqual([commandPath, claudeJsonPath, settingsPath]);
    expect(readFileSync(settingsPath, 'utf8')).toBe(afterFirstSettings);
    expect(readFileSync(claudeJsonPath, 'utf8')).toBe(afterFirstClaude);
    expect(readFileSync(commandPath, 'utf8')).toBe(afterFirstCommand);
    const settings = readJson(settingsPath);
    for (const [event, cmd] of HOOK_EVENTS) {
      expect(murmurEntries(hooksOf(settings, event), cmd)).toHaveLength(1);
    }
    const servers = (readJson(claudeJsonPath).mcpServers as Record<string, unknown>) ?? {};
    expect(Object.keys(servers).sort()).toEqual(['murmur', 'other']);
    // Backups hold the pre-murmur originals, not the first install output.
    expect(readFileSync(settingsPath + '.bak-murmur', 'utf8')).toBe(settingsText);
    expect(readFileSync(claudeJsonPath + '.bak-murmur', 'utf8')).toBe(claudeText);
    // The command file now has a backup too (it existed before the rerun).
    expect(readFileSync(commandPath + '.bak-murmur', 'utf8')).toBe(afterFirstCommand);
  });

  it('creates every file from scratch in a fresh home and defaults claudeDir to ~/.claude', async () => {
    const files = await installClient('claude');
    const defaultDir = join(osState.home, '.claude');
    expect(files).toEqual([
      join(defaultDir, 'commands', 'murmur.md'),
      claudeJsonPath,
      join(defaultDir, 'settings.json'),
    ]);
    for (const file of files) {
      expect(existsSync(file)).toBe(true);
      expect(existsSync(file + '.bak-murmur')).toBe(false);
    }
    const claudeJson = readJson(claudeJsonPath);
    expect(claudeJson).toEqual({ mcpServers: { murmur: MURMUR_SERVER } });
    const settings = readJson(join(defaultDir, 'settings.json'));
    expect(Object.keys(settings)).toEqual(['hooks']);
    for (const [event, cmd] of HOOK_EVENTS) {
      expect(hooksOf(settings, event)).toEqual([{ matcher: '*', hooks: [{ type: 'command', command: cmd }] }]);
    }
    expect(readFileSync(join(defaultDir, 'settings.json'), 'utf8').endsWith('\n')).toBe(true);
  });

  it('refuses to touch a ~/.claude.json that is not valid JSON or not an object', async () => {
    writeFileSync(claudeJsonPath, '{ definitely not json', 'utf8');
    await expect(installClient('claude', { claudeDir })).rejects.toThrow(
      'murmur install: ' + claudeJsonPath + ' is not valid JSON, fix or remove it first',
    );
    expect(readFileSync(claudeJsonPath, 'utf8')).toBe('{ definitely not json');
    writeFileSync(claudeJsonPath, '[1, 2, 3]', 'utf8');
    await expect(installClient('claude', { claudeDir })).rejects.toThrow(
      'murmur install: ' + claudeJsonPath + ' does not hold a JSON object',
    );
    expect(readFileSync(claudeJsonPath, 'utf8')).toBe('[1, 2, 3]');
  });

  it('refuses a non-object mcpServers or hooks value', async () => {
    writeFileSync(claudeJsonPath, JSON.stringify({ mcpServers: 'nope' }), 'utf8');
    await expect(installClient('claude', { claudeDir })).rejects.toThrow(
      'murmur install: "mcpServers" in ~/.claude.json is not an object',
    );
    expect(readJson(claudeJsonPath)).toEqual({ mcpServers: 'nope' });
    writeFileSync(claudeJsonPath, '{}', 'utf8');
    mkdirSync(claudeDir, { recursive: true });
    writeFileSync(settingsPath, JSON.stringify({ hooks: 42 }), 'utf8');
    await expect(installClient('claude', { claudeDir })).rejects.toThrow(
      'murmur install: "hooks" in settings.json is not an object',
    );
    expect(readJson(settingsPath)).toEqual({ hooks: 42 });
    // ~/.claude.json was already merged before settings.json failed.
    expect((readJson(claudeJsonPath).mcpServers as Record<string, unknown>).murmur).toEqual(MURMUR_SERVER);
  });

  it('refuses an unparsable settings.json', async () => {
    mkdirSync(claudeDir, { recursive: true });
    writeFileSync(settingsPath, 'nope', 'utf8');
    await expect(installClient('claude', { claudeDir })).rejects.toThrow(
      'murmur install: ' + settingsPath + ' is not valid JSON',
    );
  });
});

describe('installClient zcode', () => {
  it('writes the skill file into the given zcodeDir and backs it up on rerun', async () => {
    const zcodeDir = join(osState.home, 'zcode-config');
    const skillPath = join(zcodeDir, 'skills', 'murmur', 'SKILL.md');
    const files = await installClient('zcode', { zcodeDir });
    expect(files).toEqual([skillPath]);
    const skill = readFileSync(skillPath, 'utf8');
    expect(skill.startsWith('---\nname: murmur\n')).toBe(true);
    expect(skill).toContain('murmur send <targets> <body>');
    expect(skill).toContain('murmur thread <id> --since <seq>');
    expect(existsSync(skillPath + '.bak-murmur')).toBe(false);
    writeFileSync(skillPath, 'user edits\n', 'utf8');
    const again = await installClient('zcode', { zcodeDir });
    expect(again).toEqual([skillPath]);
    expect(readFileSync(skillPath, 'utf8')).toBe(skill);
    expect(readFileSync(skillPath + '.bak-murmur', 'utf8')).toBe('user edits\n');
  });

  it('defaults zcodeDir to ~/.zcode', async () => {
    const files = await installClient('zcode');
    expect(files).toEqual([join(osState.home, '.zcode', 'skills', 'murmur', 'SKILL.md')]);
    expect(existsSync(files[0] ?? '')).toBe(true);
  });
});

describe('installClient hummin', () => {
  it('returns no files and prints the manual wiring', async () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      const files = await installClient('hummin');
      expect(files).toEqual([]);
      const lines = logSpy.mock.calls.map((call) => String(call[0]));
      expect(lines).toHaveLength(6);
      expect(lines[0]).toBe('murmur install: hummin has no automatic wiring.');
      expect(lines[2]).toContain('murmur hook session-start');
      expect(lines[3]).toContain('murmur hook prompt');
      expect(lines[4]).toContain('murmur hook stop');
      expect(lines[5]).toContain('murmur send <targets> <body>');
    } finally {
      logSpy.mockRestore();
    }
    expect(existsSync(claudeJsonPath)).toBe(false);
  });
});
