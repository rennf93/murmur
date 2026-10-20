#!/usr/bin/env node
// Live check, Claude Code: two interactive sessions in tmux with only the
// hooks, MCP entry and /murmur command that `murmur install claude` writes
// (user settings and other MCP servers are not loaded). The owner sends one
// message to both; each must reply through murmur_reply, each reply must fan
// out to the other session, and /murmur must store its text verbatim.
// Usage: node scripts/live/claude.mjs [model]   (default haiku)
import { mkdirSync, readFileSync, rmdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { ASK, REPO, log, rig, until, verdict, wait } from './lib.mjs';

const MODEL = process.argv[2] ?? 'haiku';
const r = await rig('claude');
const installed = await r.install('claude');
const settings = JSON.parse(readFileSync(join(installed, '.claude', 'settings.json'), 'utf8'));
const mcp = JSON.parse(readFileSync(join(installed, '.claude.json'), 'utf8')).mcpServers.murmur;
writeFileSync(join(r.root, 'settings.json'), JSON.stringify({ hooks: settings.hooks, permissions: { allow: ['mcp__murmur__murmur_reply'] } }));
writeFileSync(join(r.root, 'mcp.json'), JSON.stringify({ mcpServers: { murmur: mcp } }));
// The command goes in as a project command under its own name; only that file
// (and directories it leaves empty) is removed afterwards.
const commandDir = join(REPO, '.claude', 'commands');
const commandFile = join(commandDir, 'murmur-live.md');
mkdirSync(commandDir, { recursive: true });
writeFileSync(commandFile, readFileSync(join(installed, '.claude', 'commands', 'murmur.md'), 'utf8'));
const claude = ['claude', '--model', MODEL, '--setting-sources', 'project,local', '--settings', join(r.root, 'settings.json'),
  '--mcp-config', join(r.root, 'mcp.json'), '--strict-mcp-config'].join(' ');
const SLASH_TEXT = `slash check: it's $(echo PWNED) & \`literal\` "dq" !bang $HOME`;
try {
  r.tmux('mlA', REPO, claude);
  r.tmux('mlB', REPO, claude);
  const sessions = await until('two sessions announced', () => (r.session('claude').length >= 2 ? r.session('claude') : null), 60000);
  log('sessions', JSON.stringify(sessions));
  const native = r.db.prepare('SELECT owner FROM native_transports').all().map((row) => row.owner);
  await wait(6000);
  const handles = sessions.map((s) => s.handle);
  const { messageId, threadId } = r.send(handles.join(','), ASK);
  const final = await until('both replied', () => {
    const rows = r.deliveries(messageId);
    return rows.every((d) => d.state === 'replied') ? rows : null;
  }, 180000).catch(() => r.deliveries(messageId));
  log('ask deliveries', JSON.stringify(final));
  const replies = r.db.prepare('SELECT id, sender FROM messages WHERE thread_id = ? AND id != ?').all(threadId, messageId);
  const fanout = replies.map((m) => ({ from: m.sender, to: r.db.prepare('SELECT recipient FROM deliveries WHERE message_id = ?').all(m.id).map((d) => d.recipient).sort() }));
  log('reply fan-out', JSON.stringify(fanout));
  await r.type('mlA', '/murmur-live ' + handles[1] + ' ' + SLASH_TEXT);
  const slash = await until('slash message', () => r.db.prepare('SELECT sender, body FROM messages WHERE body LIKE ? ').get('slash check:%'), 40000).catch(() => null);
  log('slash row', JSON.stringify(slash));
  verdict([
    ['both sessions announced with a native transport', handles.every((h) => native.includes(h)), handles.join(', ')],
    ['both deliveries went sent, delivered (transcript proof), replied', final.every((d) => d.state === 'replied' && d.sent && d.delivered), JSON.stringify(final)],
    ['each reply fanned out to the other session and the owner', fanout.length === 2 && fanout.every((f) => f.to.length === 2 && f.to.includes('renn') && !f.to.includes(f.from)), JSON.stringify(fanout)],
    ['/murmur stored the text verbatim as the owner', slash?.body === SLASH_TEXT && slash?.sender === 'renn', JSON.stringify(slash)],
  ]);
} finally {
  r.cleanup();
  rmSync(commandFile, { force: true });
  for (const dir of [commandDir, join(REPO, '.claude')]) {
    try { rmdirSync(dir); } catch {}
  }
}
