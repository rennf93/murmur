#!/usr/bin/env node
// Live check, Claude Code: two interactive sessions in tmux with only the
// hooks, MCP entry and /murmur command that `murmur install claude` writes
// (user settings and other MCP servers are not loaded). The owner sends one
// message to both; each must reply through murmur_reply, each reply must reach
// only the owner who asked (not the other peer), and /murmur must store its text verbatim. Before
// that, /rename in each session must become its handle (live-peer-a and
// live-peer-b), and the /murmur message typed in mlA must carry via = mlA's
// handle, with B's reply delivered to mlA as well as to the owner.
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
const SLASH_TEXT = `slash check: it's $(echo PWNED) & \`literal\` "dq" !bang $HOME. Call the murmur_reply tool once with messageId set to the number after "m" in the tag above, body "INFO received", verdict "info". Then stop.`;
try {
  r.tmux('mlA', REPO, claude);
  r.tmux('mlB', REPO, claude);
  const sessions = await until('two sessions announced', () => (r.session('claude').length >= 2 ? r.session('claude') : null), 60000);
  log('sessions', JSON.stringify(sessions));
  await wait(6000);
  // The user names each session; its next hook (a prompt) must pick the name up.
  await r.type('mlA', '/rename Live Peer A');
  await r.type('mlB', '/rename Live Peer B');
  await wait(3000);
  await r.type('mlA', 'Say only the word ok.');
  await r.type('mlB', 'Say only the word ok.');
  const renamed = await until('both sessions renamed and idle', () => {
    const rows = r.session('claude');
    const names = rows.map((x) => x.handle).sort();
    return names.length === 2 && names[0] === 'live-peer-a' && names[1] === 'live-peer-b' && rows.every((x) => x.state === 'idle') ? rows : null;
  }, 120000).catch(() => r.session('claude'));
  log('renamed sessions', JSON.stringify(renamed));
  const handles = renamed.map((x) => x.handle).sort();
  // Native transports follow a rename, so they are keyed by the new handles.
  const native = r.db.prepare('SELECT owner FROM native_transports').all().map((row) => row.owner);
  const nameOk = handles.join(',') === 'live-peer-a,live-peer-b';
  const display = r.db.prepare('SELECT handle, display FROM peers WHERE handle LIKE ?').all('live-peer-%');
  log('peer displays', JSON.stringify(display));
  const { messageId, threadId } = r.send(handles.join(','), ASK);
  const final = await until('both replied', () => {
    const rows = r.deliveries(messageId);
    return rows.every((d) => d.state === 'replied') ? rows : null;
  }, 180000).catch(() => r.deliveries(messageId));
  log('ask deliveries', JSON.stringify(final));
  const replies = r.db.prepare('SELECT id, sender FROM messages WHERE thread_id = ? AND id != ?').all(threadId, messageId);
  const fanout = replies.map((m) => ({ from: m.sender, to: r.db.prepare('SELECT recipient FROM deliveries WHERE message_id = ?').all(m.id).map((d) => d.recipient).sort() }));
  log('reply routing', JSON.stringify(fanout));
  await r.type('mlA', '/murmur-live live-peer-b ' + SLASH_TEXT);
  const slash = await until('slash message', () => r.db.prepare('SELECT id, sender, via, body FROM messages WHERE body LIKE ? ').get('slash check:%'), 40000).catch(() => null);
  log('slash row', JSON.stringify(slash));
  const slashReply = slash === null ? null : await until('reply to the slash message', () => r.db.prepare('SELECT id, sender FROM messages WHERE in_reply_to = ?').get(slash.id), 120000).catch(() => null);
  const slashTo = slashReply === null ? [] : r.db.prepare('SELECT recipient FROM deliveries WHERE message_id = ?').all(slashReply.id).map((d) => d.recipient).sort();
  log('slash reply', JSON.stringify(slashReply), JSON.stringify(slashTo));
  verdict([
    ['both sessions announced with a native transport', handles.length === 2 && handles.every((h) => native.includes(h)), handles.join(', ')],
    ['/rename became each session handle (live-peer-a, live-peer-b) with the title as display', nameOk && display.length === 2 && display.some((d) => d.display === 'Live Peer A') && display.some((d) => d.display === 'Live Peer B'), JSON.stringify(display)],
    ['both deliveries went sent, delivered (transcript proof), replied', final.every((d) => d.state === 'replied' && d.sent && d.delivered), JSON.stringify(final)],
    ['each reply reached the owner who asked and not the other session', fanout.length === 2 && fanout.every((f) => f.to.join(',') === 'renn'), JSON.stringify(fanout)],
    ['/murmur stored the text verbatim as the owner', slash?.body === SLASH_TEXT && slash?.sender === 'renn', JSON.stringify(slash)],
    ['/murmur message has via = mlA handle (live-peer-a)', slash?.via === 'live-peer-a', JSON.stringify(slash?.via)],
    ['B replied and the reply was delivered to mlA as well as the owner', slashReply?.sender === 'live-peer-b' && slashTo.join(',') === 'live-peer-a,renn', JSON.stringify(slashTo)],
  ]);
} finally {
  r.cleanup();
  rmSync(commandFile, { force: true });
  for (const dir of [commandDir, join(REPO, '.claude')]) {
    try { rmdirSync(dir); } catch {}
  }
}
