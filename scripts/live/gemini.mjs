#!/usr/bin/env node
// Live check, Gemini CLI: an interactive session in a scratch project whose
// project-level .gemini/settings.json holds what `murmur install gemini`
// writes, plus auto-update off and the isolated MURMUR_HOME for the MCP child.
// Gemini has no inbound channel, so the owner's message waits queued until
// the next BeforeAgent hook pulls it; the check submits one prompt to drive
// that. Needs GEMINI_API_KEY (the legacy personal OAuth login is refused by
// Gemini CLI 0.58).
// Usage: GEMINI_API_KEY=... node scripts/live/gemini.mjs [model]   (default gemini-2.5-flash-lite)
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { ASK, log, rig, until, verdict } from './lib.mjs';

const MODEL = process.argv[2] ?? 'gemini-2.5-flash-lite';
if (!process.env.GEMINI_API_KEY) {
  console.error('gemini live check: set GEMINI_API_KEY');
  process.exit(2);
}
const r = await rig('gemini');
const installed = await r.install('gemini');
const project = join(r.root, 'proj');
const settings = JSON.parse(readFileSync(join(installed, '.gemini', 'settings.json'), 'utf8'));
settings.mcpServers.murmur.env = { MURMUR_HOME: r.env.MURMUR_HOME, PATH: r.env.PATH };
settings.mcpServers.murmur.trust = true;
settings.general = { enableAutoUpdate: false, enableAutoUpdateNotification: false };
settings.security = { auth: { selectedType: 'gemini-api-key' } };
mkdirSync(join(project, '.gemini'), { recursive: true });
writeFileSync(join(project, '.gemini', 'settings.json'), JSON.stringify(settings, null, 2));
writeFileSync(join(project, 'README.md'), 'scratch project for a murmur live check\n');
try {
  r.tmux('mlG', project, 'gemini -m ' + MODEL + ' --skip-trust --yolo',
    { GEMINI_API_KEY: process.env.GEMINI_API_KEY, GEMINI_CLI_TRUST_WORKSPACE: 'true' });
  const [session] = await until('session announced', () => r.session('gemini')[0] && r.session('gemini'), 60000);
  log('session', JSON.stringify(session));
  const { messageId } = r.send(session.handle, ASK);
  const queued = r.deliveries(messageId)[0].state;
  await r.type('mlG', 'Check murmur and do what the message asks.');
  const final = await until('replied', () => (r.deliveries(messageId)[0].state === 'replied' ? r.deliveries(messageId) : null), 180000)
    .catch(() => r.deliveries(messageId));
  log('ask delivery', JSON.stringify(final));
  const reply = r.db.prepare('SELECT sender, body FROM messages WHERE in_reply_to = ?').get(messageId);
  await r.restartBroker();
  const second = r.send(session.handle, ASK + ' (after a broker restart)');
  const final2 = await until('replied after restart', () => (r.deliveries(second.messageId)[0].state === 'replied' ? r.deliveries(second.messageId) : null), 120000)
    .catch(() => r.deliveries(second.messageId));
  log('ask delivery after restart', JSON.stringify(final2));
  const reply2 = r.db.prepare('SELECT sender, body FROM messages WHERE in_reply_to = ?').get(second.messageId);
  log('pane\n' + r.pane('mlG', 20));
  verdict([
    ['session announced by SessionStart', session.client === 'gemini', session.handle],
    ['the message waited queued (no push channel)', queued === 'queued', queued],
    ['BeforeAgent pulled it (delivered) and the session replied', final[0].state === 'replied' && final[0].delivered, JSON.stringify(final)],
    ['the reply came from the session through murmur_reply', reply?.sender === session.handle, JSON.stringify(reply)],
    ['a second ask after a broker restart reached replied from the same session handle, no CLI restart', final2[0].state === 'replied' && reply2?.sender === session.handle, JSON.stringify([final2, reply2])],
  ]);
} finally {
  r.cleanup();
}
