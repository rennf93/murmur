#!/usr/bin/env node
// Live check, opencode: an interactive session in a scratch project with the
// plugin `murmur install opencode` writes, loaded as a project plugin (its
// SDK types resolve from ~/.config/opencode/node_modules, linked read-only).
// The owner sends one message; the plugin must inject it with promptAsync,
// ack it as delivered, and carry the murmur_reply back.
// Usage: node scripts/live/opencode.mjs [model]   (default opencode/glm-5.3-flash-free,
// which needs opencode 1.18.0 or newer; the first start after an upgrade is slow)
import { copyFileSync, mkdirSync, symlinkSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { ASK, log, rig, until, verdict } from './lib.mjs';

const MODEL = process.argv[2] ?? 'opencode/glm-5.3-flash-free';
const r = await rig('opencode');
const installed = await r.install('opencode');
const project = join(r.root, 'proj');
mkdirSync(join(project, '.opencode', 'plugins'), { recursive: true });
copyFileSync(join(installed, '.config', 'opencode', 'plugins', 'murmur.ts'), join(project, '.opencode', 'plugins', 'murmur.ts'));
symlinkSync(join(homedir(), '.config', 'opencode', 'node_modules'), join(project, '.opencode', 'node_modules'));
writeFileSync(join(project, 'README.md'), 'scratch project for a murmur live check\n');
try {
  r.tmux('mlO', project, 'opencode --model ' + MODEL);
  // The plugin announces its session once opencode creates one.
  await until('tui ready', () => r.pane('mlO').includes('Ask anything'), 60000)
    .catch((err) => { log('pane\n' + r.pane('mlO', 30)); throw err; });
  await r.type('mlO', 'Reply with the single word OK.');
  const [session] = await until('session announced', () => r.session('opencode')[0] && r.session('opencode'), 60000);
  log('session', JSON.stringify(session));
  await until('idle', () => r.session('opencode')[0].state === 'idle', 90000).catch(() => {});
  const { messageId } = r.send(session.handle, ASK);
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
  log('pane\n' + r.pane('mlO', 20));
  verdict([
    ['session announced', session.client === 'opencode', session.handle],
    ['delivery went sent, delivered (plugin ack)', final[0].sent && final[0].delivered, JSON.stringify(final)],
    ['the session replied through murmur_reply', final[0].state === 'replied' && reply?.sender === session.handle, JSON.stringify(reply)],
    ['a second ask after a broker restart reached replied from the same session handle, no CLI restart', final2[0].state === 'replied' && reply2?.sender === session.handle, JSON.stringify([final2, reply2])],
  ]);
} finally {
  r.cleanup();
}
