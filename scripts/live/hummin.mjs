#!/usr/bin/env node
// Live check, Hummin: an interactive session in a scratch project with the
// extension `murmur install hummin` writes, loaded as a project extension
// (trusted for this session only). The owner sends one message; the extension
// must inject it, ack it as delivered, and carry the murmur_reply back. Then
// the model-free /murmur must store its text verbatim as the owner.
// Usage: node scripts/live/hummin.mjs [model]   (default zai/glm-5.3-flash)
import { copyFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { ASK, log, rig, until, verdict, wait } from './lib.mjs';

const MODEL = process.argv[2] ?? 'zai/glm-5.3-flash';
const r = await rig('hummin');
const installed = await r.install('hummin');
const project = join(r.root, 'proj');
mkdirSync(join(project, '.hummin', 'extensions'), { recursive: true });
copyFileSync(join(installed, '.hummin', 'agent', 'extensions', 'murmur.ts'), join(project, '.hummin', 'extensions', 'murmur.ts'));
writeFileSync(join(project, 'README.md'), 'scratch project for a murmur live check\n');
const SLASH_TEXT = "owner check: it's $(not run) & `literal`";
try {
  r.tmux('mlH', project, 'hummin --model ' + MODEL);
  await until('trust prompt', () => r.pane('mlH').includes('Trust project folder?'), 30000);
  // Third option: "Trust (this session only)".
  await r.keys('mlH', 'Down', 'Down', 'Enter');
  const [session] = await until('session announced', () => r.session('hummin')[0] && r.session('hummin'), 60000);
  log('session', JSON.stringify(session));
  await until('idle', () => r.session('hummin')[0].state === 'idle', 90000).catch(() => {});
  const { messageId } = r.send(session.handle, ASK);
  const final = await until('replied', () => (r.deliveries(messageId)[0].state === 'replied' ? r.deliveries(messageId) : null), 180000)
    .catch(() => r.deliveries(messageId));
  log('ask delivery', JSON.stringify(final));
  const reply = r.db.prepare('SELECT sender, body FROM messages WHERE in_reply_to = ?').get(messageId);
  await r.type('mlH', '/murmur ' + session.handle + ' ' + SLASH_TEXT);
  const slash = await until('slash message', () => r.db.prepare('SELECT sender, body FROM messages WHERE body LIKE ?').get('owner check:%'), 20000).catch(() => null);
  await wait(1000);
  log('pane\n' + r.pane('mlH'));
  verdict([
    ['session announced', session.client === 'hummin', session.handle],
    ['delivery went sent, delivered (extension ack), replied', final[0].state === 'replied' && final[0].sent && final[0].delivered, JSON.stringify(final)],
    ['the reply came from the session through murmur_reply', reply?.sender === session.handle, JSON.stringify(reply)],
    ['/murmur stored the text verbatim as the owner', slash?.body === SLASH_TEXT && slash?.sender === 'renn', JSON.stringify(slash)],
  ]);
} finally {
  r.cleanup();
}
