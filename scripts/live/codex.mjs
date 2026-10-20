#!/usr/bin/env node
// Live check, Codex: an interactive session in a scratch project whose
// project-level .codex/hooks.json and config.toml are what `murmur install
// codex` writes, plus two keys the check needs (MCP calls pre-approved, and
// the isolated MURMUR_HOME, which Codex does not pass to MCP children). The
// real ~/.codex is only read for auth; folder trust is an in-memory -c
// override (a quoted dotted key is not matched, and the prompt it leaves
// would persist trust). The owner sends one message; it must be
// pushed with `codex queue`, proven by the transcript tag, and answered.
// Usage: node scripts/live/codex.mjs [model]   (default gpt-5.6-luna)
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { ASK, log, rig, until, verdict, wait } from './lib.mjs';

const MODEL = process.argv[2] ?? 'gpt-5.6-luna';
const r = await rig('codex');
const installed = await r.install('codex');
const project = join(r.root, 'proj');
mkdirSync(join(project, '.codex'), { recursive: true });
copyFileSync(join(installed, '.codex', 'hooks.json'), join(project, '.codex', 'hooks.json'));
writeFileSync(join(project, '.codex', 'config.toml'), readFileSync(join(installed, '.codex', 'config.toml'), 'utf8')
  + 'default_tools_approval_mode = "approve"\n'
  + 'env = { MURMUR_HOME = ' + JSON.stringify(r.env.MURMUR_HOME) + ', PATH = ' + JSON.stringify(r.env.PATH) + ' }\n');
writeFileSync(join(project, 'README.md'), 'scratch project for a murmur live check\n');
const codex = ['codex', '-m', MODEL, '-c', 'model_reasoning_effort="low"', '-c', `projects={${JSON.stringify(project)}={trust_level="trusted"}}`,
  '-c', 'check_for_update_on_startup=false', '--dangerously-bypass-hook-trust', '-a', 'never', '-s', 'read-only']
  .map((arg) => `'${arg}'`).join(' ');
try {
  r.tmux('mlC', project, codex);
  // Codex runs SessionStart with the first turn, so start one.
  await wait(10000);
  await r.type('mlC', 'say hi in 3 words');
  const [session] = await until('session announced', () => r.session('codex')[0] && r.session('codex'), 90000);
  log('session', JSON.stringify(session));
  await until('idle', () => r.session('codex')[0].state === 'idle', 90000).catch(() => {});
  const { messageId } = r.send(session.handle, ASK);
  const final = await until('replied', () => (r.deliveries(messageId)[0].state === 'replied' ? r.deliveries(messageId) : null), 180000)
    .catch(() => r.deliveries(messageId));
  log('ask delivery', JSON.stringify(final));
  const reply = r.db.prepare('SELECT sender, body FROM messages WHERE in_reply_to = ?').get(messageId);
  log('pane\n' + r.pane('mlC', 20));
  verdict([
    ['session announced with its thread id', session.cli_session_id !== null, session.handle + ' ' + session.cli_session_id],
    ['delivery went sent (codex queue), delivered (transcript proof), replied', final[0].state === 'replied' && final[0].sent && final[0].delivered, JSON.stringify(final)],
    ['the reply came from the session through murmur_reply', reply?.sender === session.handle, JSON.stringify(reply)],
  ]);
} finally {
  r.cleanup();
}
