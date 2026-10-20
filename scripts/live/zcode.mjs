#!/usr/bin/env node
// Live check, zcode: an interactive session in a scratch project whose
// project-level .zcode/config.json is what `murmur install zcode` writes.
// zcode runs project hooks only after a persistent workspace trust grant
// (there is no session-only trust), so the check grants trust for the
// scratch folder with zcode's own command and revokes it afterwards; if the
// trust store did not exist before, it is removed again. zcode has no inbound
// channel: the owner's message waits queued until the next UserPromptSubmit
// hook pulls it, so the check runs a second prompt to drive that.
// Usage: node scripts/live/zcode.mjs   (the model is zcode's configured default;
// ZCODE_CLI overrides the zcode.cjs path)
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, readdirSync, rmdirSync, rmSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { ASK, log, rig, until, verdict } from './lib.mjs';

const ZCODE = process.env.ZCODE_CLI ?? '/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs';
const STORE = join(process.env.ZCODE_DATA_BASE_DIR ?? homedir(), '.zcode', 'v2', 'security', 'workspace-hook-trust-v1.json');
const zcode = (...args) => spawnSync('node', [ZCODE, ...args], { encoding: 'utf8' });

const r = await rig('zcode');
const installed = await r.install('zcode');
const project = join(r.root, 'proj');
mkdirSync(join(project, '.zcode'), { recursive: true });
writeFileSync(join(project, '.zcode', 'config.json'), readFileSync(join(installed, '.zcode', 'cli', 'config.json'), 'utf8'));
writeFileSync(join(project, 'README.md'), 'scratch project for a murmur live check\n');
const storeExisted = existsSync(STORE);
// status exits 1 while the hooks are pending; its JSON carries the bundle digest.
const { bundleDigest } = JSON.parse(zcode('hooks', 'trust', 'status', '--workspace', project, '--json').stdout);
const granted = zcode('hooks', 'trust', 'grant', '--workspace', project, '--all-current', '--bundle-digest', bundleDigest);
log('trust grant exit ' + granted.status);
// The zcode.cjs shipped in the app has no TUI package, so the check drives
// two headless prompts; the second resumes the first session (--continue).
const run = (prompt, ...flags) => {
  const out = spawnSync('node', [ZCODE, '-p', prompt, '--mode', 'yolo', ...flags], {
    cwd: project,
    encoding: 'utf8',
    timeout: 240000,
    env: { ...r.env, ZCODE_BUILTIN_PROVIDER_CONFIG_FILE: join(ZCODE, '..', '..', 'config', 'provider', 'zcode-builtin.json') },
  });
  log('zcode exit ' + out.status + '\n' + (out.stdout + out.stderr).trim().split('\n').slice(-8).join('\n'));
};
try {
  run('say hi in 3 words');
  const [session] = await until('session announced', () => r.session('zcode')[0] && r.session('zcode'), 10000);
  log('session', JSON.stringify(session));
  const { messageId } = r.send(session.handle, ASK);
  const queued = r.deliveries(messageId)[0].state;
  run('Check murmur and do what the message asks.', '--continue');
  const final = await until('replied', () => (r.deliveries(messageId)[0].state === 'replied' ? r.deliveries(messageId) : null), 20000)
    .catch(() => r.deliveries(messageId));
  log('ask delivery', JSON.stringify(final));
  const reply = r.db.prepare('SELECT sender, body FROM messages WHERE in_reply_to = ?').get(messageId);
  log('sessions', JSON.stringify(r.session('zcode')));
  verdict([
    ['session announced by a hook', session.client === 'zcode', session.handle],
    ['the message waited queued (no push channel)', queued === 'queued', queued],
    ['UserPromptSubmit pulled it (delivered) and the session replied', final[0].state === 'replied' && final[0].delivered, JSON.stringify(final)],
    ['the reply came from the session through murmur_reply', reply?.sender === session.handle, JSON.stringify(reply)],
  ]);
} finally {
  r.cleanup();
  log('trust revoke exit ' + zcode('hooks', 'trust', 'revoke', '--workspace', project, '--all').status);
  if (!storeExisted && existsSync(STORE)) {
    const dir = dirname(STORE);
    for (const name of readdirSync(dir)) {
      if (name.startsWith('workspace-hook-trust-v1')) rmSync(join(dir, name), { force: true });
    }
    try { rmdirSync(dir); } catch {}
    log('removed the trust store this check created');
  }
}
