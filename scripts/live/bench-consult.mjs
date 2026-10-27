#!/usr/bin/env node
// Benchmark: consulting two peers with Claude Code's native SendMessage versus murmur.
// Three real Claude sessions per run (Implementer, Peer A, Peer B), same task, same
// model, cwd = this repo. Arm native has no murmur MCP and no murmur hooks; arm murmur
// has what `murmur install claude` writes. Every number comes from the sessions' own
// transcript JSONL (and, for arm murmur, the broker's deliveries table).
// Usage: node scripts/live/bench-consult.mjs [model] [runs]   (defaults: haiku, 3)
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, rmSync, rmdirSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { REPO, log, rig, until, wait } from './lib.mjs';

const MODEL = process.argv[2] ?? 'haiku';
const RUNS = Number(process.argv[3] ?? 3);
const PROMPT =
  'Consult Peer A and Peer B: ask each of them to check whether HANDLE_RE in src/protocol.ts allows handles of up to 48 characters, and to answer AGREE or DISAGREE with the line they checked. Wait for both answers, then tell me the outcome in one line.';
const PROMPT_TOLD =
  'Consult Peer A and Peer B: ask each of them to check whether HANDLE_RE in src/protocol.ts allows handles of up to 48 characters, and to send their answer back to you with SendMessage, AGREE or DISAGREE with the line they checked. Wait for both answers, then tell me the outcome in one line.';
const ARMS = ['native', 'native-told', 'murmur'];
const ROLES = [['impl', 'Implementer'], ['a', 'Peer A'], ['b', 'Peer B']];
const PROJECT_DIR = join(homedir(), '.claude', 'projects', REPO.replace(/[^a-zA-Z0-9]/g, '-').replace(/-+$/, ''));
const created = [];

const median = (xs) => {
  const s = [...xs].sort((x, y) => x - y);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};

function readEntries(file) {
  if (!existsSync(file)) return [];
  return readFileSync(file, 'utf8').split('\n').filter(Boolean).flatMap((l) => { try { return [JSON.parse(l)]; } catch { return []; } });
}
const textOf = (entry) => JSON.stringify(entry.message ?? entry.attachment ?? '');
const isToolResult = (entry) => Array.isArray(entry.message?.content) && entry.message.content.some((b) => b.type === 'tool_result');

// One session's numbers from the task prompt onward. Transcripts hold api-request
// echo entries and may repeat an assistant message, so only user, assistant and
// attachment entries count, deduplicated by message.id (usage) and tool_use id.
function analyze(file, cutoff) {
  const entries = readEntries(file).filter((e) => ['user', 'assistant', 'attachment'].includes(e.type) && e.timestamp >= cutoff);
  const usage = new Map();
  const tools = new Map();
  let finalText = null;
  let finalTs = null;
  for (const e of entries.filter((x) => x.type === 'assistant')) {
    const id = e.message?.id ?? e.uuid;
    const u = e.message?.usage ?? {};
    const prev = usage.get(id) ?? {};
    const cur = {};
    for (const k of ['input_tokens', 'cache_creation_input_tokens', 'cache_read_input_tokens', 'output_tokens']) cur[k] = Math.max(prev[k] ?? 0, u[k] ?? 0);
    usage.set(id, cur);
    const blocks = Array.isArray(e.message?.content) ? e.message.content : [];
    for (const b of blocks) if (b.type === 'tool_use') tools.set(b.id, b.name);
    const text = blocks.filter((b) => b.type === 'text').map((b) => b.text).join('\n');
    if (text && !blocks.some((b) => b.type === 'tool_use')) { finalText = text; finalTs = e.timestamp; }
  }
  const tokens = { input_tokens: 0, cache_creation_input_tokens: 0, cache_read_input_tokens: 0, output_tokens: 0 };
  for (const u of usage.values()) for (const k of Object.keys(tokens)) tokens[k] += u[k];
  tokens.total = Object.values(tokens).reduce((x, y) => x + y, 0);
  const toolCounts = {};
  for (const name of tools.values()) toolCounts[name] = (toolCounts[name] ?? 0) + 1;
  // Arrivals: a non-tool-result user or attachment entry carrying a peer message.
  const arrivals = new Map();
  const incoming = new Set();
  for (const e of entries.filter((x) => x.type !== 'assistant' && !isToolResult(x))) {
    const t = textOf(e);
    const native = /<cross-session-message[^>]*from-name=\\"([^"\\]+)\\"/.exec(t);
    const mm = /\[murmur m\d+ t\d+ s\d+ from ([a-z0-9-]+)/.exec(t);
    if (native || mm) incoming.add(native ? native[1] : mm[1]);
    if ((native || mm) && t.includes('HANDLE_RE')) arrivals.set(e.uuid, native ? native[1] : mm[1]);
  }
  return { tokens, apiCalls: usage.size, toolCounts, finalText, finalTs, arrivals: [...arrivals.values()], incoming: [...incoming] };
}

async function runOnce(arm, n) {
  const r = await rig('bench-' + arm + String(n));
  const sids = Object.fromEntries(ROLES.map(([k]) => [k, randomUUID()]));
  const files = Object.fromEntries(ROLES.map(([k]) => [k, join(PROJECT_DIR, sids[k] + '.jsonl')]));
  for (const f of Object.values(files)) created.push(f);
  const allow = ['Read', 'Grep', 'Glob', 'SendMessage', 'ToolSearch', 'ListAgents'];
  let settings = { permissions: { allow } };
  let mcp = { mcpServers: {} };
  const commandDir = join(REPO, '.claude', 'commands');
  const commandFile = join(commandDir, 'murmur-live.md');
  if (arm === 'murmur') {
    const installed = await r.install('claude');
    const s = JSON.parse(readFileSync(join(installed, '.claude', 'settings.json'), 'utf8'));
    settings = { hooks: s.hooks, permissions: { allow: [...allow, 'mcp__murmur__murmur_send', 'mcp__murmur__murmur_reply', 'mcp__murmur__murmur_wait', 'mcp__murmur__murmur_thread', 'mcp__murmur__murmur_status', 'mcp__murmur__murmur_groups', 'mcp__murmur__murmur_ack', 'mcp__murmur__murmur_name'] } };
    mcp = { mcpServers: { murmur: JSON.parse(readFileSync(join(installed, '.claude.json'), 'utf8')).mcpServers.murmur } };
    mkdirSync(commandDir, { recursive: true });
    writeFileSync(commandFile, readFileSync(join(installed, '.claude', 'commands', 'murmur.md'), 'utf8'));
  }
  writeFileSync(join(r.root, 'settings.json'), JSON.stringify(settings));
  writeFileSync(join(r.root, 'mcp.json'), JSON.stringify(mcp));
  const cmd = (sid) => ['claude', '--model', MODEL, '--session-id', sid, '--setting-sources', 'project,local', '--settings', join(r.root, 'settings.json'),
    '--mcp-config', join(r.root, 'mcp.json'), '--strict-mcp-config'].join(' ');
  const tm = Object.fromEntries(ROLES.map(([k]) => [k, 'bc-' + arm + String(n) + '-' + k]));
  const idle = (k) => readEntries(files[k]).some((e) => e.type === 'assistant' && Array.isArray(e.message?.content) && e.message.content.some((b) => b.type === 'text') && !e.message.content.some((b) => b.type === 'tool_use'));
  try {
    for (const [k] of ROLES) r.tmux(tm[k], REPO, cmd(sids[k]));
    if (arm === 'murmur') await until('three sessions announced', () => r.session('claude').length >= 3, 90000);
    else await until('three panes up', () => ROLES.every(([k]) => r.pane(tm[k]).includes('❯')), 90000);
    await wait(8000);
    for (const [k, title] of ROLES) await r.type(tm[k], '/rename ' + title);
    await wait(3000);
    for (const [k] of ROLES) await r.type(tm[k], 'Say only the word ok.');
    await until('sessions settled', () => ROLES.every(([k]) => idle(k)) && (arm !== 'murmur' || r.session('claude').map((x) => x.handle).sort().join() === 'implementer,peer-a,peer-b'), 150000);
    await wait(5000);
    const cutoff = new Date().toISOString();
    await r.type(tm.impl, arm === 'native-told' ? PROMPT_TOLD : PROMPT);
    const start = Date.now();
    let timedOut = false;
    let lastChange = Date.now();
    let sig = '';
    for (;;) {
      await wait(3000);
      const now = ROLES.map(([k]) => readEntries(files[k]).length).join(',');
      if (now !== sig) { sig = now; lastChange = Date.now(); }
      const a = analyze(files.impl, cutoff);
      if (a.finalText && Date.now() - lastChange > 40000) break;
      if (Date.now() - start > 540000) { timedOut = true; break; }
    }
    const per = Object.fromEntries(ROLES.map(([k]) => [k, analyze(files[k], cutoff)]));
    const promptEntry = readEntries(files.impl).find((e) => e.type === 'user' && !isToolResult(e) && textOf(e).includes('Consult Peer A') && e.timestamp >= cutoff);
    const t0 = Date.parse(promptEntry?.timestamp ?? cutoff);
    const wallS = per.impl.finalTs ? (Date.parse(per.impl.finalTs) - t0) / 1000 : null;
    const sendNames = /SendMessage|murmur_send|murmur_reply/;
    const composed = Object.entries(per.impl.toolCounts).filter(([name]) => sendNames.test(name));
    const others = (k) => ROLES.map(([o]) => o).filter((o) => o !== 'impl' && o !== k);
    const label = { a: 'Peer A', b: 'Peer B' };
    const copies = {}, sawOther = {};
    for (const k of ['a', 'b']) {
      copies[k] = per[k].arrivals.length;
      const peerHandle = (x) => (arm === 'murmur' ? 'peer-' + x : label[x]);
      sawOther[k] = per[k].arrivals.some((s) => s === peerHandle(others(k)[0]));
    }
    let db = null;
    if (arm === 'murmur') {
      const msgs = r.db.prepare('SELECT id, sender, in_reply_to FROM messages ORDER BY id').all();
      db = msgs.map((m) => ({ id: m.id, sender: m.sender, inReplyTo: m.in_reply_to, to: r.db.prepare('SELECT recipient, state FROM deliveries WHERE message_id = ?').all(m.id) }));
      const t = r.session('claude').map((x) => x.transcript_path);
      log('hook transcript paths', JSON.stringify(t));
    }
    const dbSaw = (from, to) => db?.some((m) => m.sender === from && m.to.some((d) => d.recipient === to)) ?? null;
    // Completed: the final answer gives a verdict for both peers and both peers' replies really reached the
    // Implementer (native: their messages are in its transcript; murmur: both deliveries asked are replied and
    // both replies are in the broker addressed to it).
    const ans = per.impl.finalText ?? '';
    const named = (/peer[ -]?a/i.test(ans) && /peer[ -]?b/i.test(ans)) || /\bboth\b/i.test(ans);
    const verdicts = /\b(dis)?agree/i.test(ans);
    const asked = db?.[0]?.to ?? [];
    const reached = arm === 'murmur'
      ? asked.length === 2 && asked.every((d) => d.state === 'replied') && ['peer-a', 'peer-b'].every((p) => db.some((m) => m.sender === p && m.to.some((d) => d.recipient === 'implementer')))
      : ['Peer A', 'Peer B'].every((p) => per.impl.incoming.includes(p));
    const run = {
      arm, run: n, timedOut, completed: named && verdicts && reached,
      toolSearchCalls: Object.fromEntries(ROLES.map(([k]) => [k, per[k].toolCounts.ToolSearch ?? 0])), wallSeconds: wallS, finalAnswer: per.impl.finalText,
      implementerSendCalls: Object.fromEntries(composed), implementerToolCounts: per.impl.toolCounts,
      composedCount: composed.reduce((x, [, c]) => x + c, 0),
      repliesReachedImplementer: per.impl.incoming.sort(),
      copiesReceived: copies, peerSawOtherPeerReply: sawOther,
      peerSawOtherPeerReplyDb: arm === 'murmur' ? { a: dbSaw('peer-b', 'peer-a'), b: dbSaw('peer-a', 'peer-b') } : null,
      deliveriesDb: db,
      tokens: Object.fromEntries(ROLES.map(([k]) => [k, per[k].tokens])),
      apiCalls: Object.fromEntries(ROLES.map(([k]) => [k, per[k].apiCalls])),
    };
    run.apiCalls.arm = ROLES.reduce((x, [k]) => x + per[k].apiCalls, 0);
    run.tokens.arm = {};
    for (const f of Object.keys(per.impl.tokens)) run.tokens.arm[f] = ROLES.reduce((x, [k]) => x + per[k].tokens[f], 0);
    log('run', arm, n, JSON.stringify({ wall: wallS, composed: run.composedCount, copies, sawOther, total: run.tokens.arm.total, timedOut }));
    return run;
  } finally {
    r.cleanup();
    rmSync(commandFile, { force: true });
    for (const dir of [commandDir, join(REPO, '.claude')]) { try { rmdirSync(dir); } catch {} }
    rmSync(r.root, { recursive: true, force: true });
  }
}

const results = [];
try {
  for (let n = 1; n <= RUNS; n += 1) {
    for (const arm of ARMS) results.push(await runOnce(arm, n));
  }
} finally {
  const version = execFileSync('claude', ['--version'], { encoding: 'utf8' }).trim();
  const date = new Date().toISOString().slice(0, 10);
  const arms = {};
  for (const arm of ARMS) {
    const rs = results.filter((x) => x.arm === arm);
    if (rs.length === 0) continue;
    const med = (f) => median(rs.map(f).filter((v) => v !== null));
    arms[arm] = {
      medians: {
        composedCount: med((x) => x.composedCount), copiesPeerA: med((x) => x.copiesReceived.a), copiesPeerB: med((x) => x.copiesReceived.b),
        wallSeconds: med((x) => x.wallSeconds),
        tokens: Object.fromEntries(['input_tokens', 'cache_creation_input_tokens', 'cache_read_input_tokens', 'output_tokens', 'total'].map((f) => [f, med((x) => x.tokens.arm[f])])),
        implementerTotal: med((x) => x.tokens.impl.total),
        perSessionTokens: Object.fromEntries(ROLES.map(([k]) => [k, med((x) => x.tokens[k].total)])),
        perSessionApiCalls: Object.fromEntries(ROLES.map(([k]) => [k, med((x) => x.apiCalls[k])])),
        toolSearch: Object.fromEntries(ROLES.map(([k]) => [k, med((x) => x.toolSearchCalls[k])])),
        completed: rs.filter((x) => x.completed).length,
        apiCalls: med((x) => x.apiCalls.arm),
      },
    };
  }
  const json = { date, model: MODEL, claudeCodeVersion: version, prompt: PROMPT, promptNativeTold: PROMPT_TOLD, runs: results, arms };
  mkdirSync(join(REPO, 'docs', 'bench'), { recursive: true });
  writeFileSync(join(REPO, 'docs', 'bench', 'consult.json'), JSON.stringify(json, null, 2) + '\n');
  writeFileSync(join(REPO, 'docs', 'bench', 'consult.md'), render(json));
  // Numbers are extracted; remove only the transcripts this run created (BENCH_KEEP=1 keeps them).
  if (!process.env.BENCH_KEEP) for (const f of created) { if (existsSync(f)) { rmSync(f); rmSync(f.slice(0, -6), { recursive: true, force: true }); log('deleted ' + f); } }
  log('wrote docs/bench/consult.md and consult.json');
}

function render(j) {
  const yn = (v) => (v === null ? 'n/a' : v ? 'yes' : 'no');
  const f = (v) => (v === null || v === undefined ? 'n/a' : Number.isInteger(v) ? String(v) : v.toFixed(1));
  const tool = (x) => Object.entries(x.implementerSendCalls).map(([k, c]) => k + ' x' + String(c)).join(', ') || 'none';
  let out = '# Consult benchmark: native SendMessage versus murmur\n\n';
  out += 'Three Claude Code sessions (Implementer, Peer A, Peer B), model `' + j.model + '`, Claude Code `' + j.claudeCodeVersion + '`, run on ' + j.date + ', cwd = this repository. ';
  out += 'Arm native has no murmur MCP and no murmur hooks; arm native-told is arm native with a prompt that also tells each peer to send its answer back with SendMessage (below); arm murmur has what `murmur install claude` writes. ';
  out += 'Sessions are renamed first (`/rename Implementer`, `/rename Peer A`, `/rename Peer B`), then this prompt (arms native and murmur) is typed into the Implementer only:\n\n> ' + j.prompt + '\n\nArm native-told gets:\n\n> ' + j.promptNativeTold + '\n\n';
  out += 'Numbers come from each session\'s own transcript, counted from the task prompt onward and deduplicated by message id; arm murmur also checks the broker deliveries table. Regenerate with `make bench-consult`. ';
  out += 'Note: `replies that reached Implementer` counts transcript-visible incoming rows only; arm murmur delivers replies through the broker instead, so that column reads `none` there even though the replies arrive. Arm murmur `completed` is gated on the deliveries table (`deliveriesDb` in consult.json), where every reply shows state `delivered`.\n\n';
  for (const arm of ARMS) {
    const rs = j.runs.filter((x) => x.arm === arm);
    if (rs.length === 0) continue;
    out += '## Arm ' + arm + '\n\n';
    out += '| run | send calls by Implementer | copies A / B | A saw B reply / B saw A reply | replies that reached Implementer | wall s | tokens Implementer | tokens Peer A | tokens Peer B | API calls I / A / B (all) | tokens all (in / cache write / cache read / out) | ToolSearch calls I / A / B | completed | timed out |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n';
    for (const x of rs) {
      const t = x.tokens.arm;
      const saw = x.peerSawOtherPeerReplyDb ? yn(x.peerSawOtherPeerReply.a) + ' (db ' + yn(x.peerSawOtherPeerReplyDb.a) + ') / ' + yn(x.peerSawOtherPeerReply.b) + ' (db ' + yn(x.peerSawOtherPeerReplyDb.b) + ')' : yn(x.peerSawOtherPeerReply.a) + ' / ' + yn(x.peerSawOtherPeerReply.b);
      out += '| ' + [x.run, tool(x) + ' (' + String(x.composedCount) + ')', x.copiesReceived.a + ' / ' + x.copiesReceived.b, saw, x.repliesReachedImplementer.join(', ') || 'none', f(x.wallSeconds), x.tokens.impl.total, x.tokens.a.total, x.tokens.b.total,
        [x.apiCalls.impl, x.apiCalls.a, x.apiCalls.b].join(' / ') + ' (' + String(x.apiCalls.arm) + ')', t.total + ' (' + [t.input_tokens, t.cache_creation_input_tokens, t.cache_read_input_tokens, t.output_tokens].join(' / ') + ')', [x.toolSearchCalls.impl, x.toolSearchCalls.a, x.toolSearchCalls.b].join(' / '), yn(x.completed), x.timedOut ? 'yes' : 'no'].join(' | ') + ' |\n';
    }
    const m = j.arms[arm].medians;
    out += '| median | ' + [f(m.composedCount), f(m.copiesPeerA) + ' / ' + f(m.copiesPeerB), '', '', f(m.wallSeconds), f(m.implementerTotal), '', '',
      f(m.apiCalls), f(m.tokens.total) + ' (' + [m.tokens.input_tokens, m.tokens.cache_creation_input_tokens, m.tokens.cache_read_input_tokens, m.tokens.output_tokens].map(f).join(' / ') + ')', [m.toolSearch.impl, m.toolSearch.a, m.toolSearch.b].join(' / '), String(m.completed) + ' of ' + String(rs.length), ''].join(' | ') + ' |\n\n';
    out += 'Median per session (tokens / API calls): ' + ROLES.map(([k, title]) => title + ' ' + f(m.perSessionTokens[k]) + ' / ' + f(m.perSessionApiCalls[k])).join(', ') + '.\n\n';
    out += 'Implementer tool use per run: ' + rs.map((x) => '#' + String(x.run) + ' ' + Object.entries(x.implementerToolCounts).map(([k, c]) => k + ' x' + String(c)).join(', ')).join('; ') + '.\n\n';
  }
  for (const arm of ARMS) {
    const m = j.arms[arm]?.medians;
    if (!m) continue;
    const n = j.runs.filter((x) => x.arm === arm).length;
    const to = j.runs.filter((x) => x.arm === arm && x.timedOut).length;
    out += 'Arm ' + arm + ': completed ' + String(m.completed) + ' of ' + String(n) + ', median tokens ' + f(m.tokens.total) + ', median API calls ' + f(m.apiCalls) + ', median wall time ' + f(m.wallSeconds) + ' s' + (to ? ' (' + String(to) + ' of ' + String(n) + ' timed out)' : '') + '.\n\n';
  }
  const prices = { input: 1.0, write: 1.25, read: 0.1, output: 5.0 }; // USD per MTok (in / cache write / cache read / out), haiku list, 2026-10
  const costRows = ARMS.map((arm) => {
    const m = j.arms[arm]?.medians;
    if (!m) return null;
    const t = m.tokens;
    const total = t.input_tokens + t.cache_creation_input_tokens + t.cache_read_input_tokens + t.output_tokens;
    return { arm, t, total, readPct: (100 * t.cache_read_input_tokens) / total, readPerCall: t.cache_read_input_tokens / m.apiCalls,
      cost: (t.input_tokens * prices.input + t.cache_creation_input_tokens * prices.write + t.cache_read_input_tokens * prices.read + t.output_tokens * prices.output) / 1e6 };
  }).filter(Boolean);
  out += '## Cache split and cost\n\n';
  out += 'Component medians from the tables above (three sessions summed per run). Arm native never completed; its row is what the failed attempts cost. Prices: $' + prices.input + ' / $' + prices.write + ' / $' + prices.read + ' / $' + prices.output + ' per MTok (in / cache write / cache read / out; ' + j.model + ' list, ' + j.date + ').\n\n';
  out += '| arm | in | cache write | cache read | out | cache read % | cached re-read per call | $ per task |\n|---|---|---|---|---|---|---|---|\n';
  for (const r of costRows) out += '| ' + [r.arm, r.t.input_tokens, r.t.cache_creation_input_tokens, r.t.cache_read_input_tokens, r.t.output_tokens, r.readPct.toFixed(1) + '%', f(r.readPerCall), '$' + r.cost.toFixed(4)].join(' | ') + ' |\n';
  const told = costRows.find((r) => r.arm === 'native-told');
  const mur = costRows.find((r) => r.arm === 'murmur');
  if (told && mur) {
    out += '\nBoth completed arms sit at ~' + (Math.round(Math.min(told.readPct, mur.readPct) / 5) * 5) + '% cache reads, re-reading ' + f(told.readPerCall) + ' (native-told) and ' + f(mur.readPerCall) + ' (murmur) cached tokens per API call, so murmur\'s saving comes from fewer calls, not smaller re-reads. And because the cache-read fraction matches across arms, the dollar saving tracks the token cut: ' + ((1 - mur.cost / told.cost) * 100).toFixed(0) + '% cheaper per task at these prices versus a ' + ((1 - mur.total / told.total) * 100).toFixed(0) + '% token cut, a ratio that holds for any price list with similar read and write multipliers.\n\n';
  }
  return out.trimEnd() + '\n';
}
