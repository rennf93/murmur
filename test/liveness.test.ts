import Database from 'better-sqlite3';
import { copyFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { MurmurCore } from '../src/core.js';
import type { ProcInfo } from '../src/proc.js';
import { Store } from '../src/store.js';

// The broker runs on OMEN. A session of the Mac is a session of another
// machine: the broker cannot see its pid, so it is alive for a lease after
// its last event, or while one of its connections is open.
// The broker validates machine ids as uuids (src/machine.ts validBootId); the core takes any string.
const OMEN = '0e000000-0000-4000-8000-000000000001';
const MAC = '0a000000-0000-4000-8000-000000000009';
const HOUR = 60 * 60 * 1000;
const LEASE = 12 * HOUR;

let dir: string;
let store: Store;
let core: MurmurCore;
let now: number;
let procs: Map<number, ProcInfo>;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'murmur-live-'));
  store = Store.open(join(dir, 'murmur.db'));
  now = 1_000_000;
  procs = new Map();
  core = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, () => now, () => procs, OMEN);
});

afterEach(() => {
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

const local = (pid: number) => {
  procs.set(pid, { ppid: 1, start: 'S' + String(pid), comm: 'claude' });
  return { hostPid: pid, hostStart: 'S' + String(pid), machine: OMEN };
};
// Two machines that are not the same machine have different fingerprints.
const FP_A = 'a'.repeat(32);
const FP_B = 'b'.repeat(32);
const remote = (pid = 51234, boot = MAC, name = 'mac', uptime?: number, fingerprint: string | null = FP_A) => ({
  hostPid: pid,
  hostStart: 'T' + String(pid),
  machine: boot,
  machineName: name,
  ...(uptime !== undefined ? { machineUptime: uptime } : {}),
  ...(fingerprint !== null ? { machineFingerprint: fingerprint } : {}),
});
type Host = { hostPid: number; hostStart: string; machine?: string; machineName?: string; machineUptime?: number; machineFingerprint?: string };
const open = (cli: string, host: Host, cwd: string, title?: string) =>
  core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: cli, host, cwd, source: 'startup', ...(title !== undefined ? { title } : {}) });

describe('two machines, one broker', () => {
  it('gives sessions of two machines in one folder distinct handles, and shows two peers', () => {
    const mac = open('mac', remote(), '/Users/renzof/work/guard');
    const omen = open('omen', local(100), '/home/renzof/work/guard');
    expect([mac.handle, omen.handle]).toEqual(['claude-guard', 'claude-guard-2']);
    const peers = core.status(() => ({ online: false })).peers.map((p) => p.handle);
    expect(peers).toEqual(expect.arrayContaining(['claude-guard', 'claude-guard-2']));
  });

  it('does not let a title take the handle of a live session of another machine', () => {
    const mac = open('mac', remote(), '/Users/renzof/work/guard');
    const omen = open('omen', local(100), '/home/renzof/work/other', 'claude guard');
    expect(mac.handle).toBe('claude-guard');
    expect(omen.handle).not.toBe('claude-guard');
    expect(store.sessionByCliId('claude', 'mac')?.handle).toBe('claude-guard');
  });

  it('keeps two sessions that share a pid and a start apart when their machines differ', () => {
    const host = local(100);
    const sameNumbers = { hostPid: 100, hostStart: 'S100', machine: MAC };
    const omen = open('omen', host, '/w/a');
    const mac = open('mac', sameNumbers, '/w/b');
    expect(omen.id).not.toBe(mac.id);
    expect(core.sessionForHost(host)?.id).toBe(omen.id);
    expect(core.sessionForHost(sameNumbers)?.id).toBe(mac.id);
  });

  it('keeps a session of another machine for the 12 hour lease and frees its handle after it', () => {
    open('mac', remote(), '/Users/renzof/work/guard');
    now += LEASE - 1000;
    expect(core.claimable('claude-guard', null)).toBe(false);
    now += 2000;
    expect(core.claimable('claude-guard', null)).toBe(true);
    expect(open('omen', local(100), '/home/renzof/work/guard').handle).toBe('claude-guard');
  });

  it('renews the lease with every event of the session', () => {
    const host = remote();
    open('mac', host, '/Users/renzof/work/guard');
    now += LEASE - 1000;
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host });
    now += LEASE - 1000;
    expect(core.claimable('claude-guard', null)).toBe(false);
  });

  it('keeps a session of another machine alive past the lease while a connection of its host is open', () => {
    open('mac', remote(), '/Users/renzof/work/guard');
    core.hostConnected = (h) => h.hostPid === 51234 && h.hostStart === 'T51234';
    now += 2 * LEASE;
    expect(core.claimable('claude-guard', null)).toBe(false);
  });

  it('does not keep a session alive for a connection of another machine with the same pid and start', () => {
    open('mac', remote(), '/Users/renzof/work/guard');
    core.hostConnected = (h) => h.hostPid === 51234 && h.hostStart === 'T51234' && h.hostMachine === OMEN;
    now += 2 * LEASE;
    expect(core.claimable('claude-guard', null)).toBe(true);
  });

  it('judges a client that sends no machine exactly as before: by the process table', () => {
    const dead = { hostPid: 7001, hostStart: 'S7001' };
    open('old-1', dead, '/w/guard');
    expect(core.claimable('claude-guard', null)).toBe(true);
    const alive = { ...local(7002), machine: undefined };
    open('old-2', alive, '/w/other');
    expect(core.claimable('claude-other', null)).toBe(false);
    expect(store.sessionByCliId('claude', 'old-2')?.hostMachine).toBeNull();
  });

  it('stores nothing for a client of the broker machine and the machine for another', () => {
    open('omen', local(100), '/w/a');
    open('mac', remote(), '/w/b');
    expect(store.sessionByCliId('claude', 'omen')?.hostMachine).toBeNull();
    expect(store.sessionByCliId('claude', 'mac')?.hostMachine).toBe(MAC);
  });

  it('does not match a shell of another machine to a local session by pid', () => {
    const host = local(100);
    const omen = open('omen', host, '/w/a');
    expect(core.sessionForAncestors([100, 1], OMEN)?.id).toBe(omen.id);
    expect(core.sessionForAncestors([100, 1])?.id).toBe(omen.id);
    expect(core.sessionForAncestors([100, 1], MAC)).toBeNull();
  });
});

describe('a crashed session of this machine', () => {
  it('is no sibling: restarting in the folder and naming the new session carries the groups', () => {
    open('crashed', { hostPid: 7001, hostStart: 'S7001', machine: OMEN }, '/w/guard');
    store.createGroup('crew', now);
    store.addMember('crew', 'claude-guard', now);
    const restarted = open('restarted', local(100), '/w/guard');
    expect(restarted.handle).toBe('claude-guard');
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'restarted', host: local(100), title: 'Proveground S4c: bait' });
    expect(store.sessionByCliId('claude', 'restarted')?.handle).toBe('proveground-s4c-bait');
    expect(store.listGroups().find((g) => g.name === 'crew')?.members).toEqual(['proveground-s4c-bait']);
  });

  it('still keeps a live session of another machine apart when one of two sessions is named', () => {
    const mac = open('mac', remote(), '/Users/renzof/work/guard');
    const omen = open('omen', local(100), '/home/renzof/work/guard');
    core.renameSession(omen, 'chosen-name');
    expect(store.sessionById(mac.id)?.handle).toBe('claude-guard');
  });
});

describe('the native transport of a handle two live sessions hold', () => {
  const event = (cli: string, host: Host, kind: 'session-start' | 'session-end' | 'prompt', path?: string) =>
    core.sessionEvent({ event: kind, client: 'claude', cliSessionId: cli, host, cwd: '/w/guard', ...(path !== undefined ? { native: { socketPath: path, token: 't' } } : {}) });

  it('is not overwritten by the other session and survives its SessionEnd', () => {
    // The first session looks dead when the second starts (a client without a
    // machine on another machine), so the second takes its handle; then the
    // first is seen alive.
    const first = { hostPid: 100, hostStart: 'S100' };
    const a = event('a', first, 'session-start', '/tmp/a.sock');
    const b = event('b', local(101), 'session-start');
    expect(b.handle).toBe(a.handle);
    local(100);
    event('b', local(101), 'prompt', '/tmp/b.sock');
    expect(store.getNativeTransport(a.handle)?.socketPath).toBe('/tmp/a.sock');
    event('b', local(101), 'session-end');
    expect(store.getNativeTransport(a.handle)?.socketPath).toBe('/tmp/a.sock');
    event('a', local(100), 'session-end');
    expect(store.getNativeTransport(a.handle)).toBeNull();
  });

  it('is replaced by a successor of a crashed session, which is no holder', () => {
    event('crashed', { hostPid: 7001, hostStart: 'S7001', machine: OMEN }, 'session-start', '/old.sock');
    const successor = event('successor', local(100), 'session-start', '/new.sock');
    expect(successor.handle).toBe('claude-guard');
    expect(store.getNativeTransport('claude-guard')?.socketPath).toBe('/new.sock');
    event('successor', local(100), 'session-end');
    expect(store.getNativeTransport('claude-guard')).toBeNull();
  });
});

describe('what the review of the first version found', () => {
  it('keeps the lease to the millisecond: alive at 12 hours minus 1 ms and at exactly 12 hours, gone 1 ms later', () => {
    open('mac', remote(), '/Users/renzof/work/guard');
    now += LEASE - 1;
    expect(core.claimable('claude-guard', null)).toBe(false);
    now += 1;
    expect(core.claimable('claude-guard', null)).toBe(false);
    now += 1;
    expect(core.claimable('claude-guard', null)).toBe(true);
  });

  it('moves the machine of a session whose client starts sending one', () => {
    const host = { hostPid: 500, hostStart: 'T500' };
    const first = open('mac', host, '/Users/renzof/work/guard');
    expect(first.hostMachine).toBeNull();
    const upgraded = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: { ...host, machine: MAC } });
    expect(upgraded.id).toBe(first.id);
    expect(store.sessionById(first.id)?.hostMachine).toBe(MAC);
  });

  it('does not take the host of one machine for the host of another when the broker looks a session up', () => {
    const host = local(100);
    const omen = open('omen', host, '/w/a');
    open('mac', { hostPid: 100, hostStart: 'S100', machine: MAC }, '/w/b');
    expect(core.sessionForHost({ hostPid: 100, hostStart: 'S100', machine: OMEN })?.id).toBe(omen.id);
    expect(core.sessionForHost({ hostPid: 100, hostStart: 'S100' })?.id).toBe(omen.id);
  });
});

describe('a database made by the code before host_machine', () => {
  it('opens, gains the column, and keeps its sessions and messages', () => {
    const copy = join(dir, 'old.db');
    copyFileSync(join(import.meta.dirname, 'fixtures', 'murmur-master-schema.db'), copy);
    const before = new Database(copy, { readonly: true });
    expect((before.prepare('PRAGMA table_info(sessions)').all() as Array<{ name: string }>).map((c) => c.name)).not.toContain('host_machine');
    expect((before.prepare('PRAGMA table_info(sessions)').all() as Array<{ name: string }>).map((c) => c.name)).not.toContain('host_label');
    before.close();
    const old = Store.open(copy);
    const row = old.sessionByCliId('claude', 'old-1');
    expect(row).toMatchObject({ handle: 'old-session', hostPid: 4242, hostMachine: null, hostLabel: null });
    expect(old.sessionByHost(4242, 'Mon Nov  9 10:00:00 2026', null)?.id).toBe(row?.id);
    expect(old.sessionByHost(4242, 'Mon Nov  9 10:00:00 2026', MAC)).toBeNull();
    expect(old.threadMessages(1, null)).toHaveLength(1);
    const migrated = new MurmurCore(old, { onAppend: () => {}, onStateChange: () => {} }, () => now, () => procs, OMEN);
    expect(migrated.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'old-1', host: { hostPid: 4242, hostStart: 'Mon Nov  9 10:00:00 2026' } }).handle).toBe('old-session');
    old.close();
    const again = Store.open(copy);
    expect(again.sessionByCliId('claude', 'old-1')?.handle).toBe('old-session');
    again.close();
  });
});

describe('one machine under two host names, and clients without a boot id', () => {
  it('keeps one session when the host name of a machine changes and its boot id does not', () => {
    const first = open('mac', remote(51234, MAC, 'mac-mini'), '/Users/renzof/work/guard');
    const again = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: remote(51234, MAC, 'mac-mini-2') });
    expect(again.id).toBe(first.id);
    const byHost = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: null, host: remote(51234, MAC, 'renamed-again') });
    expect(byHost.id).toBe(first.id);
    expect(store.listSessions()).toHaveLength(1);
    expect(store.sessionById(first.id)).toMatchObject({ handle: 'claude-guard', hostMachine: MAC, hostLabel: 'renamed-again' });
  });

  it('judges a client that has no boot id, so sends no machine, by the process table: one session', () => {
    const host = local(100);
    const first = open('omen', { hostPid: host.hostPid, hostStart: host.hostStart }, '/w/guard');
    const second = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'omen', host: { hostPid: host.hostPid, hostStart: host.hostStart } });
    expect(second.id).toBe(first.id);
    expect(first.hostMachine).toBeNull();
    expect(core.claimable('claude-guard', null)).toBe(false);
    procs.delete(100);
    expect(core.claimable('claude-guard', null)).toBe(true);
  });

  it('while the broker has no boot id takes a client that sends one for another machine, and one that sends none for its own', () => {
    const noBoot = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, () => now, () => procs, '');
    const sees = (cli: string, host: Host, cwd: string) => noBoot.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: cli, host, cwd, source: 'startup' });
    const withId = sees('with-id', remote(), '/w/guard');
    expect(withId).toMatchObject({ hostMachine: MAC, hostLabel: 'mac', handle: 'claude-guard' });
    expect(noBoot.claimable('claude-guard', null)).toBe(false);
    // The same folder on another machine, or on this one: never the same handle.
    expect(sees('other', remote(77, OMEN, 'omen'), '/w/guard').handle).toBe('claude-guard-2');
    const old = sees('old', { hostPid: 7001, hostStart: 'S7001' }, '/w/guard');
    expect(old).toMatchObject({ hostMachine: null, hostLabel: null, handle: 'claude-guard-3' });
    expect(noBoot.claimable('claude-guard-3', null)).toBe(true);
  });
});

describe('a broker that restarts', () => {
  it('counts every session of another machine alive for a minute, then judges it by lease and connection', () => {
    open('mac', remote(), '/Users/renzof/work/guard');
    core.hostConnected = () => true;
    now += 20 * HOUR;
    expect(core.claimable('claude-guard', null)).toBe(false);
    // The broker restarts: the connection that kept the session alive is gone.
    const restarted = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, () => now, () => procs, OMEN);
    expect(restarted.claimable('claude-guard', null)).toBe(false);
    now += 60 * 1000;
    expect(restarted.claimable('claude-guard', null)).toBe(false);
    now += 1;
    expect(restarted.claimable('claude-guard', null)).toBe(true);
  });

  it('does not extend the grace to a session that ended', () => {
    const host = remote();
    open('mac', host, '/Users/renzof/work/guard');
    core.sessionEvent({ event: 'session-end', client: 'claude', cliSessionId: 'mac', host });
    const restarted = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, () => now, () => procs, OMEN);
    expect(restarted.claimable('claude-guard', null)).toBe(true);
  });
});

describe('two machines that share a host name', () => {
  it('do not end each other: the old sessions were heard from after the new machine booted', () => {
    const a = open('a', remote(100, 'boot-a', 'Mac', 3 * 86400), '/Users/a/work/guard');
    now += HOUR;
    const b = open('b', remote(200, 'boot-b', 'Mac', 3 * 86400, FP_B), '/Users/b/work/guard');
    expect(store.sessionById(a.id)?.state).not.toBe('ended');
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'a', host: remote(100, 'boot-a', 'Mac', 3 * 86400 + 5) });
    expect(store.sessionById(b.id)?.state).not.toBe('ended');
    now += 40 * 1000;
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'b', host: remote(200, 'boot-b', 'Mac', 3 * 86400 + 45, FP_B) });
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'a', host: remote(100, 'boot-a', 'Mac', 3 * 86400 + 50) });
    expect(store.listSessions().map((r) => r.state)).toEqual(['busy', 'busy']);
  });

  it('do not end each other when a late event of the older boot arrives', () => {
    const fresh = open('fresh', remote(900, 'boot-new', 'Mac', 600), '/Users/a/work/guard');
    now += 120 * 1000;
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'late', host: remote(100, 'boot-old', 'Mac', 3 * 86400) });
    expect(store.sessionById(fresh.id)?.state).not.toBe('ended');
  });

  it('leave the old sessions to their lease when the event says nothing about the boot, or their host is connected', () => {
    const a = open('a', remote(100, 'boot-a', 'Mac'), '/Users/a/work/guard');
    now += HOUR;
    open('b', remote(200, 'boot-b', 'Mac'), '/Users/b/work/guard');
    expect(store.sessionById(a.id)?.state).not.toBe('ended');
    core.hostConnected = (h) => h.hostPid === 100;
    open('c', remote(300, 'boot-c', 'Mac', 10), '/Users/c/work/guard');
    expect(store.sessionById(a.id)?.state).not.toBe('ended');
  });
});

describe('a machine that reboots', () => {
  it('ends its old sessions at once when an event arrives with a new boot id under the same host name', () => {
    const old = open('old', remote(51234, 'mac-boot-1', 'mac'), '/Users/renzof/work/guard');
    store.createGroup('crew', now);
    store.addMember('crew', old.handle, now);
    core.registerNative({ handle: old.handle, socketPath: '/tmp/old.sock', token: 't' });
    now += 3 * 60 * 1000;
    const fresh = open('fresh', remote(777, 'mac-boot-2', 'mac', 90), '/Users/renzof/work/guard');
    expect(store.sessionById(old.id)?.state).toBe('ended');
    expect(fresh.handle).toBe(old.handle);
    expect(store.getNativeTransport(old.handle)).toBeNull();
    expect(store.listGroups().find((g) => g.name === 'crew')?.members).toEqual([old.handle]);
  });

  it('keeps the transport of a handle another live session still holds when a reboot ends one of them', () => {
    const old = open('old', remote(51234, 'mac-boot-1', 'mac'), '/Users/renzof/work/guard');
    const local1 = open('omen', local(100), '/home/renzof/work/other');
    store.reassignSession(local1.id, local1.handle, old.handle, false);
    core.registerNative({ handle: old.handle, socketPath: '/tmp/shared.sock', token: 't' });
    now += 3 * 60 * 1000;
    open('fresh', remote(777, 'mac-boot-2', 'mac', 90), '/Users/renzof/work/third');
    expect(store.sessionById(old.id)?.state).toBe('ended');
    expect(store.getNativeTransport(old.handle)?.socketPath).toBe('/tmp/shared.sock');
  });

  it('keeps a resumed session of the same CLI id and ends the others of the old boot', () => {
    const resumed = open('resumed', remote(51234, 'mac-boot-1', 'mac'), '/Users/renzof/work/guard');
    const other = open('other', remote(51300, 'mac-boot-1', 'mac'), '/Users/renzof/work/other');
    core.registerNative({ handle: resumed.handle, socketPath: '/tmp/resumed.sock', token: 't' });
    now += 3 * 60 * 1000;
    const back = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'resumed', host: remote(900, 'mac-boot-2', 'mac', 90), source: 'resume' });
    expect(back.id).toBe(resumed.id);
    expect(back).toMatchObject({ hostMachine: 'mac-boot-2', state: 'idle' });
    expect(store.sessionById(other.id)?.state).toBe('ended');
    expect(store.getNativeTransport(resumed.handle)?.socketPath).toBe('/tmp/resumed.sock');
  });

  it('reads a session heard from at the very moment the new boot began as not before it', () => {
    now += 61 * 1000;
    const old = open('old', remote(51234, 'mac-boot-1', 'mac'), '/Users/renzof/work/guard');
    open('fresh', remote(777, 'mac-boot-2', 'mac', 0), '/Users/renzof/work/other');
    expect(store.sessionById(old.id)?.state).not.toBe('ended');
    now += 1;
    open('later', remote(778, 'mac-boot-2', 'mac', 0), '/Users/renzof/work/third');
    expect(store.sessionById(old.id)?.state).toBe('ended');
  });

  it('leaves the old sessions to their lease when the host name changed too, and keeps other machines apart', () => {
    const old = open('old', remote(51234, 'mac-boot-1', 'mac'), '/Users/renzof/work/guard');
    now += 3 * 60 * 1000;
    const renamed = open('renamed', remote(777, 'mac-boot-2', 'mac-renamed', 90), '/Users/renzof/work/guard');
    expect(store.sessionById(old.id)?.state).not.toBe('ended');
    expect(renamed.handle).toBe('claude-guard-2');
    open('elsewhere', remote(5, 'linux-boot-1', 'box', 90), '/w/x');
    expect(store.sessionById(old.id)?.state).not.toBe('ended');
  });

  it('does not end sessions of the broker machine or of a client without a host name', () => {
    const here = open('here', local(100), '/w/a');
    const noLabel = open('nolabel', { hostPid: 61, hostStart: 'T61', machine: 'mac-boot-1' }, '/w/b');
    now += 3 * 60 * 1000;
    open('fresh', remote(777, 'mac-boot-2', 'mac', 90), '/w/c');
    expect(store.sessionById(here.id)?.state).not.toBe('ended');
    expect(store.sessionById(noLabel.id)?.state).not.toBe('ended');
  });
});

describe('an event that carries less than the session had', () => {
  it('keeps the machine and the label of a session whose client could not read a boot id', () => {
    const first = open('mac', remote(51234, MAC, 'mac'), '/Users/renzof/work/guard');
    const bare = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: { hostPid: 51234, hostStart: 'T51234' } });
    expect(bare.id).toBe(first.id);
    expect(store.sessionById(first.id)).toMatchObject({ hostMachine: MAC, hostLabel: 'mac' });
    expect(core.claimable('claude-guard', null)).toBe(false);
  });

  it('keeps the label when an event names the same machine without one, and drops it for another machine', () => {
    const first = open('mac', remote(51234, MAC, 'mac'), '/Users/renzof/work/guard');
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: { hostPid: 51234, hostStart: 'T51234', machine: MAC } });
    expect(store.sessionById(first.id)?.hostLabel).toBe('mac');
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: { hostPid: 51234, hostStart: 'T51234', machine: 'another-boot' } });
    expect(store.sessionById(first.id)).toMatchObject({ hostMachine: 'another-boot', hostLabel: null });
  });

  it('lets an event with the broker machine id take a session back to the broker machine', () => {
    const first = open('mac', remote(51234, MAC, 'mac'), '/Users/renzof/work/guard');
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: { hostPid: 51234, hostStart: 'T51234', machine: OMEN } });
    expect(store.sessionById(first.id)).toMatchObject({ hostMachine: null, hostLabel: null });
  });
});

describe('a client that gains a machine id', () => {
  it('finds the row an old client of the same process wrote, and no second one appears', () => {
    const host = { hostPid: 500, hostStart: 'T500' };
    const old = open('old', host, '/Users/renzof/work/guard', 'Proveground S4c bait');
    expect(old.hostMachine).toBeNull();
    const upgraded = { ...host, machine: MAC, machineName: 'mac' };
    expect(core.sessionForHost(upgraded)?.id).toBe(old.id);
    expect(store.sessionById(old.id)).toMatchObject({ hostMachine: MAC, hostLabel: 'mac', handle: 'proveground-s4c-bait' });
    const again = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'old', host: upgraded });
    expect(again.id).toBe(old.id);
    expect(store.listSessions()).toHaveLength(1);
  });

  it('does not take the row of a process that is alive on this machine, nor one that ended', () => {
    const alive = local(100);
    const mine = open('mine', { hostPid: alive.hostPid, hostStart: alive.hostStart }, '/w/a');
    expect(core.sessionForHost({ ...alive, machine: MAC, machineName: 'mac' })).toBeNull();
    expect(store.sessionById(mine.id)?.hostMachine).toBeNull();
    const host = { hostPid: 500, hostStart: 'T500' };
    open('gone', host, '/w/b');
    core.sessionEvent({ event: 'session-end', client: 'claude', cliSessionId: 'gone', host });
    expect(core.sessionForHost({ ...host, machine: MAC, machineName: 'mac' })).toBeNull();
  });
});

describe('two machines that share a host name, with and without a connection', () => {
  const settle = (): void => {
    now += 61 * 1000;
  };

  it('keeps a connected session of one machine and gives the other machine the next handle, over ten alternating events', () => {
    settle();
    const hostA = remote(100, 'boot-a', 'Mac', 5 * 86400);
    const hostB = remote(200, 'boot-b', 'Mac', 2 * 86400, FP_B);
    const a = open('a', hostA, '/Users/a/work/guard');
    core.hostConnected = (h) => h.hostPid === 100;
    const b = open('b', hostB, '/Users/b/work/guard');
    expect([a.handle, b.handle]).toEqual(['claude-guard', 'claude-guard-2']);
    for (let n = 0; n < 5; n += 1) {
      now += 45 * 1000;
      core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'a', host: { ...hostA, machineUptime: 5 * 86400 + n } });
      now += 45 * 1000;
      core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'b', host: { ...hostB, machineUptime: 2 * 86400 + n } });
    }
    expect(store.listSessions().map((r) => [r.cliSessionId, r.handle, r.state]).sort()).toEqual([['a', 'claude-guard', 'busy'], ['b', 'claude-guard-2', 'busy']]);
  });

  it('does not end a connected session for a hostile claim of its label with a new boot id and a tiny uptime', () => {
    settle();
    const a = open('a', remote(100, 'boot-a', 'Mac', 86400), '/Users/a/work/guard');
    core.hostConnected = (h) => h.hostPid === 100;
    now += 10 * 60 * 1000;
    open('hostile', remote(666, 'boot-hostile', 'Mac', 0), '/Users/x/work/guard');
    expect(store.sessionById(a.id)?.state).not.toBe('ended');
  });

  it('does end a connection-less session for the same claim when the claim carries that machine\'s fingerprint and host name: the trust boundary', () => {
    settle();
    const a = open('a', remote(100, 'boot-a', 'Mac', 86400), '/Users/a/work/guard');
    now += 10 * 60 * 1000;
    open('hostile', remote(666, 'boot-hostile', 'Mac', 0, FP_A), '/Users/x/work/guard');
    expect(store.sessionById(a.id)?.state).toBe('ended');
  });

  it('ends nothing on a reboot event inside the startup grace of a broker, and ends the old sessions after it', () => {
    settle();
    const hostOld = remote(100, 'boot-old', 'Mac', 86400);
    const old = open('old', hostOld, '/Users/a/work/guard');
    now += 10 * 60 * 1000;
    const restarted = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, () => now, () => procs, OMEN);
    now += 30 * 1000;
    restarted.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'new-1', host: remote(300, 'boot-new', 'Mac', 0), cwd: '/Users/a/work/other', source: 'startup' });
    expect(store.sessionById(old.id)?.state).not.toBe('ended');
    now += 31 * 1000;
    restarted.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'new-2', host: remote(301, 'boot-new', 'Mac', 0), cwd: '/Users/a/work/third', source: 'startup' });
    expect(store.sessionById(old.id)?.state).toBe('ended');
  });

  it('a real reboot: the old sessions hold no connection, so they end and the handle and groups are back', () => {
    settle();
    const old = open('old', remote(100, 'boot-old', 'Mac', 86400), '/Users/a/work/guard');
    store.createGroup('crew', now);
    store.addMember('crew', old.handle, now);
    now += 10 * 60 * 1000;
    const back = open('back', remote(300, 'boot-new', 'Mac', 120), '/Users/a/work/guard');
    expect(store.sessionById(old.id)?.state).toBe('ended');
    expect(back.handle).toBe(old.handle);
    expect(store.listGroups().find((g) => g.name === 'crew')?.members).toEqual([old.handle]);
  });
});

describe('a session that comes back after another took its handle', () => {
  const settle = (): void => {
    now += 61 * 1000;
  };

  it('never shares a handle: it takes the next free one, and ten alternating events end nobody', () => {
    settle();
    const hostA = remote(100, 'boot-a', 'Mac', 5 * 86400);
    const hostB = remote(200, 'boot-b', 'Mac', 0, FP_A);
    const a = open('a', hostA, '/Users/a/work/guard');
    now += 10 * 60 * 1000;
    // B claims the label with a fresh boot: the connection-less A is ended and B takes the handle.
    const b = open('b', hostB, '/Users/b/work/guard');
    expect(store.sessionById(a.id)?.state).toBe('ended');
    expect(b.handle).toBe(a.handle);
    store.createGroup('crew', now);
    store.addMember('crew', b.handle, now);
    // A speaks again: it comes back under the next free handle, and the handle's queue stays with B.
    now += 10 * 1000;
    const back = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'a', host: { ...hostA, machineUptime: 5 * 86400 + 610 }, cwd: '/Users/a/work/guard' });
    expect(back.id).toBe(a.id);
    expect(back).toMatchObject({ handle: 'claude-guard-2', state: 'busy' });
    expect(store.sessionById(b.id)?.handle).toBe('claude-guard');
    expect(store.listGroups().find((g) => g.name === 'crew')?.members).toEqual(['claude-guard']);
    for (let n = 0; n < 5; n += 1) {
      now += 45 * 1000;
      core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'a', host: { ...hostA, machineUptime: 5 * 86400 + 700 + n * 90 } });
      now += 45 * 1000;
      core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'b', host: { ...hostB, machineUptime: 100 + n * 90 } });
    }
    expect(store.listSessions().map((r) => [r.cliSessionId, r.handle, r.state]).sort()).toEqual([['a', 'claude-guard-2', 'busy'], ['b', 'claude-guard', 'busy']]);
  });

  it('names the handle of the session that comes back from its stored folder, or without a folder at all', () => {
    now += 61 * 1000;
    const hostA = remote(100, 'boot-a', 'Mac', 5 * 86400);
    const hostC = remote(300, 'boot-c', 'Mac', 5 * 86400);
    open('a', hostA, '/Users/a/work/guard');
    core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'nocwd', host: hostC });
    now += 10 * 60 * 1000;
    // A fresh boot claims the label and the first folder's handle, and ends both connection-less sessions.
    open('b', remote(200, 'boot-b', 'Mac', 0, FP_A), '/Users/b/work/guard');
    expect(store.sessionByCliId('claude', 'a')?.state).toBe('ended');
    expect(store.sessionByCliId('claude', 'nocwd')?.state).toBe('ended');
    const backA = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'a', host: hostA });
    expect(backA.handle).toBe('claude-guard-2');
    // Another session without a folder takes the plain handle while it is ended.
    const taker = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'taker', host: remote(400, 'boot-b', 'Mac', 100) });
    expect(taker.handle).toBe('claude');
    const backNoCwd = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'nocwd', host: hostC });
    expect(backNoCwd.handle).toBe('claude-2');
  });

  it('leaves a session that comes back to a free handle where it was', () => {
    const host = remote();
    const first = open('mac', host, '/Users/renzof/work/guard');
    core.sessionEvent({ event: 'session-end', client: 'claude', cliSessionId: 'mac', host });
    const back = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host });
    expect(back).toMatchObject({ id: first.id, handle: 'claude-guard', state: 'busy' });
  });
});

describe('adopting the row of the same process', () => {
  it('does not take the row of another session: the CLI ids differ', () => {
    const host = { hostPid: 500, hostStart: 'T500' };
    const other = open('other', host, '/Users/renzof/work/guard');
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'other', host });
    const mine = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'mine', host: { ...host, machine: MAC, machineName: 'mac' }, cwd: '/Users/renzof/work/guard' });
    expect(mine.id).not.toBe(other.id);
    expect(store.sessionById(other.id)).toMatchObject({ cliSessionId: 'other', hostMachine: null });
    expect(store.listSessions()).toHaveLength(2);
  });

  it('still takes the row of the same session without a CLI id on either side', () => {
    const host = { hostPid: 500, hostStart: 'T500' };
    const noId = core.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: null, host, cwd: '/Users/renzof/work/guard' });
    const upgraded = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: null, host: { ...host, machine: MAC, machineName: 'mac' } });
    expect(upgraded.id).toBe(noId.id);
    expect(store.sessionById(noId.id)).toMatchObject({ hostMachine: MAC, hostLabel: 'mac' });
  });

  it('finds the row of a remote session for a sparse event without a CLI id, and leaves its machine alone', () => {
    const first = open('mac', remote(51234, MAC, 'mac'), '/Users/renzof/work/guard');
    const sparse = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: null, host: { hostPid: 51234, hostStart: 'T51234' } });
    expect(sparse.id).toBe(first.id);
    expect(store.listSessions()).toHaveLength(1);
    expect(store.sessionById(first.id)).toMatchObject({ hostMachine: MAC, hostLabel: 'mac' });
    // Not when a live process of this machine has that pid and start.
    const mine = local(900);
    const remoteTwin = open('twin', { hostPid: 900, hostStart: 'S900', machine: MAC, machineName: 'mac' }, '/Users/renzof/work/other');
    const here = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: null, host: { hostPid: mine.hostPid, hostStart: mine.hostStart }, cwd: '/w/here' });
    expect(here.id).not.toBe(remoteTwin.id);
  });
});

describe('a container on the host of the broker', () => {
  const BOOT = '8e3a9f1b-1c2d-4e5f-8a9b-0c1d2e3f4a5b';
  const HOSTNS = BOOT + ':4026531836';
  const BOXNS = BOOT + ':4026532999';

  it('is another machine: same boot id, another pid namespace, so a process table that does not hold its pids', () => {
    const broker = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, () => now, () => procs, HOSTNS);
    procs.set(100, { ppid: 1, start: 'S100', comm: 'claude' });
    const onHost = broker.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'host', host: { hostPid: 100, hostStart: 'S100', machine: HOSTNS, machineName: 'omen' }, cwd: '/w/g', source: 'startup' });
    const inBox = broker.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'box', host: { hostPid: 7, hostStart: 'S7', machine: BOXNS, machineName: 'box' }, cwd: '/w/g', source: 'startup' });
    expect(onHost).toMatchObject({ hostMachine: null, handle: 'claude-g' });
    expect(inBox).toMatchObject({ hostMachine: BOXNS, handle: 'claude-g-2' });
    // The process table of the host never holds pid 7, yet the box session stays alive (lease).
    now += 5 * 1000;
    expect(broker.claimable('claude-g-2', null)).toBe(false);
    // Two processes of one namespace are one machine.
    const sameBox = broker.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'box2', host: { hostPid: 8, hostStart: 'S8', machine: BOXNS, machineName: 'box' }, cwd: '/w/h' });
    expect(sameBox.hostMachine).toBe(BOXNS);
  });
});

describe('the grace of the reboot rule, to the millisecond', () => {
  it('ends nothing at exactly 60 seconds after the broker started and ends the old sessions 1 ms later', () => {
    const old = open('old', remote(100, 'boot-old', 'Mac', 86400), '/Users/a/work/guard');
    const bornAt = now;
    now = bornAt + 60 * 1000;
    open('new-1', remote(300, 'boot-new', 'Mac', 0), '/Users/a/work/other');
    expect(store.sessionById(old.id)?.state).not.toBe('ended');
    now += 1;
    open('new-2', remote(301, 'boot-new', 'Mac', 0), '/Users/a/work/third');
    expect(store.sessionById(old.id)?.state).toBe('ended');
  });
});

describe('what the review of the uptime version found', () => {
  it('adopts the row of an old client of the same process while its MCP server is still connected', () => {
    const host = { hostPid: 500, hostStart: 'T500' };
    const old = open('old', host, '/Users/renzof/work/guard');
    core.hostConnected = (h) => h.hostPid === 500 && h.hostMachine === null;
    // No CLI id, so the row is found by its host (the adoption path), while the old connection is open.
    const upgraded = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: null, host: { ...host, machine: MAC, machineName: 'mac' } });
    expect(upgraded.id).toBe(old.id);
    expect(store.sessionById(old.id)).toMatchObject({ hostMachine: MAC, hostLabel: 'mac' });
    expect(store.listSessions()).toHaveLength(1);
  });

  it('keeps the label of a session on a machine without one when a sparse event arrives', () => {
    const noLabel = open('nolabel', { hostPid: 61, hostStart: 'T61', machine: MAC }, '/w/a');
    expect(noLabel.hostLabel).toBeNull();
    const sparse = core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'nolabel', host: { hostPid: 61, hostStart: 'T61' } });
    expect(sparse.id).toBe(noLabel.id);
    expect(store.sessionById(noLabel.id)).toMatchObject({ hostMachine: MAC, hostLabel: null });
  });
});

describe('what tells a reboot from a stranger with the same host name: the fingerprint', () => {
  const settle = (): void => {
    now += 61 * 1000;
  };

  it('does not end a quiet machine for another machine with the same host name and a fresh uptime', () => {
    settle();
    const a = open('a', remote(100, 'boot-a', 'Mac', 9 * 86400, FP_A), '/Users/a/work/guard');
    store.createGroup('crew', now);
    store.addMember('crew', a.handle, now);
    core.registerNative({ handle: a.handle, socketPath: '/tmp/a.sock', token: 't' });
    const mail = core.send('target-peer', { threadId: null, to: [a.handle], body: 'waiting for a' });
    now += 2 * 3600 * 1000;
    const b = open('b', remote(200, 'boot-b', 'Mac', 1800, FP_B), '/Users/b/work/guard');
    expect(store.sessionById(a.id)?.state).not.toBe('ended');
    expect(b.handle).toBe('claude-guard-2');
    expect(store.sessionById(a.id)?.handle).toBe('claude-guard');
    expect(store.listGroups().find((g) => g.name === 'crew')?.members).toEqual(['claude-guard']);
    expect(store.getNativeTransport('claude-guard')?.socketPath).toBe('/tmp/a.sock');
    expect(store.getDeliveries(mail.result.messageId).map((d) => [d.recipient, d.state])).toEqual([['claude-guard', 'queued']]);
  });

  it('ends the old sessions of the same machine at once when it comes back with a new boot id', () => {
    settle();
    const old = open('old', remote(100, 'boot-old', 'Mac', 86400, FP_A), '/Users/a/work/guard');
    store.createGroup('crew', now);
    store.addMember('crew', old.handle, now);
    now += 10 * 60 * 1000;
    const back = open('back', remote(300, 'boot-new', 'Mac', 120, FP_A), '/Users/a/work/guard');
    expect(store.sessionById(old.id)?.state).toBe('ended');
    expect(back.handle).toBe(old.handle);
    expect(store.listGroups().find((g) => g.name === 'crew')?.members).toEqual([old.handle]);
  });

  it('ends nothing when the host name differs, or when the fingerprint is missing on the event, on the row, or on both', () => {
    settle();
    const cases: Array<[string, string | null, string, string | null, string]> = [
      // [name of the case, row fingerprint, row label, event fingerprint, event label]
      ['other host name', FP_A, 'Mac', FP_A, 'MacBook'],
      ['no fingerprint on the event', FP_A, 'Mac', null, 'Mac'],
      ['no fingerprint on the row', null, 'Mac', FP_A, 'Mac'],
      ['no fingerprint on either', null, 'Mac', null, 'Mac'],
    ];
    cases.forEach(([name, rowFp, rowLabel, eventFp, eventLabel], n) => {
      const old = open('old-' + name, remote(1000 + n, 'boot-old-' + String(n), rowLabel, 86400, rowFp), '/Users/a/work/old-' + String(n));
      now += 10 * 60 * 1000;
      open('new-' + name, remote(2000 + n, 'boot-new-' + String(n), eventLabel, 0, eventFp), '/Users/a/work/new-' + String(n));
      expect(store.sessionById(old.id)?.state, name).not.toBe('ended');
    });
  });

  it('treats two clones of one image as one machine: the connected one stays, the hook-only one ends', () => {
    settle();
    const connected = open('connected', remote(100, 'boot-1', 'image', 86400, FP_A), '/Users/a/work/guard');
    const hookOnly = open('hook-only', remote(101, 'boot-1', 'image', 86400, FP_A), '/Users/a/work/other');
    core.hostConnected = (h) => h.hostPid === 100;
    now += 10 * 60 * 1000;
    open('clone', remote(300, 'boot-2', 'image', 0, FP_A), '/Users/c/work/third');
    expect(store.sessionById(connected.id)?.state).not.toBe('ended');
    expect(store.sessionById(hookOnly.id)?.state).toBe('ended');
  });

  it('does not end anything for a hostile claim of a label with a fresh boot id and an uptime of zero but another fingerprint', () => {
    settle();
    const a = open('a', remote(100, 'boot-a', 'Mac', 86400, FP_A), '/Users/a/work/guard');
    now += 10 * 60 * 1000;
    open('hostile', remote(666, 'boot-hostile', 'Mac', 0, FP_B), '/Users/x/work/guard');
    expect(store.sessionById(a.id)?.state).not.toBe('ended');
    core.hostConnected = (h) => h.hostPid === 100;
    open('hostile-2', remote(667, 'boot-hostile-2', 'Mac', 0, FP_B), '/Users/x/work/other');
    expect(store.sessionById(a.id)?.state).not.toBe('ended');
  });

  it('keeps the fingerprint of a session when an event carries none for the same machine, and replaces it for another', () => {
    const first = open('mac', remote(51234, MAC, 'mac', undefined, FP_A), '/Users/renzof/work/guard');
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: remote(51234, MAC, 'mac', undefined, null) });
    expect(store.sessionById(first.id)?.hostFingerprint).toBe(FP_A);
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: { hostPid: 51234, hostStart: 'T51234' } });
    expect(store.sessionById(first.id)).toMatchObject({ hostMachine: MAC, hostFingerprint: FP_A });
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: remote(51234, 'another-boot', 'mac', undefined, FP_B) });
    expect(store.sessionById(first.id)).toMatchObject({ hostMachine: 'another-boot', hostFingerprint: FP_B });
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: remote(51234, 'third-boot', 'mac', undefined, null) });
    expect(store.sessionById(first.id)).toMatchObject({ hostMachine: 'third-boot', hostFingerprint: null });
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'mac', host: { hostPid: 51234, hostStart: 'T51234', machine: OMEN } });
    expect(store.sessionById(first.id)).toMatchObject({ hostMachine: null, hostFingerprint: null });
  });
});

describe('a database made by the code before host_fingerprint', () => {
  it('gains the column, keeps its rows with no fingerprint, and ends nothing on a reboot claim until a fingerprint is known', () => {
    const copy = join(dir, 'before.db');
    copyFileSync(join(import.meta.dirname, 'fixtures', 'murmur-dfeeec7-schema.db'), copy);
    const before = new Database(copy, { readonly: true });
    const names = (before.prepare('PRAGMA table_info(sessions)').all() as Array<{ name: string }>).map((c) => c.name);
    expect(names).toContain('host_label');
    expect(names).not.toContain('host_fingerprint');
    before.close();
    const old = Store.open(copy);
    const row = old.sessionByCliId('claude', 'remote-1');
    expect(row).toMatchObject({ handle: 'claude-guard', hostMachine: '0a000000-0000-4000-8000-000000000009', hostLabel: 'Mac', hostFingerprint: null });
    expect(old.threadMessages(1, null)).toHaveLength(1);
    const migrated = new MurmurCore(old, { onAppend: () => {}, onStateChange: () => {} }, () => 1_700_000_000_000 + 2 * 3600 * 1000, () => procs, OMEN);
    // The core started now, so wait out its grace; the old row has no fingerprint, so nothing ends.
    const later = new MurmurCore(old, { onAppend: () => {}, onStateChange: () => {} }, (() => { let t = 1_700_000_000_000 + 2 * 3600 * 1000; return () => (t += 61_000); })(), () => procs, OMEN);
    later.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'new', host: remote(300, 'boot-new', 'Mac', 0, FP_A), cwd: '/Users/b/work/other', source: 'startup' });
    expect(old.sessionByCliId('claude', 'remote-1')?.state).not.toBe('ended');
    // The same session speaks again with a fingerprint: it is stored.
    migrated.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'remote-1', host: { hostPid: 4242, hostStart: 'Mon Nov  9 10:00:00 2026', machine: '0a000000-0000-4000-8000-000000000009', machineName: 'Mac', machineFingerprint: FP_A } });
    expect(old.sessionByCliId('claude', 'remote-1')?.hostFingerprint).toBe(FP_A);
    old.close();
    const again = Store.open(copy);
    expect(again.sessionByCliId('claude', 'remote-1')?.hostFingerprint).toBe(FP_A);
    again.close();
  });
});
