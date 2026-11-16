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
const OMEN = 'omen-boot-1';
const MAC = 'mac-boot-9';
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
const remote = (pid = 51234, boot = MAC, name = 'mac', uptime?: number) => ({ hostPid: pid, hostStart: 'T' + String(pid), machine: boot, machineName: name, ...(uptime !== undefined ? { machineUptime: uptime } : {}) });
type Host = { hostPid: number; hostStart: string; machine?: string; machineName?: string; machineUptime?: number };
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

  it('reads every client as its own machine while the broker has no boot id', () => {
    const noBoot = new MurmurCore(store, { onAppend: () => {}, onStateChange: () => {} }, () => now, () => procs, '');
    const row = noBoot.sessionEvent({ event: 'session-start', client: 'claude', cliSessionId: 'x', host: remote(), cwd: '/w/guard', source: 'startup' });
    expect(row).toMatchObject({ hostMachine: null, hostLabel: null });
    expect(noBoot.claimable('claude-guard', null)).toBe(true);
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
    const b = open('b', remote(200, 'boot-b', 'Mac', 3 * 86400), '/Users/b/work/guard');
    expect(store.sessionById(a.id)?.state).not.toBe('ended');
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'a', host: remote(100, 'boot-a', 'Mac', 3 * 86400 + 5) });
    expect(store.sessionById(b.id)?.state).not.toBe('ended');
    now += 40 * 1000;
    core.sessionEvent({ event: 'prompt', client: 'claude', cliSessionId: 'b', host: remote(200, 'boot-b', 'Mac', 3 * 86400 + 45) });
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
