import { describe, expect, it } from 'vitest';
import { currentHost, hostOf, isAlive, isShell, liveTable, readProcTable, readProcess } from '../src/proc.js';

const L = (d: string): string => d.padEnd(24);
const row = (pid: number, ppid: number, start: string, comm: string): string =>
  String(pid).padStart(5) + ' ' + String(ppid).padStart(5) + ' ' + L(start) + ' ' + comm;

const T1 = 'Tue Oct 20 06:00:00 2026';
const TEXT = [
  'garbage line',
  row(1, 0, T1, '/sbin/launchd'),
  row(100, 1, T1, '/Applications/Visual Studio Code.app/Contents/MacOS/Electron'),
  row(200, 100, T1, '-zsh'),
  row(300, 200, T1, '/opt/homebrew/bin/node'),
  row(400, 300, T1, '/bin/bash'),
  row(500, 400, T1, 'node'),
].join('\n');

describe('readProcTable', () => {
  it('parses ps text, keeping comm paths with spaces and skipping junk', () => {
    const table = readProcTable(() => TEXT);
    expect(table.size).toBe(6);
    expect(table.get(100)).toEqual({
      ppid: 1,
      start: T1,
      comm: '/Applications/Visual Studio Code.app/Contents/MacOS/Electron',
    });
    expect(table.get(200)?.comm).toBe('-zsh');
  });

  it('runs the real ps by default and finds this process', () => {
    expect(readProcTable().get(process.pid)?.ppid).toBe(process.ppid);
  });

  it('reads a listing without the comm column', () => {
    const table = readProcTable(() => String(7).padStart(5) + ' ' + String(1).padStart(5) + ' ' + L(T1));
    expect(table.get(7)).toEqual({ ppid: 1, start: T1, comm: '' });
  });
});

describe('readProcess', () => {
  it('asks ps for one pid and finds this process with the real ps', () => {
    expect(readProcess(process.pid)?.ppid).toBe(process.ppid);
  });

  it('is undefined for a missing pid or a failing ps', () => {
    expect(readProcess(1234, () => '')).toBeUndefined();
    expect(
      readProcess(1234, () => {
        throw new Error('no ps');
      }),
    ).toBeUndefined();
  });
});

describe('liveTable', () => {
  const one = (pid: number): string => (pid === 100 || pid === 200 ? row(pid, 1, T1, 'node') : '');

  it('asks ps once per distinct pid for a few pids and skips the dead ones', () => {
    const asked: number[] = [];
    const table = liveTable([100, 100, 200, 300], (pid) => {
      asked.push(pid);
      return one(pid);
    });
    expect(asked).toEqual([100, 200, 300]);
    expect([...new Map(table as Map<number, unknown>).keys()]).toEqual([100, 200]);
  });

  it('asks nothing for no pids', () => {
    expect((liveTable([], () => 'x') as Map<number, unknown>).size).toBe(0);
  });

  it('reads one listing past a few pids, and an empty table when ps fails', () => {
    const pids = [1, 2, 3, 4, 5];
    let listings = 0;
    const table = liveTable(pids, undefined, () => {
      listings += 1;
      return TEXT;
    });
    expect(listings).toBe(1);
    expect(isAlive(table, 100, T1)).toBe(true);
    const failed = liveTable(pids, undefined, () => {
      throw new Error('no ps');
    });
    expect(failed.get(100)).toBeUndefined();
  });

  it('runs the real ps in both modes and finds this process', () => {
    expect(liveTable([process.pid]).get(process.pid)?.ppid).toBe(process.ppid);
    expect(liveTable([process.pid, 1, 2, 3, 4]).get(process.pid)?.start).toBe(readProcess(process.pid)?.start);
  });
});

describe('isShell', () => {
  it('recognizes shells by basename, including the login form', () => {
    expect(isShell('/bin/zsh')).toBe(true);
    expect(isShell('-zsh')).toBe(true);
    expect(isShell('fish')).toBe(true);
    expect(isShell('/usr/bin/node')).toBe(false);
  });
});

describe('hostOf', () => {
  const table = readProcTable(() => TEXT);

  it('picks the first non-shell ancestor and lists all ancestors nearest first', () => {
    expect(hostOf(table, 500)).toEqual({ hostPid: 300, hostStart: T1, ancestors: [400, 300, 200, 100] });
  });

  it('returns null when the chain ends without a non-shell', () => {
    const t = readProcTable(() => [row(10, 1, T1, 'bash'), row(11, 10, T1, '-zsh'), row(12, 11, T1, 'node')].join('\n'));
    expect(hostOf(t, 12)).toBeNull();
  });

  it('stops at a missing parent and returns null for an unknown start pid', () => {
    const t = readProcTable(() => [row(11, 50, T1, 'bash'), row(12, 11, T1, 'node')].join('\n'));
    expect(hostOf(t, 12)).toBeNull();
    expect(hostOf(t, 999)).toBeNull();
  });

  it('stops at the host when asked not to walk the whole chain', () => {
    expect(hostOf(table, 500, false)).toEqual({ hostPid: 300, hostStart: T1, ancestors: [400, 300] });
  });

  it('caps the walk depth', () => {
    const lines = [];
    for (let pid = 10; pid < 60; pid++) {
      lines.push(row(pid, pid - 1, T1, 'sh'));
    }
    const t = readProcTable(() => lines.join('\n'));
    expect(hostOf(t, 59)).toBeNull();
    t.set(40, { ppid: 39, start: T1, comm: 'node' });
    expect(hostOf(t, 59)?.ancestors).toHaveLength(32);
  });
});

describe('currentHost', () => {
  const rows = new Map<number, string>([
    [process.ppid, row(process.ppid, 700, T1, 'zsh')],
    [700, row(700, 600, T1, 'claude')],
    [600, row(600, 1, T1, 'launchd')],
  ]);

  it('asks ps for one ancestor at a time and never for this process', () => {
    const asked: number[] = [];
    const run = (pid: number): string => {
      asked.push(pid);
      return rows.get(pid) ?? '';
    };
    expect(currentHost(run)).toEqual({ hostPid: 700, hostStart: T1, ancestors: [process.ppid, 700, 600] });
    expect(asked).toEqual([process.ppid, 700, 600]);
  });

  it('stops at the host when only the host is needed', () => {
    const asked: number[] = [];
    const run = (pid: number): string => {
      asked.push(pid);
      return rows.get(pid) ?? '';
    };
    expect(currentHost(run, false)).toEqual({ hostPid: 700, hostStart: T1, ancestors: [process.ppid, 700] });
    expect(asked).toEqual([process.ppid, 700]);
  });

  it('returns null when the runner throws', () => {
    expect(
      currentHost(() => {
        throw new Error('no ps');
      }),
    ).toBeNull();
  });
});

describe('isAlive', () => {
  it('needs the same pid and start time', () => {
    const table = readProcTable(() => TEXT);
    expect(isAlive(table, 100, T1)).toBe(true);
    expect(isAlive(table, 100, 'other')).toBe(false);
    expect(isAlive(table, 9999, T1)).toBe(false);
  });
});
