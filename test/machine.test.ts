import { describe, expect, it } from 'vitest';
import { SYSCTL_OPTIONS, machineId, machineOf, machineRef, parseBootId, readBootId, readUptime, thisMachine, thisMachineRef, validField, validUptime } from '../src/machine.js';

const SAMPLE = '8E3A9F1B-1C2D-4E5F-8A9B-0C1D2E3F4A5B';

describe('parseBootId', () => {
  it('reads a boot uuid from the Linux file and from macOS sysctl output, lower case', () => {
    expect(parseBootId(SAMPLE + '\n')).toBe(SAMPLE.toLowerCase());
    expect(parseBootId('kern.bootsessionuuid: ' + SAMPLE + '\n')).toBe(SAMPLE.toLowerCase());
  });

  it('reads nothing from text without a uuid', () => {
    for (const text of ['', '\n', 'not a uuid', '8e3a9f1b-1c2d-4e5f-8a9b']) {
      expect(parseBootId(text)).toBe('');
    }
  });
});

describe('readBootId', () => {
  it('reads /proc on Linux, /usr/sbin/sysctl on macOS, and nothing elsewhere or on failure', () => {
    const files: string[] = [];
    const cmds: string[][] = [];
    const readFile = (path: string): string => {
      files.push(path);
      return SAMPLE + '\n';
    };
    const run = (cmd: string, args: string[]): string => {
      cmds.push([cmd, ...args]);
      return SAMPLE + '\n';
    };
    expect(readBootId('linux', readFile, run)).toBe(SAMPLE.toLowerCase());
    expect(files).toEqual(['/proc/sys/kernel/random/boot_id']);
    expect(readBootId('darwin', readFile, run)).toBe(SAMPLE.toLowerCase());
    expect(cmds).toEqual([['/usr/sbin/sysctl', '-n', 'kern.bootsessionuuid']]);
    expect(readBootId('win32', readFile, run)).toBe('');
    const boom = (): string => {
      throw new Error('no such file');
    };
    expect(readBootId('linux', boom, run)).toBe('');
    expect(readBootId('darwin', readFile, boom)).toBe('');
  });

  it('asks the system, and reads a uuid or nothing', () => {
    expect(readBootId()).toMatch(/^([0-9a-f-]{36})?$/);
    expect(readBootId('darwin')).toMatch(/^([0-9a-f-]{36})?$/);
  });
});

describe('machineId', () => {
  it('is the boot id whatever the host name is, and empty without one', () => {
    expect(machineId('omen', SAMPLE.toLowerCase())).toBe(SAMPLE.toLowerCase());
    expect(machineId('omen-renamed', SAMPLE.toLowerCase())).toBe(SAMPLE.toLowerCase());
    expect(machineId('omen', '')).toBe('');
    expect(machineId()).toMatch(/^([0-9a-f-]{36})?$/);
  });
});

describe('machineRef', () => {
  it('is the boot id with the host name as a label, and nothing at all without a boot id', () => {
    expect(machineRef(SAMPLE.toLowerCase(), 'omen')).toEqual({ machine: SAMPLE.toLowerCase(), machineName: 'omen' });
    expect(machineRef(SAMPLE.toLowerCase(), 'omen-renamed').machine).toBe(SAMPLE.toLowerCase());
    expect(machineRef('', 'omen')).toEqual({});
    expect(thisMachineRef()).toEqual(thisMachineRef());
    expect(thisMachine()).toBe(machineOf(thisMachineRef()));
    expect(machineOf({})).toBe('');
    expect(machineOf({ machine: 'x' })).toBe('x');
  });
});

describe('validField', () => {
  it('takes 1 to 255 printable characters without a line break, and nothing else', () => {
    expect(validField('renn-omen')).toBe('renn-omen');
    expect(validField('x'.repeat(255))).toBe('x'.repeat(255));
    for (const bad of ['', 'x'.repeat(256), 'a\nb', 'a\u0000b', 'a\u007fb', 12, null, undefined, {}]) {
      expect(validField(bad)).toBeUndefined();
    }
  });
});

describe('readUptime', () => {
  it('reads /proc/uptime on Linux and the boot time sysctl prints on macOS, and nothing else', () => {
    expect(readUptime('linux', () => '12345.67 54321.00\n')).toBe(12345);
    expect(readUptime('linux', () => 'garbage')).toBeUndefined();
    expect(readUptime('linux', () => '-4.0 1.0')).toBeUndefined();
    const sysctl = (): string => '{ sec = 1700000000, usec = 123456 } Tue Nov 14 22:13:20 2023\n';
    expect(readUptime('darwin', () => '', sysctl, 1700000600_000)).toBe(600);
    expect(readUptime('darwin', () => '', sysctl, 1600000000_000)).toBe(0);
    expect(readUptime('darwin', () => '', () => 'nothing here')).toBeUndefined();
    expect(readUptime('win32')).toBeUndefined();
    const boom = (): string => {
      throw new Error('no');
    };
    expect(readUptime('linux', boom)).toBeUndefined();
    expect(readUptime('darwin', () => '', boom)).toBeUndefined();
    expect(readUptime()).toEqual(expect.any(Number));
    expect(readUptime('darwin')).toBeUndefined();
  });

  it('is kept to finite seconds of at most ten years where it enters the broker', () => {
    expect(validUptime(0)).toBe(0);
    expect(validUptime(86400)).toBe(86400);
    for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, 315_360_001, '5', null, undefined]) {
      expect(validUptime(bad)).toBeUndefined();
    }
  });
});

describe('the sysctl call', () => {
  it('is bounded to one second and never writes to the terminal', () => {
    expect(SYSCTL_OPTIONS.timeout).toBe(1000);
    expect(SYSCTL_OPTIONS.stdio).toEqual(['ignore', 'pipe', 'ignore']);
  });
});
