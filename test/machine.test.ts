import { describe, expect, it } from 'vitest';
import { machineId, parseBootId, readBootId } from '../src/machine.js';

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
  it('reads /proc on Linux, sysctl on macOS, and nothing elsewhere or on failure', () => {
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
    expect(cmds).toEqual([['sysctl', '-n', 'kern.bootsessionuuid']]);
    expect(readBootId('win32', readFile, run)).toBe('');
    const boom = (): string => {
      throw new Error('no such file');
    };
    expect(readBootId('linux', boom, run)).toBe('');
    expect(readBootId('darwin', readFile, boom)).toBe('');
  });
});

describe('readBootId with the real readers', () => {
  it('asks the system, and reads a uuid or nothing', () => {
    expect(readBootId()).toMatch(/^([0-9a-f-]{36})?$/);
    expect(readBootId('darwin')).toMatch(/^([0-9a-f-]{36})?$/);
  });
});

describe('machineId', () => {
  it('is the boot id, and falls back to the host name when there is none', () => {
    expect(machineId('omen', SAMPLE.toLowerCase())).toBe(SAMPLE.toLowerCase());
    expect(machineId('omen-renamed', SAMPLE.toLowerCase())).toBe(SAMPLE.toLowerCase());
    expect(machineId('omen', '')).toBe('omen');
    expect(machineId()).toMatch(/\S/);
  });
});
