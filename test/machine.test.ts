import { describe, expect, it } from 'vitest';
import { SYSCTL_OPTIONS, machineId, once, onceOk, parseWireMachine, readFingerprint, readPidNamespace, validFingerprint, validBootId, machineOf, machineRef, parseBootId, readBootId, readUptime, thisMachine, thisMachineRef, validField, validUptime } from '../src/machine.js';

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
    expect(machineRef(SAMPLE.toLowerCase(), 'omen', '', null)).toEqual({ machine: SAMPLE.toLowerCase(), machineName: 'omen' });
    expect(machineRef(SAMPLE.toLowerCase(), 'omen-renamed', '').machine).toBe(SAMPLE.toLowerCase());
    expect(machineRef('', 'omen', '4026531836', 'f'.repeat(32))).toEqual({});
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
    expect(validUptime(315_360_000)).toBe(315_360_000);
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

describe('the pid namespace in the machine', () => {
  it('names the process table: the boot id plus the namespace on Linux, the bare boot id when there is none to read', () => {
    expect(machineRef(SAMPLE.toLowerCase(), 'omen', '4026531836', 'f'.repeat(32))).toEqual({ machine: SAMPLE.toLowerCase() + ':4026531836', machineName: 'omen', machineFingerprint: 'f'.repeat(32) });
    expect(machineRef(SAMPLE.toLowerCase(), 'omen', '4026532999').machine).not.toBe(machineRef(SAMPLE.toLowerCase(), 'omen', '4026531836').machine);
    expect(readPidNamespace('linux', () => 'pid:[4026531836]')).toBe('4026531836');
    expect(readPidNamespace('linux', () => 'garbage')).toBe('');
    expect(readPidNamespace('darwin', () => 'pid:[4026531836]')).toBe('');
    expect(readPidNamespace('linux', () => {
      throw new Error('no access');
    })).toBe('');
    expect(readPidNamespace()).toMatch(/^\d*$/);
    expect(machineRef().machine).toMatch(/^([0-9a-f-]{36}(:\d+)?)?$/);
  });
});

describe('once', () => {
  it('keeps the first read: a later failing read does not change what a process said before', () => {
    let reads = 0;
    const read = once(() => {
      reads += 1;
      return reads === 1 ? { machine: SAMPLE.toLowerCase() } : {};
    });
    expect(read()).toEqual({ machine: SAMPLE.toLowerCase() });
    expect(read()).toEqual({ machine: SAMPLE.toLowerCase() });
    expect(reads).toBe(1);
    expect(thisMachineRef()).toBe(thisMachineRef());
  });
});

describe('validBootId', () => {
  it('takes a uuid in either case as one lower case value, and nothing else', () => {
    expect(validBootId('BBBBBBBB-1C2D-4E5F-8A9B-0C1D2E3F4A5B')).toBe('bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b');
    expect(validBootId('bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b')).toBe('bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b');
    expect(validBootId('BBBBBBBB-1C2D-4E5F-8A9B-0C1D2E3F4A5B:4026531836')).toBe('bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b:4026531836');
    expect(validBootId('bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b:' + '9'.repeat(20))).toBe('bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b:' + '9'.repeat(20));
    for (const bad of ['bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b:', 'bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b:' + '9'.repeat(21), 'bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b:12x', 'bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b::1', '', 'mac-boot', 'omen', 'bbbbbbbb-1c2d-4e5f-8a9b', 'x bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b', 'bbbbbbbb-1c2d-4e5f-8a9b-0c1d2e3f4a5b\n', 12, null, undefined]) {
      expect(validBootId(bad)).toBeUndefined();
    }
  });
});

describe('onceOk', () => {
  it('keeps a read once it has worked, and tries a failed read again at most once per interval', () => {
    let clock = 1000;
    let reads = 0;
    const answers = [{}, {}, { machine: 'x' }, {}];
    const read = onceOk(() => answers[Math.min(reads++, answers.length - 1)] as { machine?: string }, (v) => v.machine !== undefined, 60_000, () => clock);
    expect(read()).toEqual({});
    clock += 59_999;
    expect(read()).toEqual({});
    expect(reads).toBe(1);
    clock += 1;
    expect(read()).toEqual({});
    clock += 60_000;
    expect(read()).toEqual({ machine: 'x' });
    clock += 600_000;
    expect(read()).toEqual({ machine: 'x' });
    expect(reads).toBe(3);
  });
});

describe('readFingerprint', () => {
  const IOREG = [
    '+-o Root  <class IORegistryEntry, id 0x100000100, retain 25>',
    '  +-o MacBookPro18,3  <class IOPlatformExpertDevice, id 0x10000020f, registered, matched, active, busy 0 (143 ms), retain 40>',
    '    {',
    '      "IOPlatformSerialNumber" = "C02XXXXXXXXX"',
    '      "IOPlatformUUID" = "8E3A9F1B-1C2D-4E5F-8A9B-0C1D2E3F4A5B"',
    '      "manufacturer" = <"Apple Inc.">',
    '    }',
    '',
  ].join('\n');

  it('hashes the machine id on Linux, with the dbus copy as a fallback, and never carries the raw id', () => {
    const fp = readFingerprint('linux', (path) => (path === '/etc/machine-id' ? '0123456789abcdef0123456789abcdef\n' : ''));
    expect(fp).toBe('4b8ca916283f918e312b5785d4ad2041');
    expect(fp).not.toContain('0123456789abcdef');
    const missing = (path: string): string => {
      if (path === '/etc/machine-id') {
        throw new Error('no such file');
      }
      return '0123456789abcdef0123456789abcdef';
    };
    expect(readFingerprint('linux', missing)).toBe('4b8ca916283f918e312b5785d4ad2041');
    expect(readFingerprint('linux', () => '  \n')).toBeUndefined();
    expect(readFingerprint('linux', () => {
      throw new Error('no access');
    })).toBeUndefined();
  });

  it('reads the IOPlatformUUID that ioreg prints on macOS, by absolute path', () => {
    const calls: string[][] = [];
    const fp = readFingerprint('darwin', () => '', (cmd, args) => {
      calls.push([cmd, ...args]);
      return IOREG;
    });
    expect(fp).toBe('0f01ad4d807908a30d19031969a1aa7c');
    expect(calls).toEqual([['/usr/sbin/ioreg', '-rd1', '-c', 'IOPlatformExpertDevice']]);
    expect(readFingerprint('darwin', () => '', () => 'no uuid here')).toBeUndefined();
    expect(readFingerprint('darwin', () => '', () => {
      throw new Error('timeout');
    })).toBeUndefined();
    expect(readFingerprint('win32')).toBeUndefined();
    expect(readFingerprint()).toMatch(/^([0-9a-f]{32})?$/);
    expect(readFingerprint('darwin')).toBeUndefined();
  });
});

describe('the fingerprint on the wire', () => {
  const FP = 'abcdef0123456789abcdef0123456789';
  const BOOT = '8e3a9f1b-1c2d-4e5f-8a9b-0c1d2e3f4a5b';

  it('is 32 lower case hex characters, and nothing else', () => {
    expect(validFingerprint(FP)).toBe(FP);
    for (const bad of [FP.toUpperCase(), FP.slice(1), FP + '0', 'g' + FP.slice(1), '', 12, null, undefined]) {
      expect(validFingerprint(bad)).toBeUndefined();
    }
  });

  it('is taken only next to a machine, together with the label and the uptime', () => {
    expect(parseWireMachine({ machine: BOOT.toUpperCase(), machineName: 'mac', machineUptime: 90, machineFingerprint: FP })).toEqual({
      machine: BOOT, machineName: 'mac', machineUptime: 90, machineFingerprint: FP,
    });
    expect(parseWireMachine({ machineName: 'mac', machineUptime: 90, machineFingerprint: FP })).toEqual({});
    expect(parseWireMachine({ machine: 'not-a-uuid', machineFingerprint: FP })).toEqual({});
    expect(parseWireMachine({ machine: BOOT, machineName: 'a\nb', machineUptime: -1, machineFingerprint: FP.toUpperCase() })).toEqual({ machine: BOOT });
    expect(parseWireMachine({ machine: BOOT, machineFingerprint: FP + '0' })).toEqual({ machine: BOOT });
  });
});
