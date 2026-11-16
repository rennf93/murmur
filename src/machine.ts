import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { hostname } from 'node:os';

// A host reference is a pid and a start time, which only mean something on the
// machine that ran the process. `machine` names that machine and rides along
// with every host reference: the boot id, a uuid per machine and boot (a
// rebooted machine or a clone is another machine). `machineName`, the host
// name, is only a label: it is never compared for sameness (a host name can
// change while a machine runs), and only tells the broker that a machine it
// knew rebooted. A client that cannot read a boot id sends neither field and
// is judged as one of the broker's own machine, as before this existed.

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
const UUID_FULL = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// What a machine field may hold where it enters the broker.
const FIELD = /^[^\u0000-\u001f\u007f]{1,255}$/;

// The boot uuid inside Linux's /proc/sys/kernel/random/boot_id or macOS's
// `sysctl -n kern.bootsessionuuid` (also with its `name: ` prefix), lower
// case; empty when the text holds none.
export function parseBootId(text: string): string {
  return UUID.exec(text)?.[0].toLowerCase() ?? '';
}

// Linux reads /proc, macOS asks sysctl by absolute path with a one second
// limit (the one thing to confirm on a Mac: `sysctl -n kern.bootsessionuuid`
// prints a uuid). Anything else, or a failure, has no boot id.
export const SYSCTL_OPTIONS: { encoding: 'utf8'; timeout: number; stdio: Array<'ignore' | 'pipe'> } = { encoding: 'utf8', timeout: 1000, stdio: ['ignore', 'pipe', 'ignore'] };

const defaultRead = (path: string): string => readFileSync(path, 'utf8');
const defaultRun = (cmd: string, args: string[]): string => execFileSync(cmd, args, SYSCTL_OPTIONS);

export function readBootId(
  platform: string = process.platform,
  readFile: (path: string) => string = defaultRead,
  run: (cmd: string, args: string[]) => string = defaultRun,
): string {
  try {
    if (platform === 'linux') {
      return parseBootId(readFile('/proc/sys/kernel/random/boot_id'));
    }
    if (platform === 'darwin') {
      return parseBootId(run('/usr/sbin/sysctl', ['-n', 'kern.bootsessionuuid']));
    }
  } catch {
    // No boot id.
  }
  return '';
}

// The machine id: the boot id, or '' when there is none. The host name is not
// part of it (it is only the label of machineRef), so it is not consulted.
export function machineId(_name: string = hostname(), bootId: string = readBootId()): string {
  return bootId;
}

// Seconds since this machine booted: /proc/uptime on Linux, the boot time that
// `sysctl -n kern.boottime` prints (`{ sec = 1700000000, usec = 0 } ...`) on
// macOS; undefined when it cannot be read. Sent with a session event so the
// broker can tell that a machine booted after the last time it heard from the
// old sessions it holds under the same host name (src/core.ts).
export function readUptime(
  platform: string = process.platform,
  readFile: (path: string) => string = defaultRead,
  run: (cmd: string, args: string[]) => string = defaultRun,
  nowMs: number = Date.now(),
): number | undefined {
  try {
    if (platform === 'linux') {
      const seconds = Number.parseFloat(readFile('/proc/uptime'));
      return Number.isFinite(seconds) && seconds >= 0 ? Math.floor(seconds) : undefined;
    }
    if (platform === 'darwin') {
      const booted = /sec\s*=\s*(\d+)/.exec(run('/usr/sbin/sysctl', ['-n', 'kern.boottime']))?.[1];
      return booted === undefined ? undefined : Math.max(0, Math.floor(nowMs / 1000) - Number(booted));
    }
  } catch {
    // No uptime.
  }
  return undefined;
}

// A machine uptime from the wire: finite seconds, 0 to 10 years.
export function validUptime(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 315_360_000 ? value : undefined;
}

export interface MachineRef {
  machine?: string;
  machineName?: string;
}

// What a client puts next to its host reference: the boot id and the host
// name, or nothing when there is no boot id.
export function machineRef(bootId: string = readBootId(), name: string = hostname()): MachineRef {
  return bootId === '' ? {} : { machine: bootId, machineName: name };
}

let cached: MachineRef | undefined;

// This machine's reference, read once.
export function thisMachineRef(): MachineRef {
  cached ??= machineRef();
  return cached;
}

// The boot id of a reference, or '' when it has none.
export function machineOf(ref: MachineRef): string {
  return ref.machine ?? '';
}

// This machine's boot id, or '' when it has none.
export function thisMachine(): string {
  return machineOf(thisMachineRef());
}

// A boot id from the wire: a uuid, lower case; undefined for anything else, so
// one machine is one value however a client writes it.
export function validBootId(value: unknown): string | undefined {
  return typeof value === 'string' && UUID_FULL.test(value) ? value.toLowerCase() : undefined;
}

// A machine field from the wire: undefined unless it is 1 to 255 printable
// characters without a line break.
export function validField(value: unknown): string | undefined {
  return typeof value === 'string' && FIELD.test(value) ? value : undefined;
}
