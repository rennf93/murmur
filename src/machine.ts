import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync, readlinkSync } from 'node:fs';
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
// A machine from the wire: a boot id, optionally followed by `:` and the number
// of its pid namespace (Linux).
const MACHINE_FULL = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(:[0-9]{1,20})?$/i;
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

// The number inside `/proc/self/ns/pid` (`pid:[4026531836]`): the pid namespace
// of this process, empty when there is none to read (not Linux, or no access).
// A process table is only one machine's if its processes share this number.
export function readPidNamespace(
  platform: string = process.platform,
  readLink: (path: string) => string = (path) => readlinkSync(path),
): string {
  try {
    return platform === 'linux' ? (/\[(\d+)\]/.exec(readLink('/proc/self/ns/pid'))?.[1] ?? '') : '';
  } catch {
    return '';
  }
}

// What tells one machine from a lookalike across reboots: a hash of the id the
// system keeps for itself (Linux `/etc/machine-id`, else
// `/var/lib/dbus/machine-id`; macOS the IOPlatformUUID `ioreg` prints). The raw
// id never leaves the machine: the value is the first 32 hex characters of
// sha256("murmur-machine:" + id). Unreadable or empty: undefined, and the
// broker then never reads a boot id change as a reboot.
// The id systemd keeps in /etc/machine-id: 32 lower case hex characters. A
// placeholder (`uninitialized`) or one baked into an image names no machine.
const SYSTEM_ID = /^[0-9a-f]{32}$/;
const IOREG_UUID = /"IOPlatformUUID"\s*=\s*"([0-9a-f-]{36})"/i;

export function readFingerprint(
  platform: string = process.platform,
  readFile: (path: string) => string = defaultRead,
  run: (cmd: string, args: string[]) => string = defaultRun,
): string | undefined {
  const attempt = (read: () => string): string => {
    try {
      return read();
    } catch {
      return '';
    }
  };
  let raw = '';
  if (platform === 'linux') {
    raw = attempt(() => readFile('/etc/machine-id').trim());
    raw = SYSTEM_ID.test(raw) ? raw : attempt(() => readFile('/var/lib/dbus/machine-id').trim());
    raw = SYSTEM_ID.test(raw) ? raw : '';
  } else if (platform === 'darwin') {
    raw = attempt(() => IOREG_UUID.exec(run('/usr/sbin/ioreg', ['-rd1', '-c', 'IOPlatformExpertDevice']))?.[1] ?? '');
  }
  return raw === '' ? undefined : createHash('sha256').update('murmur-machine:' + raw).digest('hex').slice(0, 32);
}

export interface MachineRef {
  machine?: string;
  machineName?: string;
  // See readFingerprint; sent only together with a machine.
  machineFingerprint?: string;
}

// What a client puts next to its host reference: the boot id and the host
// name, or nothing when there is no boot id.
export function machineRef(
  bootId: string = readBootId(),
  name: string = hostname(),
  pidNamespace: string = readPidNamespace(),
  fingerprint: () => string | null | undefined = readFingerprint,
): MachineRef {
  if (bootId === '') {
    return {};
  }
  const read = fingerprint();
  return {
    machine: pidNamespace === '' ? bootId : bootId + ':' + pidNamespace,
    machineName: name,
    ...(read != null ? { machineFingerprint: read } : {}),
  };
}

// A read that happens once: a later failing read cannot change what a process
// said before.
export function once<T>(read: () => T): () => T {
  let value: { v: T } | undefined;
  return () => {
    value ??= { v: read() };
    return value.v;
  };
}

// A read kept once it has worked: a failed read is tried again, at most once
// per `retryMs`, so a boot id that could not be read at start (a sysctl that
// timed out) is picked up later, while a good value is never replaced.
export function onceOk<T>(read: () => T, ok: (value: T) => boolean, retryMs: number, now: () => number = Date.now): () => T {
  let good: { v: T } | undefined;
  let last: { v: T; at: number } | undefined;
  return () => {
    if (good !== undefined) {
      return good.v;
    }
    if (last === undefined || now() - last.at >= retryMs) {
      const v = read();
      last = { v, at: now() };
      if (ok(v)) {
        good = { v };
      }
    }
    return (last as { v: T }).v;
  };
}

// This machine's reference: read once it has worked, tried again every minute
// while it has not.
export function machineReader(read: () => MachineRef = machineRef, now: () => number = Date.now): () => MachineRef {
  return onceOk(read, (ref) => ref.machine !== undefined, 60_000, now);
}

export const thisMachineRef: () => MachineRef = machineReader();

// The machine of a reference (boot id, with the pid namespace on Linux), or ''
// when it has none.
export function machineOf(ref: MachineRef): string {
  return ref.machine ?? '';
}

// This machine's id, or '' when it has none.
export function thisMachine(): string {
  return machineOf(thisMachineRef());
}

// A machine from the wire: a uuid, lower case, optionally followed by `:` and
// 1 to 20 digits; undefined for anything else, so one machine is one value
// however a client writes it.
export function validBootId(value: unknown): string | undefined {
  return typeof value === 'string' && MACHINE_FULL.test(value) ? value.toLowerCase() : undefined;
}

// A fingerprint from the wire: 32 lower case hex characters, else undefined.
export function validFingerprint(value: unknown): string | undefined {
  return typeof value === 'string' && /^[0-9a-f]{32}$/.test(value) ? value : undefined;
}

// What a session event may say about its machine, from the wire: the machine
// (a boot id, lower case), and, next to it only, the host name as a label, the
// uptime and the fingerprint. Anything that is not of its shape reads as absent.
export function parseWireMachine(rec: Record<string, unknown>): { machine?: string; machineName?: string; machineUptime?: number; machineFingerprint?: string } {
  const machine = validBootId(rec.machine);
  if (machine === undefined) {
    return {};
  }
  const machineName = validField(rec.machineName);
  const machineUptime = validUptime(rec.machineUptime);
  const machineFingerprint = validFingerprint(rec.machineFingerprint);
  return {
    machine,
    ...(machineName !== undefined ? { machineName } : {}),
    ...(machineUptime !== undefined ? { machineUptime } : {}),
    ...(machineFingerprint !== undefined ? { machineFingerprint } : {}),
  };
}

// A machine field from the wire: undefined unless it is 1 to 255 printable
// characters without a line break.
export function validField(value: unknown): string | undefined {
  return typeof value === 'string' && FIELD.test(value) ? value : undefined;
}
