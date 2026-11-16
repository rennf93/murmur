import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { hostname } from 'node:os';

// A host reference is a pid and a start time, which only mean something on the
// machine that ran the process. The machine id names that machine and rides
// along with every host reference. It is the boot id (a uuid per machine and
// boot, so a rebooted machine or a clone is another machine); the host name is
// the fallback only, because a host name can change while the machine runs
// (macOS renames itself with the network) and would make one machine look like
// two.

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;

// The boot uuid inside Linux's /proc/sys/kernel/random/boot_id or macOS's
// `sysctl -n kern.bootsessionuuid` (also with its `name: ` prefix), lower
// case; empty when the text holds none.
export function parseBootId(text: string): string {
  return UUID.exec(text)?.[0].toLowerCase() ?? '';
}

// Linux reads /proc, macOS asks sysctl (the one thing to confirm on a Mac:
// `sysctl -n kern.bootsessionuuid` prints a uuid). Anything else, or a failure,
// has no boot id.
export function readBootId(
  platform: string = process.platform,
  readFile: (path: string) => string = (path) => readFileSync(path, 'utf8'),
  run: (cmd: string, args: string[]) => string = (cmd, args) => execFileSync(cmd, args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }),
): string {
  try {
    if (platform === 'linux') {
      return parseBootId(readFile('/proc/sys/kernel/random/boot_id'));
    }
    if (platform === 'darwin') {
      return parseBootId(run('sysctl', ['-n', 'kern.bootsessionuuid']));
    }
  } catch {
    // No boot id: the host name alone names the machine.
  }
  return '';
}

export function machineId(name: string = hostname(), bootId: string = readBootId()): string {
  return bootId === '' ? name : bootId;
}

let cached: string | undefined;

// This machine's id, read once.
export function thisMachine(): string {
  cached ??= machineId();
  return cached;
}
