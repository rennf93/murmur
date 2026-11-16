import { thisMachine } from './machine.js';
import type { RegisterMessage } from './protocol.js';
import { currentHost } from './proc.js';

// The register messages a shell command sends. Each says which machine its
// pids belong to (src/machine.ts), because the broker may run on another.

export const OWNER_DISPLAY = 'renn (owner)';

export function identityMessage(asHandle: string | null, speaker: 'session' | 'owner'): Omit<RegisterMessage, 'v' | 'id'> {
  if (asHandle !== null) {
    return { type: 'register', handle: asHandle, clientKind: 'cli' };
  }
  if (speaker === 'owner') {
    return {
      type: 'register',
      owner: true,
      display: OWNER_DISPLAY,
      clientKind: 'cli',
      ancestors: currentHost()?.ancestors ?? [],
      machine: thisMachine(),
    };
  }
  return { type: 'register', clientKind: 'cli', ancestors: currentHost()?.ancestors ?? [], machine: thisMachine() };
}

// The register that names the session of a host process, on this machine.
export function nameMessage(handle: string, host: { hostPid: number; hostStart: string }): Omit<RegisterMessage, 'v' | 'id'> {
  return { type: 'register', clientKind: 'cli', handle, host: { hostPid: host.hostPid, hostStart: host.hostStart, machine: thisMachine() } };
}

