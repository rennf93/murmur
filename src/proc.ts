import { execFileSync } from 'node:child_process';
import { basename } from 'node:path';

// Process-table helpers that tie every murmur client process to the agent
// CLI session it runs under. Hooks, the MCP server and the CLI all walk up
// their ancestors and take the first one that is not a shell: that process
// is the host (claude, codex, gemini's node, ...). The same rule on every
// side means a hook and an MCP server of one session agree on the host
// without any per-session config.

export interface ProcInfo {
  ppid: number;
  // ps lstart text, stable for the life of the process. Paired with the pid
  // it survives pid reuse.
  start: string;
  comm: string;
}

export interface HostInfo {
  hostPid: number;
  hostStart: string;
  // Every ancestor pid from the parent up, nearest first. The CLI matches
  // these against live session hosts to find the session it runs inside.
  ancestors: number[];
}

// All a walk or a liveness check needs from a process table.
export type ProcTable = Pick<Map<number, ProcInfo>, 'get'>;

const SHELLS = new Set(['sh', 'bash', 'zsh', 'dash', 'fish', 'ksh', 'tcsh', 'csh']);
const MAX_DEPTH = 32;
// macOS ps answers one pid with a direct kernel lookup (about 3 ms) but lists
// the whole table for two or more (about 60 ms), and any full listing costs
// 20 ms without the comm column and 38 ms with it. Up to this many pids,
// one ps each beats one listing.
const ONE_BY_ONE_MAX = 4;

// Parses ps text. lstart is a fixed 24 character field, so a line splits as:
// pid, ppid, 24 chars of start time, then comm (which may hold spaces on
// macOS, where it is the executable path; absent when ps was not asked for it).
export function readProcTable(run: () => string = defaultPs): Map<number, ProcInfo> {
  const table = new Map<number, ProcInfo>();
  for (const raw of run().split('\n')) {
    const match = /^\s*(\d+)\s+(\d+)\s+(.{24})(?:\s+(.*))?$/.exec(raw);
    if (match === null) {
      continue;
    }
    const [, pid, ppid, start, comm] = match as unknown as [string, string, string, string, string | undefined];
    table.set(Number(pid), { ppid: Number(ppid), start: start.trim(), comm: (comm ?? '').trim() });
  }
  return table;
}

function ps(args: string[]): string {
  return execFileSync('ps', args, { encoding: 'utf8', env: { ...process.env, LC_ALL: 'C' }, stdio: ['ignore', 'pipe', 'ignore'] });
}

function defaultPs(): string {
  return ps(['-axo', 'pid=,ppid=,lstart=,comm=']);
}

function defaultPsNoComm(): string {
  return ps(['-axo', 'pid=,ppid=,lstart=']);
}

function defaultPsOne(pid: number): string {
  return ps(['-o', 'pid=,ppid=,lstart=,comm=', '-p', String(pid)]);
}

// One process, or undefined when it is gone or ps is unavailable.
export function readProcess(pid: number, run: (pid: number) => string = defaultPsOne): ProcInfo | undefined {
  try {
    return readProcTable(() => run(pid)).get(pid);
  } catch {
    return undefined;
  }
}

// A table that holds at least these pids, for liveness checks.
export function liveTable(
  pids: number[],
  runOne?: (pid: number) => string,
  runAll: () => string = defaultPsNoComm,
): ProcTable {
  const distinct = [...new Set(pids)];
  if (distinct.length > ONE_BY_ONE_MAX) {
    try {
      return readProcTable(runAll);
    } catch {
      return new Map();
    }
  }
  const table = new Map<number, ProcInfo>();
  for (const pid of distinct) {
    const info = readProcess(pid, runOne);
    if (info !== undefined) {
      table.set(pid, info);
    }
  }
  return table;
}

export function isShell(comm: string): boolean {
  return SHELLS.has(basename(comm).replace(/^-/, ''));
}

// Walks up from startPid's parent. The host is the first ancestor that is
// not a shell; null when the table has no usable chain. Without full the walk
// stops at the host, so ancestors ends there instead of at the root.
export function hostOf(table: ProcTable, startPid: number, full = true): HostInfo | null {
  const ancestors: number[] = [];
  let host: { pid: number; start: string } | null = null;
  let cursor = table.get(startPid)?.ppid;
  while (cursor !== undefined && cursor > 1 && ancestors.length < MAX_DEPTH) {
    const info = table.get(cursor);
    if (info === undefined) {
      break;
    }
    ancestors.push(cursor);
    if (host === null && !isShell(info.comm)) {
      host = { pid: cursor, start: info.start };
      if (!full) {
        break;
      }
    }
    cursor = info.ppid;
  }
  if (host === null) {
    return null;
  }
  return { hostPid: host.pid, hostStart: host.start, ancestors };
}

// The calling process's host, or null when ps is unavailable. ps runs once
// per ancestor, never over the whole table; the caller's own row is not
// fetched because node already knows its parent. Callers that only need the
// host (hooks, MCP, name) pass full = false and skip the rest of the chain.
export function currentHost(run?: (pid: number) => string, full = true): HostInfo | null {
  const table: ProcTable = {
    get: (pid) => (pid === process.pid ? { ppid: process.ppid, start: '', comm: '' } : readProcess(pid, run)),
  };
  return hostOf(table, process.pid, full);
}

// True when pid is running and is still the same process that was recorded
// (same start time), so a recycled pid never reads as a live session.
export function isAlive(table: ProcTable, pid: number, start: string): boolean {
  const info = table.get(pid);
  return info !== undefined && info.start === start;
}
