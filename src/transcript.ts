import { closeSync, constants, fstatSync, lstatSync, openSync, readdirSync, readSync } from 'node:fs';

// A transcript path arrives in a hook payload, so it is trusted no further
// than this: a regular file (not a link, not a FIFO or a device) whose name
// ends in .jsonl. It is opened without blocking and without following a
// link, and checked again on the open handle, so a swap between the check and
// the open cannot make the broker wait on a pipe or read another file.
export function usableTranscript(path: string): boolean {
  try {
    return path.endsWith('.jsonl') && lstatSync(path).isFile();
  } catch {
    return false;
  }
}

// The last `bytes` of a usable transcript as whole lines (the line the cut
// lands in is dropped), or null when the path is not usable.
export function tailLines(path: string, bytes: number): string[] | null {
  if (!usableTranscript(path)) {
    return null;
  }
  try {
    const fd = openSync(path, constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW);
    try {
      // A pipe or device opened in the race after the check has size 0, so
      // nothing is read from it; O_NONBLOCK keeps the open itself from waiting.
      const stat = fstatSync(fd);
      const start = Math.max(0, stat.size - bytes);
      const buf = Buffer.alloc(stat.size - start);
      readSync(fd, buf, 0, buf.length, start);
      const lines = buf.toString('utf8').split('\n');
      if (start > 0) {
        lines.shift();
      }
      return lines;
    } finally {
      closeSync(fd);
    }
  } catch {
    return null;
  }
}

// A Task subagent writes its own transcript next to the session's, in
// <session transcript minus .jsonl>/subagents/.
export function subagentTranscripts(path: string): string[] {
  const dir = path.replace(/\.jsonl$/, '') + '/subagents';
  try {
    return readdirSync(dir).filter((name) => name.endsWith('.jsonl')).map((name) => dir + '/' + name);
  } catch {
    return [];
  }
}

const TAIL_BYTES = 256 * 1024;

// True when a tool_use entry with exactly this id is in the tail of the
// transcript or of one of its subagent transcripts. The id is matched as a
// whole JSON string on a parsed tool_use block, so an id quoted in a message
// or the prefix of a longer id never counts.
export function hasToolUse(path: string, toolUseId: string): boolean {
  const quoted = JSON.stringify(toolUseId);
  return [path, ...subagentTranscripts(path)].some((file) =>
    (tailLines(file, TAIL_BYTES) ?? []).some((line) => line.includes(quoted) && isToolUse(line, toolUseId)),
  );
}

function isToolUse(line: string, toolUseId: string): boolean {
  try {
    const content = (JSON.parse(line) as { message?: { content?: unknown } }).message?.content;
    return Array.isArray(content) && content.some((block: { type?: unknown; id?: unknown } | null) => block?.type === 'tool_use' && block.id === toolUseId);
  } catch {
    return false;
  }
}

// The newest write to a transcript or to one of its subagent transcripts, in
// ms since the epoch, or null when none can be read.
export function newestWriteMs(path: string): number | null {
  let newest: number | null = null;
  for (const file of [path, ...subagentTranscripts(path)]) {
    try {
      const stat = lstatSync(file);
      if (stat.isFile() && (newest === null || stat.mtimeMs > newest)) {
        newest = stat.mtimeMs;
      }
    } catch {
      // A missing file adds nothing.
    }
  }
  return newest;
}
