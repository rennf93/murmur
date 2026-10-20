import { execFile } from 'node:child_process';
import type { SessionRow } from './store.js';

// Command transports: a CLI's own way to hand a message to one of its
// sessions from outside. Only Codex has one today (codex queue, which
// queues a user message for a thread). Resolves true when the command
// accepted the message; proof that the session took it still comes from
// its transcript.
export type WakeFn = (session: SessionRow, text: string) => Promise<boolean>;

export const defaultWake: WakeFn = (session, text) => {
  if (session.client !== 'codex' || session.cliSessionId === null) {
    return Promise.resolve(false);
  }
  const args = ['queue', '--thread', session.cliSessionId, '--message', text];
  return new Promise<boolean>((resolve) => {
    execFile('codex', args, { timeout: 10000 }, (err) => {
      resolve(err === null);
    });
  });
};
