import { chmodSync, existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { MurmurError, newToken } from './protocol.js';

export interface MurmurPaths {
  home: string;
  socketPath: string;
  dbPath: string;
  tokenPath: string;
}

// Resolution order: explicit envHome argument, then MURMUR_HOME, then
// ~/.murmur.
export function getPaths(envHome?: string): MurmurPaths {
  const home = envHome ?? process.env.MURMUR_HOME ?? join(homedir(), '.murmur');
  return {
    home,
    socketPath: join(home, 'bus.sock'),
    dbPath: join(home, 'murmur.db'),
    tokenPath: join(home, 'token'),
  };
}

// Creates the murmur home directory (mode 0700) and, when missing, the token
// file (mode 0600) holding a freshly generated random token. Throws a plain
// Error when permissions cannot be set.
export function ensureHome(paths: MurmurPaths): void {
  mkdirSync(paths.home, { recursive: true });
  try {
    chmodSync(paths.home, 0o700);
  } catch (err) {
    throw new Error('murmur: cannot set mode 0700 on home directory ' + paths.home + ': ' + (err as Error).message);
  }
  if (!existsSync(paths.tokenPath)) {
    try {
      writeFileSync(paths.tokenPath, newToken(), { encoding: 'utf8', flag: 'wx', mode: 0o600 });
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === 'EEXIST') {
        // Lost a creation race; the token written by the winner stands.
        return;
      }
      throw new Error('murmur: cannot create token file ' + paths.tokenPath + ': ' + (err as Error).message);
    }
    try {
      // writeFileSync masks the requested mode with the process umask, so
      // pin the mode explicitly.
      chmodSync(paths.tokenPath, 0o600);
    } catch (err) {
      throw new Error('murmur: cannot set mode 0600 on token file ' + paths.tokenPath + ': ' + (err as Error).message);
    }
  }
}

// Reads and trims the shared token. Throws MurmurError('bad_token') when the
// file is missing or its trimmed contents are empty.
export function readToken(paths: MurmurPaths): string {
  let raw: string;
  try {
    raw = readFileSync(paths.tokenPath, 'utf8');
  } catch (err) {
    throw new MurmurError('bad_token', 'cannot read token file ' + paths.tokenPath + ': ' + (err as Error).message);
  }
  const token = raw.trim();
  if (token.length === 0) {
    throw new MurmurError('bad_token', 'token file ' + paths.tokenPath + ' is empty');
  }
  return token;
}

// Asserts that the home directory is mode 0700 and the token file is mode
// 0600, otherwise throws MurmurError('internal') naming the offending path.
export function verifyPerms(paths: MurmurPaths): void {
  let homeMode: number;
  let tokenMode: number;
  try {
    homeMode = statSync(paths.home).mode & 0o777;
    tokenMode = statSync(paths.tokenPath).mode & 0o777;
  } catch (err) {
    throw new MurmurError('internal', 'cannot stat murmur paths under ' + paths.home + ': ' + (err as Error).message);
  }
  if (homeMode !== 0o700) {
    throw new MurmurError('internal', 'home directory ' + paths.home + ' has mode ' + homeMode.toString(8) + ', expected 700');
  }
  if (tokenMode !== 0o600) {
    throw new MurmurError('internal', 'token file ' + paths.tokenPath + ' has mode ' + tokenMode.toString(8) + ', expected 600');
  }
}
