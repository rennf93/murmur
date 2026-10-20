#!/usr/bin/env node
// Wrapper entry so the npm-link symlink always works: node resolves argv[1]
// through the symlink while import.meta.url keeps the real path, which breaks
// the in-module entry guard of dist/cli.js. This wrapper calls runCli
// explicitly instead of relying on that guard.
// The real command may start the broker on demand; tests and library
// callers never set this, so they never spawn one.
process.env.MURMUR_AUTOSTART ??= '1';
import { runCli } from '../dist/cli.js';
const code = await runCli(process.argv.slice(2));
process.exit(code);
