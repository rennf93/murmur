# Changelog

All notable changes to murmur are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.2.2] - 2026-10-20

### Fixed

- `/murmur` now reads as coming from the owner through the session it was typed in: the message tag is `from renn via <session>`, and replies are pushed into that session as well as to `renn`. Before, replies to an owner message went only to `renn`, which has no session, so the session the human typed in never saw them. From a plain terminal (no session among the ancestors) nothing changes.
- A rename now carries the whole session: every live broker connection bound to the old handle (the session's MCP server, an adapter) follows to the new one, so its presence and later sends use the new name. Pending delivery proof and batched notifications follow the rename too.

### Added

- Claude sessions take the name the user gave them. The hook reads the last 256 KB of the transcript, takes the latest `customTitle` and sends it as `title`; the session's handle becomes its slug (`Ledger, Implementer, Companion - Claude 3` becomes `ledger-implementer-companion-claude-3`) and its display the full title. It follows a later `/rename` only while the handle is automatic: `murmur_name` and `murmur name` pin it. A slug held by another live session keeps the current handle.
- `via` on messages, thread views, `MessageSummary` and `SendResult`, backed by a nullable `messages.via` column.
- `murmur prune` and the `prune` protocol request: removes peers that are not the owner and have no session, message, delivery, group membership or live connection (the v0.1 `claude-<8 hex>` leftovers), and prints the removed handles.
- Schema migration: `messages.via` and `sessions.pinned` are added to an existing database when `PRAGMA table_info` lacks them.

### Changed

- Handles may be up to 48 characters (`^[a-z0-9][a-z0-9-]{1,47}$`), up from 32, so title slugs fit.
- The MCP instructions and the `murmur_send` description tell agents to consult several sessions with one `murmur_send` to all of them instead of separate `SendMessage` calls.
- Hummin's model-free `/murmur` passes its process as the ancestor, so its messages carry the session as `via`.

## [0.2.1] - 2026-10-20

### Fixed

- `murmur mcp` exits when its client goes away. The MCP SDK's stdio transport does not watch stdin for end of file, so a server whose CLI had exited or been killed kept running with its broker connection (and the session's presence) open; one from a Codex test was still up 10 hours later. The server now also returns when stdin ends or closes.
- The MCP server reports the package version instead of a hard-coded `0.2.0`.

### Changed

- `publishConfig` no longer sets `provenance: true`, so a hand `npm publish` works outside CI. The release workflow still publishes with `--provenance`.

## [0.2.1-rc.1] - 2026-10-20

### Changed

- Release pipeline check: the first version published to npm by CI through trusted publishing, as a prerelease on the `next` dist-tag. No code changes.

## [0.2.0] - 2026-10-20

### Added

- CLI-agnostic hooks: one hook entry (`murmur hook <event> --client <cli>`) for Claude Code, Codex, Gemini CLI and zcode, each speaking that CLI's hook dialect.
- `murmur install <claude|codex|gemini|zcode|opencode|hummin>`: idempotent merges into each CLI's own config, with a one-time `.bak-murmur` backup of every touched file.
- opencode plugin (`adapters/opencode/murmur.ts`): tools, push with `promptAsync`, presence.
- Hummin extension (`adapters/hummin/murmur.ts`): tools, push with `sendUserMessage`, and a model-free `/murmur`.
- Host-process identity (`src/proc.ts`): a session is identified by its host process, so one global config serves every session and no per-session handle is configured. Sessions are named `<cli>-<folder>` with a numeric suffix for more live sessions in the same folder; restarts reuse their old name.
- `murmur name <handle>` and the `murmur_name` tool rename a session while keeping its messages, deliveries and groups.
- `murmur send --as <handle>` and `murmur reply --as <handle>` for scripted agents. Commands run from an agent's shell speak as that agent; from a plain terminal they speak as the owner.
- `murmur slash`: the `/murmur` backing, with a quoted-heredoc form so quotes, `$(...)`, backticks and `;` arrive verbatim, `--claude-escaped` for Claude Code's `!` escaping, and bare discovery output (usage, peers, groups).
- Delivery proof (`src/proof.ts`): `delivered` now requires evidence from the receiving side (the message tag in the receiver's own transcript, an adapter confirmation, or a hook handover). `sent` is never reported as `delivered`.
- Notes to senders: `queued_delivered` when a queued message finally arrives, `peer_offline` when a session ends with unconsumed messages.
- `murmur_ack` to tell senders a message was seen and will be answered later.
- Live checks against real CLIs in tmux (`scripts/live/`, `make verify CLI=<cli>`) and a benchmark (`scripts/bench.mjs`).
- Packaging for npm, a CI workflow, a release workflow with npm trusted publishing, and a documentation site.

### Changed

- Claude Code native transport corrected from the Claude binary's own injection recipe: `{"type":"auth",...}` then `{"type":"user","message":{...}}`. v0.1 guessed a different frame and counted a clean socket close as delivered.
- Performance: SQLite indexes, a prepared statement cache, a lazily imported MCP SDK (only `murmur mcp` loads it), and a single per-pid `ps` walk for identity.
- Wire protocol version 2.

### Removed

- `scripts/e2e.mjs`, replaced by the live checks in `scripts/live/`.

## [0.1.0] - 2026-10-20

### Added

- Local session message bus: one broker process owning a SQLite store and one Unix domain socket, with token authentication.
- Group threads: one message fans out to any mix of handles and groups; replies reach the whole thread; per-thread monotonic `seq`.
- Delivery states per recipient (queued, sent, delivered, acknowledged, replied) and verdicts (`agree`, `disagree`, `info`).
- Presence (busy, idle, online, presumed-offline, offline) and `murmur_wait` with per-peer reply latency p50 and p95.
- Native Claude Code socket transport, an MCP server exposing the bus as tools, a CLI, and an installer.
- Limits: send rate, duplicate window, thread and inbox caps.
- 283 tests at 100 percent lines and branches, and an end-to-end rig that replayed the live 3-peer scenario.

[0.2.2]: https://github.com/rennf93/murmur/releases/tag/v0.2.2
[0.2.1]: https://github.com/rennf93/murmur/releases/tag/v0.2.1
[0.2.1-rc.1]: https://github.com/rennf93/murmur/releases/tag/v0.2.1-rc.1
[0.2.0]: https://github.com/rennf93/murmur/releases/tag/v0.2.0
[0.1.0]: https://github.com/rennf93/murmur/releases/tag/v0.1.0
