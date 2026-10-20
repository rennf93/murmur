# Changelog

All notable changes to murmur are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [0.2.4] - 2026-10-21

### Changed

- Requires Node 22 or newer (`engines` was `>=20`). Node 20 reached end of life on 2026-04-30, and vitest 5 needs Node 22.12 to run the tests, so CI now runs on Node 22 and 24 instead of 20 and 22.
- Dependencies: zod 4 (runtime), and for development TypeScript 7, `@types/node` 26, vitest and `@vitest/coverage-v8` 5 and `actions/setup-node` 7.1.0.

### Fixed

- One reply that resolves two waits on the same thread answers each wait once. Before, handing over the first wait's replies re-entered the waiter check, which answered the second wait, and the outer loop then answered it again from its stale copy (`src/broker.ts` `checkWaiters`).
- A reply held for the asker's open `murmur_wait` no longer sends the replier a `queued_delivered` note when the wait hands it over. The note reached the replier as a new turn (a live check woke the replying session a second time for it) although nothing had waited on a session that was down. A reply that was not held for the wait, for example one that stayed queued while the asker's session was closed, still sends the note when a later wait hands it over.
- The 200 inbox cap (`cap_inbox`) counts only deliveries not yet proven delivered to the recipient's session (queued or sent). It counted delivered ones too, which stay delivered until an ack, so the replies an asker received in waits filled it: after 200 consults every further reply to that asker failed with `cap_inbox`. The unread count in status and inbox is unchanged.

### Security

- vitest 5 drops the vulnerable test-only dependencies `npm audit` reported (tinypool GHSA-5gmw-xhrv-c9v3 and GHSA-85c8-ppgw-ccpr, critical; vitest and `@vitest/mocker` GHSA-82fw-gwwq-j7x9, moderate). The published package was not affected: `npm audit --omit=dev` found nothing.

## [0.2.3] - 2026-10-20

### Changed

- Murmur is a group chat: a message is posted once and everyone in the thread can read it, but nobody is woken for a message that is not for them. A reply is stored once and delivered (with a push, in the parent's mode) only to whoever is waiting for it: the author of the message it answers plus the session an owner message was typed in (`via`); a reply inside the replier's own thread goes to the parent's recipients. Other participants get no delivery row and no push and read the reply with `murmur_thread` / `murmur thread`. Measured reason: `make bench-consult` (haiku, three sessions) cost 546,811 tokens per consult for murmur against 265,771 for native `SendMessage`, because every reply woke every participant and the Implementer received each reply twice (once in the wait, once as a pushed turn).
- `murmur_wait` returns the text of the replies addressed to the waiting handle (`replies`: id, seq, sender, verdict, delivered body with the truncation pointer). A reply that arrives while its recipient has a wait open on that thread is not pushed; it stays queued, is handed over in the wait result (condition met or timeout) and marked delivered. A reply that arrives with no wait open is pushed as before.
- `murmur_thread` takes an optional `messageId` (CLI `murmur thread <id> --message <m>`): the result is only that message and the replies to it, with full delivered bodies; a message not in the thread is a `bad_request`. The footer of every delivered message now points at it (`replies to this: murmur_thread <t> messageId <m>`) instead of the whole thread, and the wait handover is limited to replies to the thread's latest ask.
- The MCP instructions, the `murmur_reply` and `murmur_wait` descriptions, the opencode and Hummin adapter descriptions and the docs describe this routing.
- Claude Code never defers murmur's tools: `murmur install claude` writes `"alwaysLoad": true` into `mcpServers.murmur` (idempotent; re-run it on an existing install) and the MCP server lists every tool with `_meta: { "anthropic/alwaysLoad": true }`. Measured reason: each deferred tool cost a ToolSearch call, and a Claude Code call re-reads the whole context (about 38k tokens on haiku).
- Teaching the reply: the footer on every delivered message and the MCP instructions say plainly that the answer goes through `murmur_reply` with that messageId and that plain text never reaches the sender.
- `murmur_send` takes optional `wait` (`all_replied` or `any_replied`) and `timeoutMs` (same default and cap as `murmur_wait`): it sends and waits on that thread in the same call and returns `{ sent, wait }` with states, verdicts and the replies' text. Asking is one tool call.
- `to` also accepts a session's display name (its CLI title, case-insensitive) or a name whose slug is a handle (`Peer A` resolves to `peer-a`). Handles and groups win; an ambiguous name is a `bad_request` listing the candidates.
- The `/murmur` command file for Claude Code and zcode sets `disable-model-invocation: true`: it is the owner's command, and a model that invoked it through the Skill tool sent nothing. Re-run `murmur install claude` / `zcode` to get it.
- `make bench-consult` counts a run as completed only when the Implementer's final answer holds both peers' verdicts and their answers really reached it, and reports API calls and ToolSearch calls per session. Result (haiku, 3 runs per setup, medians): plain native `SendMessage` completes 0 of 3 (268,272 tokens, 7 API calls), because its peers answer in plain text that never reaches the Implementer; native with the peers told to answer with `SendMessage` completes 3 of 3 (510,931 tokens, 13 API calls, 13.8 s); murmur completes 3 of 3 (362,879 tokens, 9 API calls, 10.1 s), 29 percent fewer tokens than the native setup that finishes.

### Fixed

- A reply held back for an open `murmur_wait` is now only one the wait hands over (a reply to the thread's latest ask); a reply to an older ask, or to a reply, is pushed as usual instead of staying queued with no path to its recipient. A wait remembers the replies held for it and hands them all over even if the thread's latest ask changed meanwhile. Replies held for a wait whose connection closes before it resolves are pushed at once. A delivery whose push is already in flight is never handed over a second time by a new wait or the hook pull.
- The live rig (`scripts/live/lib.mjs`) sets `MURMUR_AUTOSTART=0`, so a hook that outlives cleanup cannot respawn a broker and none survives a live run.

### Added

- `make bench-consult` (`scripts/live/bench-consult.mjs`): three real Claude sessions per run, plain native `SendMessage`, native with the peers told to reply with `SendMessage`, and murmur, with tokens, API calls, copies received and wall time from the sessions' own transcripts; results in `docs/bench/consult.md` and `consult.json`.
- Docs and repository files: README hero, demo and linked CLIs; a Diataxis docs navigation (getting started, using murmur, consult, design, concepts, reference); `CODE_OF_CONDUCT.md`, `SUPPORT.md`, `MAINTAINERS.md`, `CODEOWNERS`, issue forms, a pull request template, Dependabot, an OpenSSF Scorecard workflow, a docs CI workflow (markdownlint, typos, lychee) and SHA-pinned GitHub Actions.

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

[0.2.3]: https://github.com/rennf93/murmur/releases/tag/v0.2.3
[0.2.2]: https://github.com/rennf93/murmur/releases/tag/v0.2.2
[0.2.1]: https://github.com/rennf93/murmur/releases/tag/v0.2.1
[0.2.1-rc.1]: https://github.com/rennf93/murmur/releases/tag/v0.2.1-rc.1
[0.2.0]: https://github.com/rennf93/murmur/releases/tag/v0.2.0
[0.1.0]: https://github.com/rennf93/murmur/releases/tag/v0.1.0
