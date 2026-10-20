# murmur

Murmur is a local, client-agnostic message bus for AI CLI sessions that share one machine and one user account: a single broker process owns a SQLite store and a single Unix domain socket, and agent sessions connect to it as named peers that talk in threads. One send produces one message id that fans out to any mix of peer handles and `@groups`, so no session composes the same announcement twice and no session relays another's words by hand. Claude Code sessions get delivery into the running session over its native messaging socket, hook-driven injection of pending messages at each prompt, and an MCP server with eleven `murmur_*` tools; any other agent that can run a shell command (zcode, hummin, a plain script) drives the same broker through the `murmur` CLI. Three delivery modes (push, notify, silent) are the dial between interrupting a live session now and leaving a quiet note in its inbox.

## Install

Requires Node 20 or newer.

```
npm install
npm run build            # emits dist/; the package bin is bin/murmur.js, a small
                         # wrapper with a shebang that calls runCli explicitly,
                         # because node resolves argv[1] through the npm-link
                         # symlink while import.meta.url keeps the real path,
                         # which defeats an in-module entry guard
npm link                 # required: puts murmur on PATH; the mcp config, the
                         # hooks and the /murmur command all invoke plain
                         # 'murmur' from PATH
murmur init              # create the murmur home and the owner peer
murmur serve             # start the broker; prints the socket path
murmur install claude    # or: murmur install zcode | hummin
```

- `murmur init` creates `~/.murmur` (mode 0700), a fresh random token file `~/.murmur/token` (mode 0600), `murmur.db`, and the owner peer `renn`. Set `MURMUR_HOME` to relocate the home; every component resolves paths the same way.
- `murmur serve` listens on `<home>/bus.sock` and prints that path. `murmur serve --daemon` re-spawns itself detached from the terminal.
- `murmur install claude` writes and merges, idempotently, with a one-time `.bak-murmur` backup before the first write to each file:
  - `~/.claude/commands/murmur.md`, the `/murmur` slash command,
  - `mcpServers.murmur` in `~/.claude.json` (launches `murmur mcp`),
  - hooks in `~/.claude/settings.json`: SessionStart runs `murmur hook session-start`, UserPromptSubmit runs `murmur hook prompt`, Stop runs `murmur hook stop`.
- `murmur install zcode` writes `~/.zcode/skills/murmur/SKILL.md`, a CLI-first skill for sending and polling. MCP is the identity-correct path where the client supports MCP servers; the CLI skill with `--as` is the fallback.
- `murmur install hummin` writes nothing; it prints the three `murmur hook` commands to wire by hand.

Per-session handle: edit the `MURMUR_HANDLE` value that `murmur install claude` writes into `mcpServers.murmur` in `~/.claude.json` (placeholder `CHANGE-ME-per-session`) so every session carries its own handle on the bus. Fallbacks when it is unset: the MCP server registers as `anon-<pid>`; hooks resolve `claude-` plus the first 8 characters of the session id, or `claude-unknown` when no session id is available.

## Quickstart

One terminal, straight after `murmur init`, real transcript:

```
$ murmur status
renn offline unread 0
$ murmur groups create smoke-test
smoke-test:
$ murmur groups add smoke-test renn
smoke-test: renn
$ murmur send @smoke-test first message on the bus
renn: queued
message 1 thread 1 seq 0
$ murmur thread 1
0 renn first message on the bus
$ murmur groups delete smoke-test
```

`murmur groups delete` tidies the group and keeps threads and history. The delivery prints `queued` because there is no live connection and no native transport for that recipient yet; it becomes `delivered` on the next hook pull or read.

Two sessions: in terminal A run `murmur send <peer-handle> question`. The peer's session receives it via its native socket or at its next prompt hook. The peer replies from its session (the `murmur_reply` tool) or from a shell (`murmur send --as <handle> --thread <id> <targets> <body>`). The sender catches up with `murmur thread <id>` and `murmur_collect`.

## Delivery truth table

The table assumes the default `push` mode. `notify` stores the message and emits only batched notifications; `silent` only stores. Only `push` ever moves a delivery out of queued at send time.

| Peer | What is delivered | When the agent sees it | State it lands in | Reply path |
| --- | --- | --- | --- | --- |
| Claude peer with native transport | The message text (cut at 4000 chars with a truncation notice when the stored body is longer) plus a reply footer, written to the session's own messaging socket under `/tmp/cc-socks/<pid>.sock`, registered at session-start from `CLAUDE_CODE_MESSAGING_SOCKET` and `CLAUDE_CODE_MESSAGING_TOKEN` | Immediately, mid-session: the running Claude Code session receives it over its native messaging socket | `queued`, then `sent`, then `delivered` in one step at send time; `sentAt` and `deliveredAt` both stamped | The footer names it: `murmur_reply in_reply_to <message id>` (MCP tool, optionally with a verdict), then `murmur_thread <thread id>` to read the full thread |
| Claude peer without native transport | A push event over the session's live broker connection (the MCP facade's long-lived connection) when one exists; the body itself stays in the store. With no live connection at send time, nothing leaves the broker | The facade forwards no events on its own. At the next user prompt the UserPromptSubmit hook drains up to 10 pending messages into `additionalContext`; or on demand via `murmur_inbox` / `murmur_read` | `sent` if a live connection took the push event, otherwise `queued`; `delivered` once the hook pull or `murmur_read` hands it over | The rendered header says `reply with murmur_reply in_reply_to <id>`; same alternatives as the row above |
| zcode or hummin peer | Nothing is pushed; the message waits in the store as a queued delivery. Batched notifications find no live connection and no native transport and are dropped; the data stays in the database | zcode: at its next turn start, by polling as the installed skill instructs, `murmur thread <id> --since <last seq seen>`. hummin: at the next prompt if the operator wired `murmur hook prompt` by hand; the hook drains up to 10 pending messages | `queued`. It becomes `delivered` only through a hook pull (hummin) or `murmur_read`; a plain CLI `murmur thread` read leaves the delivery state untouched | `murmur send --as <handle> --thread <id> <targets> <body>`, the form the zcode skill documents; CLI replies without `--as` attribute to the owner handle |
| Offline peer | Nothing at send time; the delivery row is born queued. A failed native push (error reply, write failure, 5s timeout, vanished transport row) is skipped silently and also stays queued | On its next pull after returning: prompt hook fetch, `murmur_read`, or `murmur_inbox`. The sender can `murmur_wait` on the thread and receives `peer_offline` when the peer drops with the message unconsumed, and `queued_delivered` when the queued message is finally handed out | `queued` until read or fetch, then `delivered` | Same reply paths, once the peer is back |

## Group and thread model

- One message id fans out. A send targets any mix of handles and `@group` names; the broker expands groups, validates every handle, dedups while preserving order, appends one message row, and creates one delivery row per recipient.
- Sequencing. Every message carries a per-thread monotonic `seq`, allocated inside the append transaction and unique per thread. `murmur thread <id> --since <seq>` reads only what is newer, so peers never re-read and never disagree about order.
- Verdicts. A reply carries an optional machine-readable verdict: `agree`, `disagree`, or `info`. The convention is to start the reply body with `AGREE`, `DISAGREE`, or `INFO` so peers can scan stances. `murmur_collect` returns the per-recipient matrix and the verdict tally for the thread's latest ask.
- References. A message may carry a `references` list (paths, commits, any pointer). Verification artifacts stay in their repos; messages carry pointers, not payloads.
- Truncation at 4000. Delivered copies of long bodies are cut at 4000 characters with a notice pointing at the stored original; the full body is always stored and readable. Summaries show a first line capped at 120 characters.

## Presence tiers

`murmur status` prints one line per peer: handle, presence, unread count, optional display name. Presence resolves in tiers, first match wins:

1. **Live connection.** An authenticated connection is bound to the handle right now (a register or a hook binds it): `online`.
2. **Hook busy or idle.** No live connection, but the last hook event says something: `prompt` means the session is working (`busy`, with `busySince`), `session-start` and `stop` mean `idle` (with `idleSince`).
3. **Gone quiet.** No connection and no hook state, and the last authenticated activity is more than 5 minutes old (or never happened): `offline`.
4. **Presumed-offline with age.** Seen within the last 5 minutes but not connected now: `presumed-offline`. The status payload carries `lastActivityAgeMs`, the age of the freshest of last-seen and last hook activity, plus the peer's unread count.

## The human surface

- **`murmur send` (positional form).** `murmur send <targets> <body...> [--as <handle>] [--mode <mode>] [--refs <a,b>] [--thread <id>]`. Targets are a comma separated list of handles and `@groups`. Default identity is the owner handle `renn`, display `renn (owner)`. `--as <handle>` registers and sends as that handle instead, with no display name; it exists for scripted agents replying on their own identity, while humans keep the default owner identity. An invalid `--as` handle is rejected with `invalid handle: <handle>` and exit code 1. Output is one `recipient: state` line per delivery, then a final `message <id> thread <t> seq <s>` line. Other read-side commands: `murmur status`, `murmur thread <id> [--since <n>] [--full]`, and `murmur groups list|create|add|remove|delete`.
- **The `/murmur` slash command.** `murmur install claude` writes `~/.claude/commands/murmur.md` with frontmatter (`description: Send to a murmur group or peer; with no arguments it lists peers, groups and usage` and `argument-hint: <@group | handle> <message>`, so the autocomplete hint spells out the `/murmur <@group | handle> <message>` syntax), a pre-exec line `!murmur slash $ARGUMENTS`, and a fallback block. Discovery is built into the command: bare `/murmur` (or `murmur slash` in a terminal) prints one usage line, `/murmur <@group | handle[,more]> <message>   (run bare like this for peers and groups)`, then `peers:` with the exact lines `murmur status` prints, then `groups:` with the exact lines `murmur groups list` prints, so the active groups and peers are visible from the command itself. `murmur slash status` and `murmur slash groups` delegate to those views; any other arguments behave exactly like `murmur send` with the same parsing, flags and output. The bang-mode caveat: the exclamation line is a Claude Code command-file feature, where a `!`-prefixed line in a command markdown file is pre-executed as a shell command when the command runs; it is a distinct mechanism from typing `!command` directly in the input box (interactive bash mode). The file itself carries a comment that the pre-exec form must be verified against the installed Claude version, and its fallback block covers versions where pre-exec does not run: it instructs the model to run `murmur slash $ARGUMENTS` through the Bash tool instead, then report the printed lines verbatim (the per-recipient delivery states and the final message, thread, and seq line when sending; the usage, peers and groups listing when no arguments were given).

## Doctrine

The bus exists to remove two costs and to never touch a third thing. The two costs: duplicated composition, where every member of a group reformats and re-addresses the same fact, and sender-side relaying, where session A hand-carries B's words to C and drops or garbles them on the way. One `send` with one message id fans out to every recipient; the broker, not the sender, owns storage and delivery; the delivery modes are the token dial between interrupting a live session (`push`), leaving a batched note (`notify`), and storing silently (`silent`). The third thing is independent verification, and the bus must never remove it. A message is a claim, not evidence: peers verify against the artifacts themselves. The `references` field exists exactly for that: verification artifacts stay in their repos, and messages carry pointers to them.

## Limits

- **Send rate.** 20 sends per sender per sliding 60 second window, then `rate_limited`. The counters are in memory by design and reset when the broker restarts.
- **Duplicate suppression.** An identical (sender, body, recipient set) send within 60 seconds is rejected with `duplicate`.
- **Thread cap.** 500 messages per thread, then `cap_thread`.
- **Inbox cap.** 200 unread deliveries (queued, sent, or delivered) per recipient, then `cap_inbox`.
- **1 MiB lines.** A request line over 1 MiB is rejected and the connection destroyed.
- **Wait cap.** `murmur_wait` times out at 120000 ms by default and is capped at 300000 ms.
- **Body and summary sizes.** Delivered copies truncated at 4000 chars (full body always stored); summary first lines capped at 120 chars.
- **Hook budget.** A hook never blocks a session for long: 10 second hard deadline on the broker interaction, up to 10 messages drained per prompt.
- **Notifications.** Batched per recipient and flushed on a 60 second timer; best effort, never a delivery guarantee (the data stays in the database).

## Security posture

- **Unix domain socket only, mode 0600.** The broker listens on `<home>/bus.sock` and chmods the socket to 0600 immediately after listen; it refuses to run if it cannot. There is no TCP listener anywhere.
- **Token file.** One shared random 256-bit token lives at `<home>/token`, mode 0600, inside a home directory pinned to 0700. The first line on any connection must be `auth` with that token; anything else draws `auth_required` and a disconnect.
- **Same-user boundary.** A 0700 home, a 0600 socket, and a 0600 token mean only processes running as the same OS user can connect or learn the token. That is the entire trust boundary, and it is deliberate: murmur carries context between an operator's own sessions, not between strangers.
- **Messages never approve anything.** Deliveries are injected context for a session to read, never permission decisions; hooks exit 0 on every failure path and can never fail or block a session.

## Verification

- `make test` runs the full suite (285 tests).
- `make coverage` runs the suite under the coverage gate: it enforces 100 percent on statements, branches, functions and lines, and it must never be weakened.
- `make verify` runs the end-to-end rig with real headless claude sessions; `node scripts/e2e.mjs --fake-peers` exits 3 as its honest degraded-mode pass signal (real mode exits 0).
- The repo lives at `/Users/renzof/Documents/GitHub/ZZZ/murmur` and requires Node 20 or newer.

## Troubleshooting

- **`murmur broker is down; start it with: murmur serve`.** Every CLI command prints this (exit code 2) when the socket cannot be connected to or closes early. Start the broker, with `--daemon` to detach, and retry. The MCP server prints a similar line to stderr and exits when it cannot connect or register.
- **Spike fallback.** When a native push to a live Claude session fails (error reply, write failure, 5s timeout, missing transport row), the delivery is left queued, never rolled back, and surfaces through the prompt hook pull instead. Notifications are equally best effort: with no live connection and no native transport the batch is dropped and the data stays in the database.
- **Install refuses to guess.** A `.claude.json` or `settings.json` that is not valid JSON stops the install with an error naming the file; fix or remove it first. Existing content is otherwise preserved and merged.
- **Handle collisions and identity.** Two sessions must not share a handle: set `MURMUR_HANDLE` per session (see Install). Use `murmur status` to see who is on the bus and at what presence before sending.
