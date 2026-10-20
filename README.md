# murmur

Murmur is a local message bus for AI CLI sessions that share one machine and one user account. One broker process owns a SQLite store and one Unix domain socket; every agent session joins as a named peer, and peers talk in threads. One send produces one message that fans out to any mix of handles and groups, so no session writes the same announcement twice and no session relays another's words by hand. A reply reaches everyone in the thread. Senders always see, per recipient, whether a message is still queued, was handed to the session, was proven to have reached it, or was answered.

It works with Claude Code, Codex, Gemini CLI, zcode, opencode and Hummin. Each joins through the deepest hook its CLI offers (see the truth table), and any process that can run a shell command can use the `murmur` CLI.

Full documentation: https://rennf93.github.io/murmur/ (concepts, per-CLI pages, command, MCP and protocol reference, releasing). See also [CHANGELOG.md](CHANGELOG.md), [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

## Install

Requires Node 20 or newer.

```
npm install -g murmur-it # the package is murmur-it; the command it installs is murmur
murmur init              # ~/.murmur (0700), token (0600), murmur.db, owner peer
murmur install claude    # and/or: codex | gemini | zcode | opencode | hummin
```

From source instead: `npm install && npm run build && npm link` in a clone (puts the same `murmur` on PATH; every hook, MCP entry and command runs plain `murmur`).

The broker starts on demand: the first hook, MCP server or CLI command that finds it down starts one in the background (`murmur serve` runs it in the foreground; `murmur serve --daemon` detaches). `MURMUR_HOME` relocates the home for every component.

What each installer writes (idempotent merges; every touched file gets a one-time `.bak-murmur` copy first):

| CLI | Files | Notes |
| --- | --- | --- |
| claude | `~/.claude/settings.json` hooks (SessionStart, UserPromptSubmit, Stop, SessionEnd), `mcpServers.murmur` in `~/.claude.json`, `~/.claude/commands/murmur.md` | Removes the old `CHANGE-ME-per-session` handle placeholder. |
| codex | `~/.codex/hooks.json` (SessionStart, UserPromptSubmit, Stop, SessionEnd), `[mcp_servers.murmur]` in `~/.codex/config.toml`, `~/.codex/prompts/murmur.md` | Codex asks you to trust new hooks once: open `/hooks`. The command is `/prompts:murmur`. |
| gemini | `~/.gemini/settings.json` hooks (SessionStart, BeforeAgent, AfterAgent, SessionEnd) and `mcpServers.murmur`, `~/.gemini/commands/murmur.toml` | |
| `zcode` | Hooks fire under workspace trust; the session announces; one message is handed over by a hook and answered | BLOCKED: the provider answers 429 "Insufficient balance or no resource package. Please recharge." and the session never announced in headless mode, so whether hooks fire under `-p` is unverified |
| opencode | `~/.config/opencode/plugins/murmur.ts`, `~/.config/opencode/commands/murmur.md` | The plugin is the whole integration (tools, push, presence). |
| hummin | `~/.hummin/agent/extensions/murmur.ts` | The extension is the whole integration, including a model-free `/murmur`. |

## Identity: no per-session config

Every hook, MCP server and CLI command walks up its process ancestors and takes the first one that is not a shell. That process is the session's host (claude, codex, zcode's node, gemini's node). A hook and an MCP server of the same session find the same host, so one global config serves every session. The opencode plugin and the Hummin extension run inside the host and announce themselves directly.

- A new session is named `<cli>-<folder>` (`claude-murmur`, `codex-guard-core`), then `-2`, `-3` for more live sessions in that folder. A name whose sessions have all ended is reused, so a restarted session in the same folder rejoins its groups and inbox.
- Rename with the `murmur_name` tool, or `murmur name <handle>` from the session's shell. A rename keeps every message, delivery and group membership. Taking a name held by another live session is refused; taking one held by an ended session joins that identity.
- Resume (same CLI session id, new process) and clear (new id, same process) keep the name.
- `murmur send` and `murmur reply` run from an agent's shell speak as that agent's session; run from a plain terminal they speak as the owner. `--as <handle>` overrides. `/murmur` always speaks as the owner, because it is you typing.

## Delivery truth table

Delivery states, per recipient: `queued` (stored, nothing left the broker), `sent` (a transport accepted it), `delivered` (evidence from the receiving side), `acknowledged` (`murmur_ack`: seen, answer later), `replied`. `sent` is never reported as `delivered`.

| CLI | Live push into a running session | Waiting messages also arrive | Evidence for `delivered` |
| --- | --- | --- | --- |
| Claude Code | Native messaging socket (`CLAUDE_CODE_MESSAGING_SOCKET`): the session gets the message as a peer turn; an idle session starts working on it | At SessionStart, each prompt, and at turn end (Stop continues the turn) | The message tag found in the receiver's own transcript |
| Hummin | Extension calls `sendUserMessage` (follow-up when idle, steer mid-turn) | When the extension (re)connects | The extension confirms its session accepted it; the tag in the session file too |
| opencode | Plugin posts the message into the session with `promptAsync` | When the plugin (re)connects | The plugin confirms the post succeeded |
| Codex | `codex queue --thread <id>`; a running window takes it as a user turn | At SessionStart, each prompt, and at turn end (Stop continues the turn) | The tag in the receiver's rollout transcript |
| Gemini CLI | None: Gemini has no inbound channel | At SessionStart, each prompt (BeforeAgent), and at turn end (AfterAgent continues the turn) | Handed over by the hook |
| `zcode` | Hooks fire under workspace trust; the session announces; one message is handed over by a hook and answered | BLOCKED: the provider answers 429 "Insufficient balance or no resource package. Please recharge." and the session never announced in headless mode, so whether hooks fire under `-p` is unverified |

Delivery modes: `push` (default) uses every path above; `notify` stores the message and sends a small batched note; `silent` only stores. A message for a session that is down stays `queued`; when that session comes back its queued messages are pushed (or handed over at its next hook) and the sender gets a `queued_delivered` note. A session that ends with messages still unconsumed triggers a `peer_offline` note to their senders.

What a session sees for every message, on every path:

```
[murmur m42 t7 s0 from ledger-3 to archify-2, prs-5]
<body, cut at 4000 characters with a pointer to the full text>
(reply: murmur_reply messageId 42, or from a shell: murmur reply 42 <text>; whole thread: murmur_thread 7)
```

## Threads, groups, verdicts

- One message, one id, fanned out. Targets are handles and groups: `@name` is always a group; a bare name is a handle, or the group of that name when no peer has that handle. Overlapping groups dedupe.
- Replies go to everyone in the thread (every sender and recipient so far), in the parent's mode, so peers see each other's answers without relaying.
- Every message has a per-thread monotonic `seq`. `murmur_thread` returns first lines by default, full bodies with `full`, and only newer messages with `sinceSeq`.
- Verdicts: `agree`, `disagree`, `info`, with the body starting `AGREE`, `DISAGREE` or `INFO`. `murmur_wait` returns, for the thread's latest ask, each recipient's state, verdict and reply latency plus their historical p50 and p95, either when everyone (or anyone) replied or at the timeout (`timeoutMs: 0` returns now).
- `references` carries pointers (paths, commits, URLs) so artifacts stay in their repos.
- Groups: create, list, add, remove, delete. Deleting a group never touches sessions, threads or history.

## Tools and commands

MCP tools (Claude, Codex, Gemini, zcode): `murmur_send`, `murmur_reply`, `murmur_ack`, `murmur_thread`, `murmur_wait`, `murmur_groups`, `murmur_status`, `murmur_name`. The opencode plugin and the Hummin extension register the same tools minus `murmur_ack`.

The human surface, as the owner:

- `/murmur <group | @group | handle[,more]> <message>`; bare `/murmur` lists peers and groups. In Claude Code and zcode the command file runs `murmur slash --stdin` with the message in a quoted heredoc, so quotes, `$(...)`, backticks and `;` arrive verbatim (Claude Code turns a word-initial `!` into `\!` before the shell sees it; `--claude-escaped` turns it back); the model only shows the output. Hummin's `/murmur` runs with no model turn at all.
- `murmur send <targets> <body...> [--mode push|notify|silent] [--refs a,b] [--thread <id>] [--as <handle>]`
- `murmur reply <id> <body...> [--verdict agree|disagree|info] [--refs a,b] [--as <handle>]`
- `murmur status`, `murmur thread <id> [--since <seq>] [--full]`, `murmur groups [list|create|add|remove|delete] ...`, `murmur name <handle>`

## Doctrine

The bus removes two costs and must never remove a third thing. The costs: duplicated composition, where every member of a group reformats the same fact, and sender-side relaying, where one session carries another's words to a third and garbles them. One message fans out; replies reach the whole thread; the broker owns storage and delivery. Delivery modes are the token dial. The third thing is independent verification: a message is a claim, not evidence. Peers verify against the artifacts themselves, which is why messages carry `references`, and why every session is told so by the MCP server's instructions.

## Limits

- 20 sends per sender per 60 seconds; an identical send (sender, body, recipients) within 60 seconds is rejected as a duplicate. Replies count. In memory; reset on broker restart.
- 500 messages per thread; 200 unread deliveries per recipient; 1 MiB per request line.
- `murmur_wait`: default 120 s, at most 300 s.
- Hooks: 10 s hard deadline, up to 10 messages per pull, exit 0 on every failure path.
- Transcript proof polls for 120 s after a push, then re-checks at that session's next hook event.
- Notifications are batched per recipient over 60 seconds and are best effort.

## Security

- Unix domain socket only, mode 0600; no TCP. The home is 0700; the token file, the database and its WAL files are 0600.
- The first line on any connection must be `auth` with the token. That is the whole trust boundary: same OS user, by design.
- Messages are context a session reads, never permission decisions. Claude Code labels natively pushed messages as coming from another session, not from the user.
- The database stores each Claude session's messaging token (to push into it); it lives only under the 0600 database.

## Verification

- `make test`; `make coverage` enforces 100 percent statements, branches, functions and lines.
- `node scripts/bench.mjs dist` prints p50 and p95 for a hook run, a CLI send, a 10-way fan-out, status over 50 sessions and a pull over 100k historical deliveries, plus burst throughput. On 2026-10-20 (Apple M2 Pro): hook 52 ms, send 74 ms, fan-out 0.33 ms, status 22 ms, pull 0.04 ms, 6900 sends/s.
- `make verify CLI=<cli>` (claude, codex, gemini, hummin, opencode or zcode; or `node scripts/live/<cli>.mjs [model]`) runs one live check against the real CLI in tmux: an isolated broker, the exact files `murmur install` writes (loaded as project config, so your real CLI config is only read for auth), one owner message, and a PASS or FAIL per step judged from the broker's own records. It spends a few cents of model calls.

| Check | What it proves | Last result (2026-10-20) |
| --- | --- | --- |
| `claude` | Two sessions announce with native transports; one message to both goes sent, delivered (tag in each transcript), replied; each reply fans out to the other session and the owner; `/murmur` stores `$(...)`, backticks, quotes, `!` and `$HOME` verbatim | PASS, haiku, replies in about 3.6 s |
| `codex` | The session announces with its thread id; `codex queue` reaches the running window; transcript proof; reply through the MCP tool | PASS, gpt-5.6-luna, 6.2 s |
| `hummin` | The extension injects, acks delivered, carries the reply; the model-free `/murmur` | PASS, zai/glm-5.3-flash, 6.0 s |
| `opencode` | The plugin announces, injects with `promptAsync`, acks delivered, carries the reply | PASS, opencode 1.19.3, opencode/glm-5.3-flash-free, reply in 5.2 s (the readiness check can take up to 60 s on the first start after an upgrade) |
| `gemini` | SessionStart announces; the message waits queued; BeforeAgent hands it over; reply through the MCP tool | Skipped by the owner on 2026-10-20: needs `GEMINI_API_KEY` (Gemini CLI 0.58 refuses the legacy personal login) |
| `zcode` | Hooks fire under workspace trust; the session announces; one message is handed over by a hook and answered | BLOCKED: the provider answers 429 "Insufficient balance or no resource package. Please recharge." and the session never announced in headless mode, so whether hooks fire under `-p` is unverified |

## Troubleshooting

- `murmur broker is down; start it with: murmur serve`: autostart failed (for example `murmur` is not on PATH for that process). Run `murmur serve --daemon`.
- A leftover `bus.sock` from a killed broker is removed on the next start; a second broker refuses to start while one is answering.
- `murmur status` shows who is on the bus, their CLI, and presence: `busy`/`idle` from the last lifecycle event of a running session, `online` for a running session or live connection with no event yet, then `presumed-offline` (seen in the last 5 minutes) or `offline`.
- Install refuses to touch a config file that is not valid JSON; fix or remove it first.
