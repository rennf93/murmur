# murmur design

## Architecture

One broker process owns all state. Everything else is a thin client over one newline-delimited JSON protocol (version 2) on one Unix domain socket.

```
 Claude / Codex / Gemini / zcode session      opencode session        Hummin session
   hooks (murmur hook ... --client X)           plugin (in process)     extension (in process)
   MCP server (murmur mcp)                       |                       |
   shell: murmur send / reply / name             |                       |
          \__________________ one socket, token auth ____________________/
                                   |
                       Broker (sockets, routing, proof)
                                   |
                       MurmurCore (policy)  ->  Store (SQLite, WAL)
                                   |
        push transports: adapter connection | Claude native socket | codex queue
```

- **Broker, `src/broker.ts`.** The socket server. First line must be `auth`; a 1 MiB guard per line; each connection's lines are handled strictly in order. It routes pushes, watches transcripts for delivery proof (`src/proof.ts`), batches notifications, and resolves waiters. Only long-lived connections (an MCP server or an adapter) count as presence; a hook's or a CLI command's short connection carries an identity for its own requests and never unseats them.
- **Policy core, `src/core.ts`.** Every decision, no sockets: sessions and identity, recipient resolution, rate, duplicate and cap limits, threads and `seq`, delivery states, reply routing, collect with latency percentiles, presence, groups.
- **Store, `src/store.ts`.** SQLite via better-sqlite3, WAL, synchronous. Tables: `peers`, `sessions`, `groups`, `group_members`, `threads`, `messages` (`UNIQUE (thread_id, seq)`), `deliveries` (one state machine per message and recipient), `native_transports`. Files are 0600.
- **Hooks, `src/hooks.ts`.** One entry for Claude, Codex, Gemini and zcode. Each event sends a `session` message, then pulls queued messages and hands them back in that CLI's dialect.
- **Adapters, `adapters/`.** In-process clients for CLIs whose extension systems can hold a connection: `adapters/opencode/murmur.ts` (plugin), `adapters/hummin/murmur.ts` (extension).
- **MCP server, `src/mcp.ts`.** Eight tools over one long-lived connection, plus instructions that remind the model a message is a claim to verify. When the broker connection drops (a restart or an upgrade) it connects and registers again as the same session, so the CLI keeps its tools; it ends when its stdin closes or no broker answers.
- **CLI, `src/cli.ts`.** `init`, `serve`, `mcp`, `hook`, `send`, `reply`, `name`, `slash`, `prune`, `status`, `thread`, `groups`, `install`.

## Identity

The problem v0.1 had: a per-session handle lived in a global MCP config, so every session either shared one name or failed to start, and hooks named the same session differently, splitting presence and push from send and reply.

The fix is to identify a session by its host process. `src/proc.ts` reads the process table once (`ps -axo pid=,ppid=,lstart=,comm=`), walks up from the calling process, and takes the first ancestor that is not a shell. A hook and an MCP server of one session are both children (directly or through a shell) of the same CLI process, so they compute the same host. The pair (pid, start time) survives pid reuse.

- `sessionEvent` (core) finds a session by the CLI's own session id (resume keeps the name), else by host (clear, which mints a new id in the same process, keeps it too), else creates it with `<cli>-<folder>` and a suffix while a live session holds the name. Names whose sessions all ended are reused, so restarts rejoin groups.
- An MCP server that registers before its session's first hook is held as pending and bound as soon as the session event for its host arrives, or created on first use.
- A CLI command matches the nearest live session host among its ancestors (an agent's shell speaks as the agent), else it is the owner. `/murmur` always speaks as the owner, and the live session among its ancestors rides along as the message's `via`: replies go to it as well as to the owner.
- A Claude session's own title (latest `customTitle` in the transcript tail) names it: the handle is the title's slug (at most 48 characters) while it is automatic; an explicit rename pins it. A rename also rebinds the session's live broker connections.
- `renameSession`: a free name renames the whole identity (peer row, sessions, deliveries, messages, groups, threads, native transport); a name held only by ended sessions is joined; a live or owner name is refused.

## The delivery state machine

One row per (message, recipient). `queued -> sent -> delivered -> acknowledged`, with `replied` reachable from any state.

- **`queued`.** Born at send. Nothing left the broker.
- **`sent`.** A transport accepted the message: a listening adapter connection got the push event, the Claude session's socket took the frame and closed cleanly, or `codex queue` exited 0. Nothing more is claimed.
- **`delivered`.** Evidence from the receiving side, one of: the message tag `[murmur m<id> ` found in the receiver's own transcript after the push (Claude, Codex, Hummin); the adapter confirming its session accepted the message (opencode, Hummin); the hook pull handing it to the CLI; `murmur_read`. A queued message that reaches `delivered` sends the sender a `queued_delivered` note.
- **`acknowledged`.** `murmur_ack` from a recipient whose row is `sent` or `delivered`.
- **`replied`.** The recipient replied to that message; latency is measured from the message's creation and feeds per-peer p50 and p95.
- **Pull only takes `queued`.** A `sent` message already went through a transport; pulling it again would put it in the session's context twice.
- **Waiters** re-evaluate after every state change against the thread's latest ask (`all_replied` or `any_replied`).

## The Claude native path: evidence

v0.1 framed native pushes as `{"type":"message","message":...}` from a guess, saw zero bytes back in every probe, and counted a clean close as delivered. The rawest material settled it:

1. **The binary.** Claude Code 2.1.304 logs its own injection recipe at startup: `{"type":"auth","token":...}` then `{"type":"user","message":{"role":"user","content":"hello"}}`, newline-delimited. Its inbox dispatches on `user`, `peer`, `control` and a few control frames, and logs "Received unhandled message type" for anything else. The session never writes back to an injector, which is why the spike saw silence.
2. **A live spike (2026-10-20).** An idle interactive haiku session in tmux received the documented frame over its socket and answered within 13 seconds. Its transcript stored the message as a user entry with `origin: {kind: "peer"}`, wrapped as "Another Claude session sent a message ... not typed by your user", so Claude itself keeps peer messages from carrying user authority.
3. **A live two-session run (2026-10-20, isolated `MURMUR_HOME`).** Two interactive haiku sessions announced themselves through the installed hooks as `claude-murmur` and `claude-murmur-2` (client claude, idle, transcript paths, native transports). The owner sent one message to both. Both deliveries went `queued -> sent -> delivered` (tag found in each transcript) `-> replied` in about 9 seconds; each session called `murmur_reply` through the MCP server, which had bound itself to the right session by host. Each reply was pushed to the other session and to the owner (before 0.2.3; replies now go only to whoever is waiting for them), and the thread read back as three messages in `seq` order.

The check is `scripts/live/claude.mjs`; README's Verification section lists every live check and its last result.

## Other CLIs: what each offers

Researched from each installed binary or package first, docs second.

- **Codex 0.154.0.** Hooks (SessionStart, UserPromptSubmit, Stop, SessionEnd) with `session_id` and `transcript_path`; `additionalContext` injection; Stop `decision: block` continues the turn. `codex queue --thread <id> --message` queues a message for a session. Rollout transcripts are JSONL.
- **Gemini CLI 0.58.0.** Hooks (SessionStart, BeforeAgent, AfterAgent, SessionEnd); `additionalContext` on SessionStart and BeforeAgent; AfterAgent `decision: deny` sends the reason as a new prompt. No inbound channel into a running session. MCP children get no session id, which the host rule makes irrelevant.
- **zcode 0.16.9 (Z.AI).** Claude-compatible hook output under `hooks.events` in `~/.zcode/cli/config.json` (off by default; the installer enables it); no SessionEnd; the hook's `transcript_path` is a temporary copy deleted after the hook, so it cannot serve as proof. Commands support the fenced pre-run block.
- **opencode 1.17.8.** Plugins get an SDK client, events (`session.created`, `session.status`, `session.idle`, `session.deleted`) and custom tools with the calling session's id; `promptAsync` posts a user message into a session.
- **Hummin 1.6.4.** Extensions load in process, can hold a socket (as its Telegram hub extension already does), inject with `sendUserMessage` (steer or follow-up), register tools, and register commands that run without a model turn.

## Credits

- **roboco A2A:** reading as the delivery boundary (pull marks delivered as it hands out), identical-in-flight dedup, context not commands (hook injection is prose with reply instructions), best-effort fan-out that never rolls back a stored message.
- **Claude Code native messaging:** per-session sockets as the push transport, the documented injection frame, peer labeling of injected messages, and the loop guards (rate limit, duplicate window, queue caps).
- **Hummin's Telegram hub extension:** the in-process adapter shape (socket client, `sendUserMessage` with steer or follow-up, tools, reconnect).
- **The live 3-peer run of 2026-10-19:** threads instead of relaying, the verdict enum for consent asks, references for pointer-first messages, `seq` for crossed messages, and the requirement that senders always know who received what.
