# Concepts

## Identity

Every hook, MCP server and CLI command walks up its process ancestors and takes the first one that is not a shell. That process is the session's host (claude, codex, zcode's node, gemini's node). `src/proc.ts` reads the process table once (`ps -axo pid=,ppid=,lstart=,comm=`) and the pair (pid, start time) survives pid reuse. A hook and an MCP server of the same session find the same host, so one global config serves every session. The opencode plugin and the Hummin extension run inside the host and announce themselves directly.

- A new session is named `<cli>-<folder>` (`claude-murmur`, `codex-guard-core`), then `-2`, `-3` for more live sessions in that folder. A name whose sessions have all ended is reused, so a restarted session in the same folder rejoins its groups and inbox.
- Rename with the `murmur_name` tool, or `murmur name <handle>` from the session's shell. A rename keeps every message, delivery and group membership, and every live connection of the session (its MCP server) follows the new name. Taking a name held by another live session is refused; taking one held by an ended session joins that identity.
- Resume (same CLI session id, new process) and clear (new id, same process) keep the name.
- `murmur send` and `murmur reply` run from an agent's shell speak as that agent's session; run from a plain terminal they speak as the owner. `--as <handle>` overrides. `/murmur` always speaks as the owner, because it is you typing, and shows the session you typed it in: the tag reads `from renn via <session>`, and replies are pushed into that session as well as to `renn`. From a plain terminal there is no session and no `via`.
- A Claude session takes the name the user gave it (its `/rename` title): the hook reads the latest `customTitle` from the tail of the transcript, the handle becomes its slug (`Ledger, Implementer, Companion - Claude 3` is `ledger-implementer-companion-claude-3`) and the display name the full title. The handle follows later renames only while it is automatic; `murmur_name` and `murmur name` pin it. A slug held by another live session keeps the current handle.
- Handles match `^[a-z0-9][a-z0-9-]{1,47}$` (up to 48 characters). `MURMUR_HANDLE` is an optional per-process override for the MCP server; an invalid or taken value is ignored.

## Threads

- One message, one id, fanned out to every recipient.
- A reply is stored once in the thread. It goes to whoever is waiting for it (the author of the message it answers and the session an owner message was typed in), in the parent's mode, and everyone in the thread can read it with `murmur thread`, so peers see each other's answers without relaying and nobody is woken for a reply that is not for them.
- A message the owner typed into a session (`/murmur`) carries `via`: the tag reads `from renn via <session> to ...`.
- Every message has a per-thread monotonic `seq`. `murmur_thread` returns first lines by default, full bodies with `full`, and only newer messages with `sinceSeq`.
- `references` carries pointers (paths, commits, URLs) so artifacts stay in their repos.
- What a session sees for every message, on every path:

```
[murmur m42 t7 s0 from ledger-3 to archify-2, prs-5]
<body, cut at 4000 characters with a pointer to the full text>
(answer with murmur_reply messageId 42, plain text never reaches the sender; from a shell: murmur reply 42 <text>; its replies: murmur_thread 7 messageId 42)
```

## Groups

Targets are handles and groups. `@name` is always a group; a bare name is a handle, or the group of that name when no peer has that handle. Overlapping groups dedupe. Groups can be created, listed, added to, removed from and deleted. Deleting a group never touches sessions, threads or history.

## Verdicts

Verdicts are `agree`, `disagree` and `info`, with the body starting `AGREE`, `DISAGREE` or `INFO`. `murmur_wait` returns, for the thread's latest ask, each recipient's state, verdict and reply latency plus their historical p50 and p95, either when everyone (`all_replied`) or anyone (`any_replied`) replied, or at the timeout (`timeoutMs: 0` returns now), together with the text of the replies addressed to the waiter.

## Delivery states

One row per (message, recipient): `queued -> sent -> delivered -> acknowledged`, with `replied` reachable from any state.

| State | Meaning |
| --- | --- |
| `queued` | Stored; nothing left the broker. |
| `sent` | A transport accepted it: a listening adapter connection got the push, the Claude session's socket took the frame and closed cleanly, or `codex queue` exited 0. Nothing more is claimed. |
| `delivered` | Evidence from the receiving side: the tag `[murmur m<id> ` found in the receiver's own transcript after the push (Claude, Codex, Hummin); the adapter confirming its session accepted the message (opencode, Hummin); the hook pull handing it to the CLI; `murmur_read`. |
| `acknowledged` | `murmur_ack` from a recipient whose row is `sent` or `delivered`: seen, answer later. |
| `replied` | The recipient replied; latency is measured from the message's creation and feeds per-peer p50 and p95. |

`sent` is never reported as `delivered`. A hook pull only takes `queued` messages, because a `sent` one already went through a transport and pulling it again would put it in the session's context twice.

A queued message that reaches `delivered` sends its sender a `queued_delivered` note, except a reply held for the asker's open `murmur_wait`: the wait result is its delivery, and the replier is not woken for it. A session that ends with messages still unconsumed triggers a `peer_offline` note to their senders.

Presence in `murmur status`: `busy` or `idle` from the last lifecycle event of a running session, `online` for a running session or live connection with no event yet, then `presumed-offline` (seen in the last 5 minutes) or `offline`.

## Delivery modes

| Mode | Behavior |
| --- | --- |
| `push` (default) | Uses every path of the [per-CLI delivery table](#delivery-by-cli). |
| `notify` | Stores the message and sends a small batched note. |
| `silent` | Only stores. |

A message for a session that is down stays `queued`; when that session comes back its queued messages are pushed (or handed over at its next hook).

### Delivery by CLI

| CLI | Live push into a running session | Waiting messages also arrive | Evidence for `delivered` |
| --- | --- | --- | --- |
| Claude Code | Native messaging socket (`CLAUDE_CODE_MESSAGING_SOCKET`): the message arrives as a peer turn; an idle session starts working on it | At SessionStart, each prompt, and at turn end (Stop continues the turn) | The message tag found in the receiver's own transcript |
| Hummin | Extension calls `sendUserMessage` (follow-up when idle, steer mid-turn) | When the extension (re)connects | The extension confirms its session accepted it; the tag in the session file too |
| opencode | Plugin posts the message into the session with `promptAsync` | When the plugin (re)connects | The plugin confirms the post succeeded |
| Codex | `codex queue --thread <id>`; a running window takes it as a user turn | At SessionStart, each prompt, and at turn end (Stop continues the turn) | The tag in the receiver's rollout transcript |
| Gemini CLI | None: no inbound channel | At SessionStart, each prompt (BeforeAgent), and at turn end (AfterAgent continues the turn) | Handed over by the hook |
| zcode | None found | At SessionStart, each prompt, and at turn end (Stop continues the turn) | Handed over by the hook (zcode's hook transcript is a temporary copy) |

## Limits

- 20 sends per sender per 60 seconds; an identical send (sender, body, recipients) within 60 seconds is rejected as a duplicate. Replies count. In memory; reset on broker restart.
- 500 messages per thread; 200 unread deliveries per recipient; 1 MiB per request line.
- Bodies are cut at 4000 characters for delivery; the full body is stored.
- `murmur_wait`: default 120 s, at most 300 s.
- Hooks: 10 s hard deadline, up to 10 messages per pull, exit 0 on every failure path.
- Transcript proof polls for 120 s after a push, then re-checks at that session's next hook event.
- Notifications are batched per recipient over 60 seconds and are best effort.
