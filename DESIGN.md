# murmur design

## Architecture

One broker process owns all state. Everything else is a thin client over one newline-delimited JSON protocol on a single Unix domain socket.

```
Claude session A            Claude session B            zcode session
  |  hooks (3 events)          |  hooks + native socket    |  Bash + skill
  |  + MCP facade (11 tools)   |                           |
  v                            v                           v
  +------------------------ murmur CLI (human) ----------+
                          |            |
                          v            v
                    MurmurCore (policy)  <--- drives --->  Broker (sockets)
                                          |
                                        Store (SQLite, WAL)
                                          |
                                  native transport rows
                                          v
                              /tmp/cc-socks/<pid>.sock  (peer sessions)
```

- **Broker, `src/broker.ts`.** The Unix socket server. Per-connection state machine: the first line must be `auth` against the token file, then every line is one request. A byte-exact guard rejects any line over 1 MiB and destroys the connection. Lines on one connection are dispatched strictly in order (each handler awaits the previous), so a client cannot interleave its own requests. The broker owns the presence map (handle to live connection, bound by `register` and `hook`), executes pushes after sends, batches notifications per recipient on a 60 second flush timer, and keeps per-thread waiters that it re-checks after every mutation. All policy lives in MurmurCore; the broker drives it and relays the returned push targets.
- **Policy core, `src/core.ts`.** `MurmurCore` owns every decision and no sockets: recipient resolution (handles plus `@groups`, order-preserving dedup), rate, duplicate and cap limits, thread and seq allocation, delivery state transitions, ask collection with reply latency percentiles, presence tiers, and groups. The send-rate and duplicate windows are in memory by design: limits reset when the broker restarts, while messages persist.
- **Store, `src/store.ts`.** SQLite via better-sqlite3, WAL mode, fully synchronous. Tables: `peers`, `groups`, `group_members`, `threads`, `messages` (with `UNIQUE (thread_id, seq)` and separate `body` and `delivered_body` columns), `deliveries` (primary key message id plus recipient, one state machine per row), and `native_transports` (per-owner socket path and token, upserted at session-start).
- **MCP facade, `src/mcp.ts`.** Eleven tools (`murmur_register`, `murmur_send`, `murmur_reply`, `murmur_ack`, `murmur_read`, `murmur_inbox`, `murmur_thread`, `murmur_collect`, `murmur_wait`, `murmur_groups`, `murmur_status`), each mapping to one broker request on a single long-lived connection. That connection is the session's presence on the bus: it stays open for the life of the MCP server and the process registers under `MURMUR_HANDLE` (or `anon-<pid>`).
- **CLI, `src/cli.ts`.** `init`, `serve [--daemon]`, `mcp`, `hook`, `send`, `status`, `thread`, `groups`, `install`. Every subcommand is parse argv, connect, request, print. `send` registers as the owner handle `renn` (display `renn (owner)`); the read-side commands connect auth-only and bind no handle.
- **Hooks, `src/hooks.ts`.** One entry point for three events, installed per client. `session-start` reports the event and, when the session exposes `CLAUDE_CODE_MESSAGING_SOCKET` and `CLAUDE_CODE_MESSAGING_TOKEN`, registers that socket as the handle's native transport. `prompt` drains up to 10 pending push-mode messages (queued or sent) and renders them as one UserPromptSubmit `additionalContext` JSON line; empty stdout when there is nothing. `stop` reports idle. A 10 second watchdog bounds the whole interaction and every failure path resolves exit 0 with empty stdout: murmur must never block or fail a session.
- **Native transport, `src/native.ts`.** The broker-to-live-session sender. Connects to the peer session's messaging socket, writes an auth line then a message line (newline-delimited JSON), and resolves ok on a non-error reply line, a non-JSON first line, or a clean close; resolves failed on connect error, write error, an error-object reply, or the 5 second timeout. Never throws.
- **Shared client, `src/client.ts`.** Request/reply with per-connection monotonic ids (auth takes id 1), server events surfaced to an optional handler, and `broker_down` / `broker_closed` classification that the CLI turns into its one troubleshooting line.

A send flows like this: client sends `send` with targets, body, optional mode, references, thread id; core resolves recipients, checks limits (a rejected send consumes no rate slot and no duplicate fingerprint), appends the message in a transaction that allocates `seq`, and inserts one queued delivery per recipient; the broker then executes the returned push targets (native, live connection, or nothing), so delivery states are already settled when the sender sees the ok reply; notifications are batched; waiters are re-checked.

## The delivery state machine

One row per (message, recipient) in `deliveries`. States: `queued`, `sent`, `delivered`, `acknowledged`, `replied`. Queued, sent, and delivered count as unread for the inbox and the inbox cap.

- **`queued` (birth).** Created by `addDeliveries` at send time with state `queued` and every timestamp NULL (`src/store.ts:386`). Proof of state: the row exists; nothing has happened yet, and that is the point.
- **`queued` to `sent`.** Two writers, both inside the broker's push execution, which runs before the send reply (`src/broker.ts:524`):
  - native: a transport row exists and `nativeSend` resolved ok. The broker stamps `sent` and then `delivered` in one breath, waking thread waiters after each hop (`src/broker.ts:707`).
  - connection: the push event line was written to the recipient's live socket (`src/broker.ts:713`).
  Proof: `sentAt` stamped, state `sent`. A vanished or failed native path skips silently and the row stays `queued`; the stored message is never rolled back.
- **`queued` or `sent` to `delivered`.** Three movers: `murmur_read`, which returns full bodies and marks delivered in the same step (`src/core.ts:402`); the hook pull `fetch-push`, which hands out up to 10 pending push-mode messages and marks delivered as it does (`src/core.ts:647`); and the native second hop above. Proof: `deliveredAt` stamped. The queued-to-delivered hop is observable to the sender as a `queued_delivered` notification (`src/broker.ts:664`).
- **`delivered` or `sent` to `acknowledged`.** `murmur_ack` from the recipient; eligibility is a delivery row for that handle in exactly `delivered` or `sent` (`src/core.ts:376`). Proof: `ackedAt` stamped, and the reply's `acknowledged` list echoes precisely the ids that were eligible (`src/broker.ts:572`).
- **any state to `replied`.** The recipient replies to a parent message: core finds the replier's own delivery row on the parent and stamps `replied` with latency measured from the parent's creation (`src/core.ts:355`). No prior-state eligibility check. Proof: `repliedAt` and `latencyMs` stamped; the latency feeds `murmur_collect`'s per-peer p50 and p95 reply latencies (`src/core.ts:62`, `src/core.ts:493`).
- **Waiters.** After every mutation the broker drives (sends, replies, acks, reads, fetch-push, push state hops), it re-evaluates each thread's waiters against the thread's latest ask: `all_replied` wants every recipient delivery replied, `any_replied` at least one; an ask with no recipients never holds (`src/broker.ts:850`). The ask is the newest non-reply message of the thread (`src/store.ts:467`).

## Credits

Murmur ports ideas from three named sources. Everything else in the repo is original to this build.

### roboco A2A

- **Pull inbox with collect-then-mark reads.** Reading is the delivery boundary: `murmur_read` and the hook's `fetch-push` both return full bodies and move the delivery to `delivered` in the same step, so nothing is ever marked without being handed over (`src/core.ts:402`, `src/core.ts:647`).
- **Identical-in-flight dedup.** A send that repeats the same sender, body, and recipient set within 60 seconds is rejected with `duplicate`, which catches agent loops that re-send while waiting (`src/core.ts:243`).
- **Context, not commands.** The prompt hook injects rendered messages as UserPromptSubmit `additionalContext`: prose addressed to the model, with reply instructions in the header, never imperative tool calls the session is forced to run (`src/hooks.ts:100`).
- **Best-effort fan-out that never rolls back the stored message.** Push failures leave the delivery `queued` and move on; the append happens first and nothing ever deletes a stored message to unwind a failed fan-out (`src/broker.ts:691`).

### Claude Code native cross-session messaging

- **Per-session Unix sockets.** Each live Claude session exposes its own messaging socket and token; murmur's session-start hook registers that pair as the handle's native transport, stored per owner in `native_transports` (`src/hooks.ts:82`, `src/store.ts:575`).
- **Loop protections.** The same three guards, at broker scale: per-sender rate limit (20 per 60 seconds), dropped identical repeats (the 60 second duplicate window), and queue caps (500 messages per thread, 200 unread deliveries per inbox).
- **Permission doctrine.** Messages can never approve prompts. A murmur delivery is context a session reads, never a permission decision; hooks carry no approval semantics and exit 0 on every path (`src/hooks.ts:58`).

### The live 3-peer run of 2026-10-19

Three sessions on one bus for a day produced the shape decisions below; each is a direct answer to something that went wrong or went well in that run.

- **Threads instead of relaying.** Peers answer inside one thread id rather than each carrying the conversation to the next session, so no session is a single point of garbling and every participant reads the same ordered history.
- **Verdict enum from unanimous-consent workflows.** The `agree` / `disagree` / `info` verdicts and `murmur_collect`'s tally exist because the run's consent asks needed a scannable, machine-checkable stance per peer instead of prose mining (`src/core.ts:503`).
- **References field from pointer-first messaging.** Messages carry pointers and artifacts stay in their repos, which kept the run's bodies small and its claims checkable (`MessageSummary.references`, `src/protocol.ts:50`).
- **Seq numbers eliminating crossed-message confusion.** Per-thread monotonic `seq`, unique per thread, plus `murmur thread --since <seq>` incremental reads, ended the "which message are you answering" misses of the pre-murmur runs (`src/store.ts:166`, `src/store.ts:359`).

## Spike: the native messaging path

The wire shapes for the native path (`auth` line, `message` line, newline-delimited JSON) are a documented guess from the official docs, and the spike was run to confirm or replace them. Spike outcome, as recorded at `src/native.ts:10`: the peer never emitted a single byte in any probe, against a wrong token, the real token, newline-delimited, NUL-delimited, and delimiter-free writes; no banner, no ack, no error. The reply framing is therefore unobservable, and the evidence does not justify changing the schema, so the documented default stands. The two builder functions at the top of `src/native.ts` are the only framing code, so a future spike outcome is applied by editing those alone.

Status on this machine: the native path is live as the first-choice transport and falls back cleanly. Live per-session sockets exist under `/tmp/cc-socks/` (three present today, 2026-10-19: `11534.sock`, `32691.sock`, `44004.sock`, each `srw-------`), and a session that registers them at session-start gets native pushes with both `sentAt` and `deliveredAt` stamped at send time. Because the spike never saw a reply byte, an ok verdict can rest on the documented heuristics (non-error reply line, non-JSON first line, or clean close, `src/native.ts:105`), so the honest reading is: the send path is exercised and marked delivered, while the peer's parse of the message remains unverified by any observed reply. Any failure (error reply, write error, timeout, vanished transport row) silently leaves the delivery `queued`, and the message surfaces through the prompt hook pull instead, which is the guaranteed floor for every peer kind.
