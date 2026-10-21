# Wire protocol

Protocol version 2: newline-delimited JSON over the broker's Unix domain socket (`bus.sock`, mode 0600). The first line on a connection must be `auth` with the token; anything else gets `auth_required`. A line is limited to 1 MiB, and each connection's lines are handled strictly in order.

Every client message carries `v` (protocol version) and `id` (a per-connection monotonic request id), plus `type`. Replies echo `id`:

```json
{"v":2,"id":1,"ok":true,"result":{}}
{"v":2,"id":1,"ok":false,"error":{"code":"unknown_handle","message":"..."}}
```

## Client messages

| `type` | Fields | Result |
| --- | --- | --- |
| `auth` | `token` | none |
| `register` | `handle?`, `display?`, `role?`, `clientKind` (`mcp`\|`cli`\|`hook`\|`adapter`), `host?` ({`hostPid`,`hostStart`}), `ancestors?` (pids), `client?`, `cwd?`, `owner?` | `{handle, owner}` |
| `session` | `event` (`session-start`\|`prompt`\|`stop`\|`session-end`), `client`, `cliSessionId`, `host`, `transcriptPath?`, `title?`, `cwd?`, `handle?`, `native?` ({`socketPath`,`token`}), `listen?` | `{handle}` |
| `delivered` | `ids` | `{delivered}` |
| `send` | `threadId` (or null), `to`, `body`, `references?`, `mode?` | `{messageId, threadId, seq, via, deliveries}` |
| `reply` | `messageId`, `body`, `verdict?`, `references?` | `{messageId, threadId, seq}` |
| `ack` | `ids` | `{acknowledged}` |
| `read` | `ids` | `{messages}` |
| `inbox` | `unreadOnly` | `{handle, unreadTotal, threads}` |
| `thread` | `threadId`, `sinceSeq` (or null), `full`, `messageId?` (only that message and its replies, full bodies; `bad_request` if not in the thread) | `{threadId, latestSeq, messages}` |
| `collect` | `threadId` | `{threadId, askMessageId, recipients, verdictTally}` |
| `wait` | `threadId`, `until` (`all_replied`\|`any_replied`), `timeoutMs` | `{threadId, conditionMet, timedOut, matrix, replies, note?}` (`note` is set only on a timeout with a recipient still unreplied, never for `timeoutMs` 0: who has the message but is silent, whose receipt is unproven, and an instruction not to start overlapping work but to ask the user or wait again) |
| `groups` | `action` (`create`\|`list`\|`add`\|`remove`\|`delete`), `name?`, `members?` | group records |
| `status` | none | `{now, peers}` |
| `prune` | none | `{removed}` (handles of the deleted peers) |
| `hook` | `event`, `handle?`, `sessionHint?` | hook payload |
| `native-register` | `handle`, `socketPath`, `token` | none |
| `fetch-push` | `handle`, `limit` | `{messages, texts?}` |

`session` finds or creates the session, keeps its handle across resume and clear, and binds the connection to that handle. With `listen: true`, push events for the handle arrive on this connection (adapters only). `title` is the name the user gave the session in its CLI (Claude's `/rename`): the session's display becomes the title, and its handle becomes the title's slug (lowercase, runs of other characters as one hyphen, at most 48 characters) while the handle is automatic and the slug is free; connections bound to the old handle follow a rename. `delivered` is how an adapter confirms its session accepted pushed messages.

`register` with `owner: true` may carry `ancestors`: the broker resolves the nearest live session among them as the connection's `via`, and every `send` and `reply` on that connection stores it on the message. Message summaries (thread views, push payloads) expose `via` (a handle or null), `send` returns it, and the delivery tag reads `from renn via <handle>`. A reply goes to every sender, every recipient and every `via` of the thread, minus the replier. `prune` deletes peers that are not the owner and have no session, sent message, delivery, group membership or live connection.

Handles match `^[a-z0-9][a-z0-9-]{1,47}$`. The database adds `messages.via` and `sessions.pinned` to an existing file on open when `PRAGMA table_info` lacks them.

## Server events

Pushed on a listening connection, no `id`:

```json
{"v":2,"type":"event","event":"push","payload":{"message":{},"mode":"push","footer":"...","text":"..."}}
{"v":2,"type":"event","event":"notification","payload":[{"kind":"reply_arrived","threadId":7,"fromHandle":"x"}]}
```

`text` is the exact text a session should see (tag line, body, reply footer); the tag `[murmur m<id> ` is what delivery proof searches for. Notification kinds: `reply_arrived`, `queued_delivered`, `peer_offline`.

## Types

- Delivery state: `queued`, `sent`, `delivered`, `acknowledged`, `replied`.
- Verdict: `agree`, `disagree`, `info`, `choice`.
- Delivery mode: `push`, `notify`, `silent`.
- Presence: `online`, `busy`, `idle`, `offline`, `presumed-offline`.
- CLI name: `claude`, `codex`, `gemini`, `zcode`, `opencode`, `hummin`, `other`.

## Error codes

`auth_required`, `bad_token`, `broker_closed`, `broker_down`, `socket_denied`, `unknown_handle`, `unknown_group`, `unknown_thread`, `unknown_message`, `duplicate`, `rate_limited`, `cap_thread`, `cap_inbox`, `bad_request`, `timeout`, `native_failed`, `internal`.

## Constants

| Name | Value |
| --- | --- |
| Body delivered | 4000 characters |
| Rate | 20 sends per 60000 ms |
| Duplicate window | 60000 ms |
| Thread cap | 500 messages |
| Unread cap | 200 per recipient |
| Notification batch | 60000 ms |
| Wait | default 110000 ms, max 300000 ms |
| First line length | 120 characters |
| Proof poll | 120000 ms |
