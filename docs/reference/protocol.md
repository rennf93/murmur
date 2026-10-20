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
| `session` | `event` (`session-start`\|`prompt`\|`stop`\|`session-end`), `client`, `cliSessionId`, `host`, `transcriptPath?`, `cwd?`, `handle?`, `native?` ({`socketPath`,`token`}), `listen?` | `{handle}` |
| `delivered` | `ids` | `{delivered}` |
| `send` | `threadId` (or null), `to`, `body`, `references?`, `mode?` | `{messageId, threadId, seq, deliveries}` |
| `reply` | `messageId`, `body`, `verdict?`, `references?` | `{messageId, threadId, seq}` |
| `ack` | `ids` | `{acknowledged}` |
| `read` | `ids` | `{messages}` |
| `inbox` | `unreadOnly` | `{handle, unreadTotal, threads}` |
| `thread` | `threadId`, `sinceSeq` (or null), `full` | `{threadId, latestSeq, messages}` |
| `collect` | `threadId` | `{threadId, askMessageId, recipients, verdictTally}` |
| `wait` | `threadId`, `until` (`all_replied`\|`any_replied`), `timeoutMs` | `{threadId, conditionMet, timedOut, matrix}` |
| `groups` | `action` (`create`\|`list`\|`add`\|`remove`\|`delete`), `name?`, `members?` | group records |
| `status` | none | `{now, peers}` |
| `hook` | `event`, `handle?`, `sessionHint?` | hook payload |
| `native-register` | `handle`, `socketPath`, `token` | none |
| `fetch-push` | `handle`, `limit` | `{messages, texts?}` |

`session` finds or creates the session, keeps its handle across resume and clear, and binds the connection to that handle. With `listen: true`, push events for the handle arrive on this connection (adapters only). `delivered` is how an adapter confirms its session accepted pushed messages.

## Server events

Pushed on a listening connection, no `id`:

```json
{"v":2,"type":"event","event":"push","payload":{"message":{},"mode":"push","footer":"...","text":"..."}}
{"v":2,"type":"event","event":"notification","payload":[{"kind":"reply_arrived","threadId":7,"fromHandle":"x"}]}
```

`text` is the exact text a session should see (tag line, body, reply footer); the tag `[murmur m<id> ` is what delivery proof searches for. Notification kinds: `reply_arrived`, `queued_delivered`, `peer_offline`.

## Types

- Delivery state: `queued`, `sent`, `delivered`, `acknowledged`, `replied`.
- Verdict: `agree`, `disagree`, `info`.
- Delivery mode: `push`, `notify`, `silent`.
- Presence: `online`, `busy`, `idle`, `offline`, `presumed-offline`.
- CLI name: `claude`, `codex`, `gemini`, `zcode`, `opencode`, `hummin`, `other`.

## Error codes

`auth_required`, `bad_token`, `broker_closed`, `broker_down`, `unknown_handle`, `unknown_group`, `unknown_thread`, `unknown_message`, `duplicate`, `rate_limited`, `cap_thread`, `cap_inbox`, `bad_request`, `timeout`, `native_failed`, `internal`.

## Constants

| Name | Value |
| --- | --- |
| Body delivered | 4000 characters |
| Rate | 20 sends per 60000 ms |
| Duplicate window | 60000 ms |
| Thread cap | 500 messages |
| Unread cap | 200 per recipient |
| Notification batch | 60000 ms |
| Wait | default 120000 ms, max 300000 ms |
| First line length | 120 characters |
| Proof poll | 120000 ms |
