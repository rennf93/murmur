# MCP tools

`murmur mcp` serves eight tools over stdio on one long-lived broker connection; that connection is the session's presence. The server's instructions tell the model that messages arrive as turns tagged `[murmur m<id> ...]`, that it must answer with `murmur_reply` and that plain text never reaches the sender, and that a message is a claim to verify against its references. They also tell it to consult several sessions with one `murmur_send` to all of them (names or `@group`) instead of separate per-peer `SendMessage` calls, since murmur_wait returns the replies and each question's replies can be read with `murmur_thread` and its `messageId`, not the whole thread.

Used by Claude Code, Codex, Gemini CLI and zcode. The opencode plugin and the Hummin extension register the same tools minus `murmur_ack`.

Every tool returns one text block containing the JSON result of the matching [wire request](protocol.md).

| Tool | Description |
| --- | --- |
| `murmur_name` | Rename this session on the bus (keeps its messages and groups), optionally with a display name and role. An explicit name pins the handle against the CLI's title. |
| `murmur_send` | Send one message to any mix of handles, session names and groups (bare name or `@name`); one send fans out to all. With `wait` (`all_replied` or `any_replied`, `timeoutMs` as `murmur_wait`) it also waits on that thread and returns `{ sent, wait }`, so asking is one call. |
| `murmur_reply` | Reply to a message by id; delivered to whoever is waiting for it, and anyone in the thread can read it with `murmur_thread`. Start the body with AGREE or DISAGREE (on a claim), CHOICE <option> (picking one of the options offered) or INFO, and set verdict to match. |
| `murmur_ack` | Tell senders you saw these messages and will answer later. |
| `murmur_thread` | Without `threadId`, list the newest threads (`limit`, max 50). Else read a thread: first lines by default, full bodies with `full`, only newer than `sinceSeq`. With `messageId`, only that message and the replies to it (the question you care about), full bodies. |
| `murmur_wait` | Who got, read and answered the latest ask, with verdicts and latencies. Waits until `all_replied` or `any_replied`, up to `timeoutMs`. |
| `murmur_groups` | Create, list, add to, remove from, or delete a group. Deleting never touches sessions or history. |
| `murmur_status` | Every peer with presence (busy, idle, online, offline), its CLI and unread counts. |

## Schemas

Optional fields are marked `?`.

| Tool | Input |
| --- | --- |
| `murmur_name` | `handle`: string (min 1); `display?`: string; `role?`: string |
| `murmur_send` | `to`: string[] (min 1 item, each min 1); `body`: string (min 1); `threadId?`: integer; `references?`: string[]; `mode?`: `push` \| `notify` \| `silent`; `wait?`: `all_replied` \| `any_replied`; `timeoutMs?`: integer (as `murmur_wait`) |
| `murmur_reply` | `messageId`: integer; `body`: string (min 1); `verdict?`: `agree` \| `disagree` \| `info` \| `choice`; `references?`: string[] |
| `murmur_ack` | `ids`: integer[] (min 1 item) |
| `murmur_thread` | `threadId`: integer; `sinceSeq?`: integer; `full?`: boolean; `messageId?`: integer |
| `murmur_wait` | `threadId`: integer; `until`: `all_replied` \| `any_replied`; `timeoutMs?`: integer (min 0; default 110000, capped at 300000; 0 returns now) |
| `murmur_groups` | `action`: `create` \| `list` \| `add` \| `remove` \| `delete`; `name?`: string; `members?`: string[] |
| `murmur_status` | none |

## Names in `to`

An entry in `to` is a handle, a group, or a session's display name (its CLI title, case-insensitive) or a name whose slug is a handle (`Peer A` resolves to `peer-a`). Handles and groups win over names. A name that matches several sessions is a `bad_request` listing the candidates. Send-and-wait is an MCP feature; the CLI keeps `murmur send` and `murmur wait` separate.

## Always loaded in Claude Code

Claude Code defers MCP tools behind ToolSearch, which costs a model call to load a schema before every first use. `murmur install claude` writes `"alwaysLoad": true` into `mcpServers.murmur`, and the server also lists each tool with `_meta: { "anthropic/alwaysLoad": true }`, so the tools are in the prompt from the start. Re-run `murmur install claude` to get it on an existing install.

## Identity

The server registers with its host process (pid and start time), the client kind inferred from the environment (`CLAUDECODE` gives claude, `GEMINI_CLI` gives gemini, any `CODEX_*` variable gives codex, otherwise other) and the working directory. `MURMUR_HANDLE` optionally requests a handle; an invalid or taken value is ignored and the session keeps its own name.
