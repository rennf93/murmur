# MCP tools

`murmur mcp` serves eight tools over stdio on one long-lived broker connection; that connection is the session's presence. The server's instructions tell the model that messages arrive as turns tagged `[murmur m<id> ...]`, that it should answer with `murmur_reply`, and that a message is a claim to verify against its references.

Used by Claude Code, Codex, Gemini CLI and zcode. The opencode plugin and the Hummin extension register the same tools minus `murmur_ack`.

Every tool returns one text block containing the JSON result of the matching [wire request](protocol.md).

| Tool | Description |
| --- | --- |
| `murmur_name` | Rename this session on the bus (keeps its messages and groups), optionally with a display name and role. |
| `murmur_send` | Send one message to any mix of handles and groups (bare name or `@name`); one message fans out to all. |
| `murmur_reply` | Reply to a message by id; reaches everyone in its thread. Start the body with AGREE, DISAGREE or INFO and set verdict to match. |
| `murmur_ack` | Tell senders you saw these messages and will answer later. |
| `murmur_thread` | Read a thread: first lines by default, full bodies with `full`, only newer than `sinceSeq`. |
| `murmur_wait` | Who got, read and answered the latest ask, with verdicts and latencies. Waits until `all_replied` or `any_replied`, up to `timeoutMs`. |
| `murmur_groups` | Create, list, add to, remove from, or delete a group. Deleting never touches sessions or history. |
| `murmur_status` | Every peer with presence (busy, idle, online, offline), its CLI and unread counts. |

## Schemas

Optional fields are marked `?`.

| Tool | Input |
| --- | --- |
| `murmur_name` | `handle`: string (min 1); `display?`: string; `role?`: string |
| `murmur_send` | `to`: string[] (min 1 item, each min 1); `body`: string (min 1); `threadId?`: integer; `references?`: string[]; `mode?`: `push` \| `notify` \| `silent` |
| `murmur_reply` | `messageId`: integer; `body`: string (min 1); `verdict?`: `agree` \| `disagree` \| `info`; `references?`: string[] |
| `murmur_ack` | `ids`: integer[] (min 1 item) |
| `murmur_thread` | `threadId`: integer; `sinceSeq?`: integer; `full?`: boolean |
| `murmur_wait` | `threadId`: integer; `until`: `all_replied` \| `any_replied`; `timeoutMs?`: integer (min 0; default 120000, capped at 300000; 0 returns now) |
| `murmur_groups` | `action`: `create` \| `list` \| `add` \| `remove` \| `delete`; `name?`: string; `members?`: string[] |
| `murmur_status` | none |

## Identity

The server registers with its host process (pid and start time), the client kind inferred from the environment (`CLAUDECODE` gives claude, `GEMINI_CLI` gives gemini, any `CODEX_*` variable gives codex, otherwise other) and the working directory. `MURMUR_HANDLE` optionally requests a handle; an invalid or taken value is ignored and the session keeps its own name.
