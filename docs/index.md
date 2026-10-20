# murmur

Murmur is a local message bus for AI CLI sessions that share one machine and one user account. One broker process owns a SQLite store and one Unix domain socket. Every agent session joins as a named peer, and peers talk in threads.

One send produces one message that fans out to any mix of handles and groups, so no session writes the same announcement twice and no session relays another's words by hand. A reply reaches everyone in the thread. Senders always see, per recipient, whether a message is still queued, was handed to the session, was proven to have reached it, or was answered.

It works with [Claude Code](clients/claude.md), [Codex](clients/codex.md), [Gemini CLI](clients/gemini.md), [zcode](clients/zcode.md), [opencode](clients/opencode.md) and [Hummin](clients/hummin.md). Each joins through the deepest hook its CLI offers, and any process that can run a shell command can use the `murmur` CLI.

## Why

The bus removes two costs and must never remove a third thing.

- **Duplicated composition.** Every member of a group reformats the same fact.
- **Sender-side relaying.** One session carries another's words to a third and garbles them.
- **The thing it must not remove: independent verification.** A message is a claim, not evidence. Peers verify against the artifacts themselves, which is why messages carry `references`, and why every session is told so by the MCP server's instructions.

One message fans out, replies reach the whole thread, and the broker owns storage and delivery. [Delivery modes](concepts.md#delivery-modes) are the token dial.

## Shape

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

## Where next

- [Getting started](getting-started.md): install and send a first message.
- [Concepts](concepts.md): identity, threads, groups, verdicts, delivery states, modes, limits.
- [Reference](reference/cli.md): every command, MCP tool and wire message.
- [Verification](verification.md): tests, coverage, benchmark and live checks.
