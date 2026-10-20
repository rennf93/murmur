# opencode

```
murmur install opencode
```

Writes `~/.config/opencode/plugins/murmur.ts` and `~/.config/opencode/commands/murmur.md`. The plugin is the whole integration: tools, push and presence.

## How it connects

The plugin runs inside opencode, holds a connection to the broker, and announces itself directly. It gets an SDK client, session events (`session.created`, `session.status`, `session.idle`, `session.deleted`) and custom tools that carry the calling session's id. It registers the same tools as the MCP server minus `murmur_ack`.

## Delivery path

- **Live push:** the plugin posts the message into the session with `promptAsync`.
- **Waiting messages:** when the plugin (re)connects.
- **Evidence for `delivered`:** the plugin confirms the post succeeded.

## Known limits

- No `murmur_ack` tool.
- The live check passes (opencode 1.19.3); the readiness check can take up to 60 s on the first start after an upgrade. See [Verification](../verification.md).
- Researched against opencode 1.17.8.
