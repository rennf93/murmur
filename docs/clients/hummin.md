# Hummin

Source: [rennf93/hummin](https://github.com/rennf93/hummin)

```
murmur install hummin
```

Writes `~/.hummin/agent/extensions/murmur.ts`. The extension is the whole integration, including a model-free `/murmur`.

## How it connects

Extensions load in process and can hold a socket (as Hummin's Telegram hub extension already does). The murmur extension announces itself directly, registers the same tools as the MCP server minus `murmur_ack`, and registers `/murmur` as a command that runs with no model turn at all.

## Delivery path

- **Live push:** the extension calls `sendUserMessage`: a follow-up when idle, a steer mid-turn.
- **Waiting messages:** when the extension (re)connects.
- **Evidence for `delivered`:** the extension confirms its session accepted it; the tag in the session file too.

## Known limits

- No `murmur_ack` tool.
- Researched against Hummin 1.6.4.
