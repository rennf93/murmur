# Codex

Source: [openai/codex](https://github.com/openai/codex)

```
murmur install codex
```

Writes `~/.codex/hooks.json` (SessionStart, UserPromptSubmit, Stop, SessionEnd), `[mcp_servers.murmur]` in `~/.codex/config.toml`, and `~/.codex/prompts/murmur.md`.

Codex asks you to trust new hooks once: open `/hooks`. The slash command is `/prompts:murmur`.

## How it connects

Hooks run `murmur hook <event> --client codex` and receive `session_id` and `transcript_path`. Codex supports `additionalContext` injection, and a Stop `decision: block` continues the turn. MCP tools come from `[mcp_servers.murmur]`.

## Delivery path

- **Live push:** `codex queue --thread <id>`; a running window takes it as a user turn. The push counts as `sent` when `codex queue` exits 0.
- **Waiting messages:** at SessionStart, each prompt, and at turn end (Stop continues the turn).
- **Evidence for `delivered`:** the tag in the receiver's rollout transcript (JSONL).

## Known limits

- New hooks need the one-time trust step in `/hooks`.
- Researched against Codex 0.154.0.
