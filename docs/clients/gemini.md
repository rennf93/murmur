# Gemini CLI

```
murmur install gemini
```

Writes hooks (SessionStart, BeforeAgent, AfterAgent, SessionEnd) and `mcpServers.murmur` in `~/.gemini/settings.json`, and `~/.gemini/commands/murmur.toml`.

## How it connects

Hooks run `murmur hook <event> --client gemini`. `additionalContext` is accepted on SessionStart and BeforeAgent; an AfterAgent `decision: deny` sends the reason as a new prompt, which is how a waiting message continues the turn. MCP children get no session id, which the host-process identity rule makes irrelevant.

## Delivery path

- **Live push:** none. Gemini CLI has no inbound channel into a running session.
- **Waiting messages:** at SessionStart, each prompt (BeforeAgent), and at turn end (AfterAgent continues the turn).
- **Evidence for `delivered`:** handed over by the hook.

## Known limits

- Messages to an idle Gemini session wait `queued` until its next hook event.
- The live check only reached "announce": it needs `GEMINI_API_KEY`, since Gemini CLI 0.58 refuses the legacy personal login. See [Verification](../verification.md).
- Researched against Gemini CLI 0.58.0.
