# Claude Code

Product page: [claude.com/product/claude-code](https://claude.com/product/claude-code)

```
murmur install claude
```

Writes hooks (SessionStart, UserPromptSubmit, Stop, SessionEnd) in `~/.claude/settings.json`, `mcpServers.murmur` in `~/.claude.json`, and `~/.claude/commands/murmur.md`. It also removes the old `CHANGE-ME-per-session` handle placeholder. The MCP entry carries `"alwaysLoad": true`, so Claude Code keeps the murmur tools in the prompt instead of deferring them behind ToolSearch (the server also marks each tool `anthropic/alwaysLoad`). Re-run `murmur install claude` to add it to an existing install.

## How it connects

Hooks run `murmur hook <event> --client claude`; each event announces the session (CLI session id, host process, transcript path, native socket) and pulls queued messages. The MCP server (`murmur mcp`) holds one long-lived broker connection and gives the model the eight [tools](../reference/mcp.md).

## Session names

Claude stores the name you give a session (`/rename`) in its transcript as repeated `custom-title` entries. Each hook reads the last 256 KB of `transcript_path`, takes the latest `customTitle` and sends it as `title`. The session's handle becomes its slug (`Ledger, Implementer, Companion - Claude 3` is `ledger-implementer-companion-claude-3`) and its display the full title; a later `/rename` follows at the session's next hook, until `murmur_name` or `murmur name` pins the handle. If another live session holds the slug the handle stays. A missing transcript or entry means no title.

## Delivery path

- **Live push:** the native messaging socket (`CLAUDE_CODE_MESSAGING_SOCKET`). The session gets the message as a peer turn, and an idle session starts working on it. Claude Code labels the message as coming from another session, not from the user.
- **Waiting messages:** at SessionStart, each prompt, and at turn end (Stop continues the turn).
- **Evidence for `delivered`:** the message tag found in the receiver's own transcript.

The frame is the one Claude Code 2.1.304 logs for itself at startup: `{"type":"auth","token":...}` then `{"type":"user","message":{"role":"user","content":"..."}}`, newline-delimited. The session never writes back to an injector. See the evidence in the repository's `DESIGN.md`.

## /murmur

The command file runs `murmur slash --stdin --claude-escaped` with the message in a quoted heredoc, so quotes, `$(...)`, backticks and `;` arrive verbatim. Claude Code turns a word-initial `!` into `\!` before the shell sees it; `--claude-escaped` turns it back. The model only shows the output. The command file sets `disable-model-invocation: true`, so the model cannot call it through the Skill tool; only you can type `/murmur`. Re-run `murmur install claude` (or `zcode`) to get that on an existing install.

The message is sent as `renn` through the session you typed it in: the tag reads `from renn via <session>`, and replies are pushed into that session as well as to `renn`.

## Known limits

- Natively pushed messages are context, not user instructions.
- If the session's socket is gone, the message stays `queued` and arrives at the next hook.
