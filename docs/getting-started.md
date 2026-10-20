# Getting started

Requires Node 22 or newer.

## Install

=== "From npm"

    ```
    npm install -g murmur-it
    ```

    The package is `murmur-it`; the command it installs is `murmur`.

=== "From source"

    ```
    git clone https://github.com/rennf93/murmur.git
    cd murmur
    npm install
    npm run build
    npm link        # puts murmur on PATH
    ```

`npm link` (or a global install) is required: every hook, MCP entry and command that `murmur install` writes runs plain `murmur`.

## Initialize

```
murmur init
```

Creates `~/.murmur` (mode 0700), the token (0600), `murmur.db`, and the owner peer. `MURMUR_HOME` relocates the home for every component.

The broker starts on demand: the first hook, MCP server or CLI command that finds it down starts one in the background. `murmur serve` runs it in the foreground; `murmur serve --daemon` detaches.

## Install into a CLI

```
murmur install claude    # or: codex | gemini | zcode | opencode | hummin
```

Installs are idempotent merges, and every touched file gets a one-time `.bak-murmur` copy first. Install refuses to touch a config file that is not valid JSON.

| CLI | What it writes | Notes |
| --- | --- | --- |
| claude | `~/.claude/settings.json` hooks (SessionStart, UserPromptSubmit, Stop, SessionEnd), `mcpServers.murmur` in `~/.claude.json`, `~/.claude/commands/murmur.md` | Removes the old `CHANGE-ME-per-session` handle placeholder. |
| codex | `~/.codex/hooks.json` (SessionStart, UserPromptSubmit, Stop, SessionEnd), `[mcp_servers.murmur]` in `~/.codex/config.toml`, `~/.codex/prompts/murmur.md` | Codex asks you to trust new hooks once: open `/hooks`. The command is `/prompts:murmur`. |
| gemini | `~/.gemini/settings.json` hooks (SessionStart, BeforeAgent, AfterAgent, SessionEnd) and `mcpServers.murmur`, `~/.gemini/commands/murmur.toml` | |
| zcode | `~/.zcode/cli/config.json` (`hooks.enabled`, SessionStart, UserPromptSubmit, Stop; `mcp.servers.murmur`), `~/.zcode/commands/murmur.md`, `~/.zcode/skills/murmur/SKILL.md` | zcode has no SessionEnd hook. |
| opencode | `~/.config/opencode/plugins/murmur.ts`, `~/.config/opencode/commands/murmur.md` | The plugin is the whole integration (tools, push, presence). |
| hummin | `~/.hummin/agent/extensions/murmur.ts` | The extension is the whole integration, including a model-free `/murmur`. |

Per-CLI detail: [Claude Code](clients/claude.md), [Codex](clients/codex.md), [Gemini CLI](clients/gemini.md), [zcode](clients/zcode.md), [opencode](clients/opencode.md), [Hummin](clients/hummin.md).

## First message

Start two sessions (in different folders or CLIs). Each is named `<cli>-<folder>`, for example `claude-murmur` and `codex-guard-core`. List who is on the bus:

```
murmur status
```

From a plain terminal you speak as the owner:

```
murmur send claude-murmur,codex-guard-core "Which of you owns the migration?" --refs docs/plan.md
```

Each session receives:

```
[murmur m42 t7 s0 from renn to claude-murmur, codex-guard-core]
Which of you owns the migration?
refs: docs/plan.md
(answer with murmur_reply messageId 42, plain text never reaches the sender; from a shell: murmur reply 42 <text>; its replies: murmur_thread 7 messageId 42)
```

The sessions answer with `murmur_reply`, and each reply goes to whoever asked; everyone in the thread can read it. Read the thread with `murmur thread 7`.

## /murmur

`/murmur <group | @group | handle[,more]> <message>` sends as the owner from inside any session; bare `/murmur` lists peers and groups. In Codex the command is `/prompts:murmur`. See [Concepts](concepts.md#identity) for who speaks as whom.
