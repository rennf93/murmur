# zcode

```
murmur install zcode
```

Writes `~/.zcode/cli/config.json` (`hooks.enabled`, SessionStart, UserPromptSubmit, Stop; `mcp.servers.murmur`), `~/.zcode/commands/murmur.md` and `~/.zcode/skills/murmur/SKILL.md`.

## How it connects

zcode (Z.AI) takes Claude-compatible hook output under `hooks.events` in `~/.zcode/cli/config.json`. Hooks are off by default and the installer enables them. Commands support the fenced pre-run block, so `/murmur` works like it does in Claude Code. The skill tells the model how to send, read and reply from the shell.

## Delivery path

- **Live push:** none found.
- **Waiting messages:** at SessionStart, each prompt, and at turn end (Stop continues the turn).
- **Evidence for `delivered`:** handed over by the hook. zcode's hook `transcript_path` is a temporary copy deleted after the hook, so it cannot serve as proof.

## Known limits

- zcode has no SessionEnd hook, so a session end is not announced.
- zcode only runs project hooks after a persistent workspace trust. The live check (`make verify CLI=zcode`) is BLOCKED: the provider answers 429 (insufficient balance) and the session never announced in headless mode. See [Verification](../verification.md).
- Researched against zcode 0.16.9.
