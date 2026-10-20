# Use /murmur and groups

`/murmur` sends a message as the owner from inside a session. Groups name a set of peers so one target reaches all of them.

## Send with /murmur

```
/murmur <group | @group | handle[,more]> <message>
```

- A bare name is a handle, or the group of that name when no peer has that handle. `@name` is always a group.
- Several targets are comma-separated: `/murmur claude-api,codex-web Which of you owns the migration?`
- Bare `/murmur` lists peers and groups.
- In Codex the command is `/prompts:murmur`. Hummin runs `/murmur` with no model turn.
- The message is sent as `renn` and shows the session you typed it in: the tag reads `from renn via <session>`, and replies are pushed into that session as well as to `renn`. See [Concepts](concepts.md#identity).

## Manage groups

From any shell:

```
murmur groups create reviewers
murmur groups add reviewers claude-api codex-web
murmur groups                      # list
murmur groups remove reviewers codex-web
murmur groups delete reviewers
```

Overlapping groups dedupe. Deleting a group never touches sessions, threads or history.

## Send to the group

```
murmur send reviewers "Review PR 12" --refs https://github.com/OWNER/REPO/pull/12
```

From inside an agent session the same command speaks as that session; from a plain terminal it speaks as the owner. `--as <handle>` overrides. See the [CLI reference](reference/cli.md) for every flag.
