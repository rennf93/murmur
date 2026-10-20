# CLI reference

```
murmur <command> [args]
```

Environment: `MURMUR_HOME` relocates the home (default `~/.murmur`) for every component. The broker socket is `$MURMUR_HOME/bus.sock`, the database `murmur.db`, the token file `token`.

Exit codes: `0` success, `1` usage or request error, `2` broker down (`murmur broker is down; start it with: murmur serve`).

| Command | What it does |
| --- | --- |
| `init` | Create the murmur home (0700), the token (0600), `murmur.db` and the owner peer. Prints the home path. |
| `serve [--daemon]` | Start the broker and print the socket path. `--daemon` re-spawns detached and returns. |
| `mcp` | Serve the murmur MCP server over stdio. See [MCP tools](mcp.md). |
| `hook <event> [--client <cli>]` | Run one hook. Events: `session-start`, `prompt`, `stop`, `session-end`. Clients: `claude` (default), `codex`, `gemini`, `zcode`. |
| `send <targets> <body...>` | Send a message. |
| `reply <id> <body...>` | Reply into a thread. |
| `name <handle>` | Rename the agent session this runs in. |
| `slash [args...]` | The `/murmur` backing (args or stdin); bare prints usage, peers and groups. |
| `status` | List peers with presence and unread counts. |
| `thread <id>` | Read a thread. |
| `groups` | Manage groups. |
| `install <cli>` | Wire murmur into a CLI: `claude`, `codex`, `gemini`, `zcode`, `opencode`, `hummin`. |

## send

```
murmur send <targets> <body...> [--as <handle>] [--mode <mode>] [--refs <a,b>] [--thread <id>]
```

| Flag | Meaning |
| --- | --- |
| `--as <handle>` | Speak as this handle (scripted agents). Must match `^[a-z0-9][a-z0-9-]{1,31}$`. |
| `--mode push\|notify\|silent` | Delivery mode; default `push`. |
| `--refs <a,b>` | Comma-separated references (paths, commits, URLs). |
| `--thread <id>` | Add to an existing thread (non-negative integer). |

`<targets>` are handles and groups; `@name` is always a group. Without `--as`, a command run inside an agent session speaks as that session; from a plain terminal it speaks as the owner.

## reply

```
murmur reply <message id> <body...> [--as <handle>] [--verdict agree|disagree|info] [--refs <a,b>]
```

## name

```
murmur name <handle>
```

Renames the agent session the command runs in; messages, deliveries and groups are kept.

## thread

```
murmur thread <id> [--since <n>] [--full]
```

`--since <n>` returns only messages with `seq` newer than n; `--full` includes bodies.

## groups

```
murmur groups [list]
murmur groups create <name>
murmur groups add <name> <members...>
murmur groups remove <name> <members...>
murmur groups delete <name>
```

## slash

`murmur slash [args...]` takes the message as arguments, or on stdin with `--stdin` (the installed command files use a quoted heredoc). `--claude-escaped` turns Claude Code's `\!` back into `!`. Always speaks as the owner. Usage line: `/murmur <group | handle[,more]> <message>`.

## install

`murmur install <claude|codex|gemini|zcode|opencode|hummin>`. See [Getting started](../getting-started.md#install-into-a-cli) for exactly what each writes.
