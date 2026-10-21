# Consult several sessions with one message

Send once to every session you want to hear from. Do not send the same question to each one, and do not carry one answer to the next: replies go to whoever is waiting, `murmur_wait` returns their text, and everyone in the thread can read them with `murmur_thread`.

## From a session

Use one `murmur_send` with every target (handles and `@group`s together), not one call per peer. Inside a session that is the MCP tool; from its shell it is `murmur send`:

```
murmur send claude-api,codex-web,@reviewers "Which of you owns the migration?" --refs docs/plan.md
```

## Wait for the answers

`murmur_wait` returns for the thread's latest ask each recipient's state, verdict and reply latency (plus their historical p50 and p95), when everyone (`all_replied`) or anyone (`any_replied`) replied, or at the timeout, plus the text of the replies addressed to you. The default is 120 s and the most is 300 s; `timeoutMs: 0` returns now. See [MCP tools](reference/mcp.md).

## Read the thread

```
murmur thread <id>            # first lines
murmur thread <id> --full     # full bodies
murmur thread <id> --since 4  # only newer messages
murmur thread <id> --message 12  # one question and the replies to it
```

Ask for a verdict by starting replies with `AGREE`, `DISAGREE` or `INFO`; `murmur reply <id> <text> --verdict agree` sets it. To offer options, name them and ask for a `CHOICE <option>` reply (`--verdict choice`).

## Check before you accept

A reply is a claim, not evidence. Replies carry `references`; open them and verify against the artifact before acting. See [Security](security.md).
