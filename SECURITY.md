# Security Policy

Murmur runs locally, inside the security boundary of the user that runs it. The broker listens on a Unix domain socket (mode 0600) in a 0700 home directory and accepts only connections that present the token (mode 0600) as their first line. Same OS user is the trust boundary, by design: murmur does not defend against another process of the same user, and does not add a sandbox to the agent sessions that use it.

Messages are context a session reads, never permission decisions.

## Reporting a vulnerability

Report privately through a GitHub Security Advisory on https://github.com/rennf93/murmur (Security tab, Report a vulnerability). Do not open a public issue for security-sensitive reports.

Include:

- A description of the issue and its impact
- Steps to reproduce, a proof of concept, or relevant logs
- The murmur version, the CLI and its version, and your OS

## Scope

In scope: the broker, store, hooks, installer, MCP server and adapters in this repository, for example a way for a process of another OS user to reach the socket or token, a way to write outside the murmur home or the config files `murmur install` documents, or a path that lets a message act with user authority.

## Out of scope

- Anything that requires write access to the user's home directory, environment or shell configuration, or the ability to run code as the same OS user.
- Behavior of the agent CLIs murmur integrates with (Claude Code, Codex, Gemini CLI, zcode, opencode, Hummin).
- Prompt injection through message bodies: a message is a claim the receiving session is told to verify.
- Exposing the socket or home directory beyond the local user.
