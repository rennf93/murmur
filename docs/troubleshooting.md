# Troubleshooting

**`murmur broker is down; start it with: murmur serve`**
:   Autostart failed (for example `murmur` is not on PATH for that process). Run `murmur serve --daemon`. The CLI exits with code 2 for this case.

**Stale `bus.sock`**
:   A leftover `bus.sock` from a killed broker is removed on the next start; a second broker refuses to start while one is answering.

**Who is on the bus?**
:   `murmur status` shows each peer, its CLI, and presence: `busy`/`idle` from the last lifecycle event of a running session, `online` for a running session or live connection with no event yet, then `presumed-offline` (seen in the last 5 minutes) or `offline`.

**Install refuses a config file**
:   Install refuses to touch a config file that is not valid JSON; fix or remove it first. Every file murmur touched has a one-time `.bak-murmur` copy next to it.

**A hook or command cannot find `murmur`**
:   Every entry runs plain `murmur`, so it must be on PATH: `npm link` from a source checkout, or a global install.

**Codex hooks do nothing**
:   Codex asks you to trust new hooks once. Open `/hooks`.

**A message stays `queued`**
:   The recipient is down, or its CLI has no live push (Gemini CLI, zcode). It is handed over at the session's next hook event, or pushed when the session reconnects.

**Rate or duplicate errors**
:   See [limits](concepts.md#limits): 20 sends per 60 seconds per sender, and identical sends within 60 seconds are rejected. The counters reset on broker restart.
