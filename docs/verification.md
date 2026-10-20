# Verification

## Tests and coverage

- `make test` builds and runs the suite.
- `make coverage` enforces 100 percent statements, branches, functions and lines (`vitest.config.ts` sets the thresholds). CI runs it on ubuntu and macOS with Node 22 and 24.

## Benchmark

`node scripts/bench.mjs dist` prints p50 and p95 for a hook run, a CLI send, a 10-way fan-out, status over 50 sessions and a pull over 100k historical deliveries, plus burst throughput.

On 2026-10-20 (Apple M2 Pro): hook 52 ms, send 74 ms, fan-out 0.33 ms, status 22 ms, pull 0.04 ms, 6900 sends/s.

## Live checks

`make verify CLI=<cli>` with cli one of claude, codex, gemini, hummin, opencode, zcode (or `node scripts/live/<cli>.mjs [model]`) runs one live check against the real CLI in tmux: an isolated broker, the exact files `murmur install` writes (loaded as project config, so your real CLI config is only read for auth), one owner message, and a PASS or FAIL per step judged from the broker's own records. It spends a few cents of model calls.

| Check | What it proves | Last result (2026-10-20) |
| --- | --- | --- |
| `claude` | Two sessions announce with native transports; one message to both goes sent, delivered (tag in each transcript), replied; each reply reaches the owner who asked and not the other session; `/murmur` stores `$(...)`, backticks, quotes, `!` and `$HOME` verbatim | PASS, haiku, replies in about 3.6 s |
| `codex` | The session announces with its thread id; `codex queue` reaches the running window; transcript proof; reply through the MCP tool | PASS, gpt-5.6-luna, 6.2 s |
| `hummin` | The extension injects, acks delivered, carries the reply; the model-free `/murmur` | PASS, zai/glm-5.3-flash, 6.0 s |
| `opencode` | The plugin announces, injects with `promptAsync`, acks delivered, carries the reply | PASS, opencode 1.19.3, opencode/glm-5.3-flash-free, reply in 5.2 s (the readiness check can take up to 60 s on the first start after an upgrade) |
| `gemini` | SessionStart announces; the message waits queued; BeforeAgent hands it over; reply through the MCP tool | Skipped by the owner on 2026-10-20: needs `GEMINI_API_KEY` (Gemini CLI 0.58 refuses the legacy personal login) |
| `zcode` | Hooks fire under workspace trust; the session announces; one message is handed over by a hook and answered | BLOCKED: the provider answers 429 "Insufficient balance or no resource package. Please recharge." and the session never announced in headless mode, so whether hooks fire under `-p` is unverified |

## Evidence behind the Claude native path

The binary's own injection recipe, a live single-session spike, and a live two-session run settled the frame format. The write-up is in the repository's [DESIGN.md](https://github.com/rennf93/murmur/blob/master/DESIGN.md).

### zcode check notes

`scripts/live/zcode.mjs` uses headless `-p` prompts, because the `zcode.cjs` in the app bundle fails to start its TUI ("Cannot find package '@zcode/tui'"). It sets `ZCODE_BUILTIN_PROVIDER_CONFIG_FILE` to `<app>/Contents/Resources/config/provider/zcode-builtin.json`, since without it `zcode.cjs` cannot locate its provider config. It grants workspace hook trust with `zcode hooks trust grant --workspace <dir> --all-current --bundle-digest <d>`, revokes it afterwards, and deletes the trust store if the check created it.
