# Consult benchmark: native SendMessage versus murmur

Three Claude Code sessions (Implementer, Peer A, Peer B), model `haiku`, Claude Code `2.1.304 (Claude Code)`, run on 2026-10-20, cwd = this repository. Arm native has no murmur MCP and no murmur hooks; arm native-told is arm native with a prompt that also tells each peer to send its answer back with SendMessage (below); arm murmur has what `murmur install claude` writes. Sessions are renamed first (`/rename Implementer`, `/rename Peer A`, `/rename Peer B`), then this prompt (arms native and murmur) is typed into the Implementer only:

> Consult Peer A and Peer B: ask each of them to check whether HANDLE_RE in src/protocol.ts allows handles of up to 48 characters, and to answer AGREE or DISAGREE with the line they checked. Wait for both answers, then tell me the outcome in one line.

Arm native-told gets:

> Consult Peer A and Peer B: ask each of them to check whether HANDLE_RE in src/protocol.ts allows handles of up to 48 characters, and to send their answer back to you with SendMessage, AGREE or DISAGREE with the line they checked. Wait for both answers, then tell me the outcome in one line.

Numbers come from each session's own transcript, counted from the task prompt onward and deduplicated by message id; arm murmur also checks the broker deliveries table. Regenerate with `make bench-consult`. Note: `replies that reached Implementer` counts transcript-visible incoming rows only; arm murmur delivers replies through the broker instead, so that column reads `none` there even though the replies arrive. Arm murmur `completed` is gated on the deliveries table (`deliveriesDb` in consult.json), where every reply shows state `delivered`.

## Arm native

| run | send calls by Implementer | copies A / B | A saw B reply / B saw A reply | replies that reached Implementer | wall s | tokens Implementer | tokens Peer A | tokens Peer B | API calls I / A / B (all) | tokens all (in / cache write / cache read / out) | ToolSearch calls I / A / B | completed | timed out |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | SendMessage x2 (2) | 1 / 1 | no / no | none | 5.7 | 115327 | 76320 | 76347 | 3 / 2 / 2 (7) | 267994 (18 / 15772 / 250918 / 1286) | 0 / 0 / 0 | no | no |
| 2 | SendMessage x2 (2) | 1 / 1 | no / no | Peer A | 13.5 | 199990 | 159727 | 76449 | 5 / 4 / 2 (11) | 436166 (28 / 21650 / 411998 / 2490) | 1 / 1 / 0 | no | no |
| 3 | SendMessage x2 (2) | 1 / 1 | no / no | none | 5.0 | 115384 | 76472 | 76416 | 3 / 2 / 2 (7) | 268272 (18 / 15758 / 250956 / 1540) | 0 / 0 / 0 | no | no |
| median | 2 | 1 / 1 |  |  | 5.7 | 115384 |  |  | 7 | 268272 (18 / 15772 / 250956 / 1540) | 0 / 0 / 0 | 0 of 3 |  |

Median per session (tokens / API calls): Implementer 115384 / 3, Peer A 76472 / 2, Peer B 76416 / 2.

Implementer tool use per run: #1 ListAgents x1, SendMessage x2; #2 ListAgents x1, ToolSearch x1, SendMessage x2; #3 ListAgents x1, SendMessage x2.

## Arm native-told

| run | send calls by Implementer | copies A / B | A saw B reply / B saw A reply | replies that reached Implementer | wall s | tokens Implementer | tokens Peer A | tokens Peer B | API calls I / A / B (all) | tokens all (in / cache write / cache read / out) | ToolSearch calls I / A / B | completed | timed out |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | SendMessage x2 (2) | 1 / 1 | no / no | Peer A, Peer B | 15.0 | 195523 | 157708 | 157726 | 5 / 4 / 4 (13) | 510957 (32 / 22050 / 486597 / 2278) | 0 / 1 / 1 | yes | no |
| 2 | SendMessage x2 (2) | 1 / 1 | no / no | Peer A, Peer B | 13.8 | 195287 | 157841 | 157803 | 5 / 4 / 4 (13) | 510931 (32 / 22223 / 486330 / 2346) | 0 / 1 / 1 | yes | no |
| 3 | SendMessage x2 (2) | 1 / 1 | no / no | Peer A, Peer B | 11.4 | 195237 | 157765 | 157803 | 5 / 4 / 4 (13) | 510805 (34 / 21986 / 486468 / 2317) | 0 / 1 / 1 | yes | no |
| median | 2 | 1 / 1 |  |  | 13.8 | 195287 |  |  | 13 | 510931 (32 / 22050 / 486468 / 2317) | 0 / 1 / 1 | 3 of 3 |  |

Median per session (tokens / API calls): Implementer 195287 / 5, Peer A 157765 / 4, Peer B 157803 / 4.

Implementer tool use per run: #1 ListAgents x1, SendMessage x2, Bash x1; #2 ListAgents x1, SendMessage x2, Bash x1; #3 ListAgents x1, SendMessage x2.

## Arm murmur

| run | send calls by Implementer | copies A / B | A saw B reply / B saw A reply | replies that reached Implementer | wall s | tokens Implementer | tokens Peer A | tokens Peer B | API calls I / A / B (all) | tokens all (in / cache write / cache read / out) | ToolSearch calls I / A / B | completed | timed out |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | mcp__murmur__murmur_send x1 (1) | 1 / 1 | no (db no) / no (db no) | none | 8.7 | 80421 | 120803 | 120698 | 2 / 3 / 3 (8) | 321922 (20 / 16777 / 303586 / 1539) | 0 / 0 / 0 | yes | no |
| 2 | mcp__murmur__murmur_send x1 (1) | 1 / 1 | no (db no) / no (db no) | none | 10.1 | 121109 | 120855 | 120915 | 3 / 3 / 3 (9) | 362879 (22 / 17365 / 343788 / 1704) | 0 / 0 / 0 | yes | no |
| 3 | mcp__murmur__murmur_send x1 (1) | 1 / 1 | no (db no) / no (db no) | none | 11.5 | 121290 | 121172 | 163109 | 3 / 3 / 4 (10) | 405571 (24 / 18541 / 384811 / 2195) | 0 / 0 / 0 | yes | no |
| median | 1 | 1 / 1 |  |  | 10.1 | 121109 |  |  | 9 | 362879 (22 / 17365 / 343788 / 1704) | 0 / 0 / 0 | 3 of 3 |  |

Median per session (tokens / API calls): Implementer 121109 / 3, Peer A 120855 / 3, Peer B 120915 / 3.

Implementer tool use per run: #1 mcp__murmur__murmur_send x1; #2 mcp__murmur__murmur_status x1, mcp__murmur__murmur_send x1; #3 mcp__murmur__murmur_status x1, mcp__murmur__murmur_send x1.

Arm native: completed 0 of 3, median tokens 268272, median API calls 7, median wall time 5.7 s.

Arm native-told: completed 3 of 3, median tokens 510931, median API calls 13, median wall time 13.8 s.

Arm murmur: completed 3 of 3, median tokens 362879, median API calls 9, median wall time 10.1 s.

## Cache split and cost

Component medians from the tables above (three sessions summed per run). Arm native never completed; its row is what the failed attempts cost. Prices: $1 / $1.25 / $0.1 / $5 per MTok (in / cache write / cache read / out; haiku list, 2026-10-20).

| arm | in | cache write | cache read | out | cache read % | cached re-read per call | $ per task |
|---|---|---|---|---|---|---|---|
| native | 18 | 15772 | 250956 | 1540 | 93.5% | 35850.9 | $0.0525 |
| native-told | 32 | 22050 | 486468 | 2317 | 95.2% | 37420.6 | $0.0878 |
| murmur | 22 | 17365 | 343788 | 1704 | 94.7% | 38198.7 | $0.0646 |

Both completed arms sit at ~95% cache reads, re-reading 37420.6 (native-told) and 38198.7 (murmur) cached tokens per API call, so murmur's saving comes from fewer calls, not smaller re-reads. And because the cache-read fraction matches across arms, the dollar saving tracks the token cut: 26% cheaper per task at these prices versus a 29% token cut, a ratio that holds for any price list with similar read and write multipliers.
