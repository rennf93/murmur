/**
 * murmur: join a hummin session to the murmur local message bus.
 *
 * murmur (https://github.com/rennf93/murmur) is a client-agnostic bus for AI
 * CLI sessions: group threads, delivery states, verdicts, presence. This is
 * the hummin client, shaped like hummin-tg-hub (one long-lived broker socket
 * per session, reconnect, model tools), speaking murmur wire protocol v2
 * (newline-delimited JSON over $MURMUR_HOME/bus.sock):
 *
 *   session_start     -> auth + session-start announce -> {handle}
 *   agent_start/end   -> prompt / stop lifecycle events (busy / idle)
 *   push event        -> sendUserMessage(payload.text verbatim, steer when
 *                        busy, followUp when idle) then a `delivered` frame
 *   notification evt  -> one "[murmur notice]" line, only when idle
 *   murmur_* tools    -> one request each on the same connection
 *   /murmur           -> model-free: short cli connection that sends, or
 *                        (bare) shows peers and groups
 *   session_shutdown  -> session-end + close
 *
 * Config: MURMUR_HOME (default ~/.murmur) holds bus.sock and token.
 * Murmur failures never throw into the session; the connection retries
 * every 5 seconds while the session lives.
 */

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { connect, type Socket } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";
import { Type } from "typebox";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

// ---------------------------------------------------------------------------
// config

const HOME = process.env.MURMUR_HOME ?? join(homedir(), ".murmur");
const SOCKET = join(HOME, "bus.sock");
const TOKEN_FILE = join(HOME, "token");
const PROTOCOL = 2;
const RECONNECT_MS = 5000;
const REQUEST_TIMEOUT_MS = 30_000;

type Json = Record<string, unknown>;
interface Host { hostPid: number; hostStart: string }

// ---------------------------------------------------------------------------
// socket client (request/reply by id, events to a callback)

interface Client {
	request(body: Json, timeoutMs?: number): Promise<unknown>;
	close(): void;
}

function openClient(onEvent: (f: Json) => void, onClose: () => void): Promise<Client> {
	return new Promise((resolve, reject) => {
		let buf = "";
		let nextId = 1;
		let up = false;
		const pending = new Map<number, { ok: (r: unknown) => void; fail: (e: Error) => void }>();
		const sock: Socket = connect(SOCKET);

		function failAll(e: Error): void {
			for (const p of pending.values()) p.fail(e);
			pending.clear();
		}

		const client: Client = {
			request(body, timeoutMs = REQUEST_TIMEOUT_MS) {
				return new Promise((ok, fail) => {
					const id = nextId++;
					const timer = setTimeout(() => {
						pending.delete(id);
						fail(new Error("murmur request timed out"));
					}, timeoutMs);
					pending.set(id, {
						ok: (r) => (clearTimeout(timer), ok(r)),
						fail: (e) => (clearTimeout(timer), fail(e)),
					});
					sock.write(JSON.stringify({ v: PROTOCOL, id, ...body }) + "\n", (err) => {
						if (err && pending.delete(id)) (clearTimeout(timer), fail(err));
					});
				});
			},
			close() {
				sock.end();
			},
		};

		sock.on("data", (b) => {
			buf += b.toString();
			let nl: number;
			while ((nl = buf.indexOf("\n")) >= 0) {
				const line = buf.slice(0, nl);
				buf = buf.slice(nl + 1);
				let f: Json;
				try {
					f = JSON.parse(line) as Json;
				} catch {
					continue;
				}
				if (f.type === "event") {
					try {
						onEvent(f);
					} catch {
						// a bad event must not kill the reader
					}
				} else if (typeof f.id === "number") {
					const p = pending.get(f.id);
					if (!p) continue;
					pending.delete(f.id);
					if (f.ok === true) p.ok(f.result);
					else {
						const err = f.error as { code?: string; message?: string } | undefined;
						p.fail(new Error(`${err?.code ?? "error"}: ${err?.message ?? "murmur request failed"}`));
					}
				}
			}
		});
		sock.on("error", (e) => {
			if (!up) reject(e);
			failAll(e);
		});
		sock.on("close", () => {
			failAll(new Error("murmur connection closed"));
			if (up) onClose();
		});
		sock.on("connect", () => {
			client
				.request({ type: "auth", token: readFileSync(TOKEN_FILE, "utf8").trim() })
				.then(() => {
					up = true;
					resolve(client);
				})
				.catch((e) => {
					sock.destroy();
					reject(e);
				});
		});
	});
}

function readHost(): Host {
	// hostStart must match what the broker computes: ps lstart under LC_ALL=C
	let hostStart = "";
	try {
		hostStart = execFileSync("ps", ["-o", "lstart=", "-p", String(process.pid)], {
			env: { ...process.env, LC_ALL: "C" },
			encoding: "utf8",
		}).trim();
	} catch {
		// broker will treat the host as unverifiable
	}
	return { hostPid: process.pid, hostStart };
}

function text(s: string, isError = false): { content: { type: "text"; text: string }[]; details: undefined; isError?: boolean } {
	return { content: [{ type: "text", text: s }], details: undefined, ...(isError ? { isError } : {}) };
}

function fmtDeliveries(r: Json): string {
	const lines = ((r.deliveries as { recipient: string; state: string }[] | undefined) ?? []).map(
		(d) => `${d.recipient}: ${d.state}`,
	);
	lines.push(`message ${String(r.messageId)} thread ${String(r.threadId)} seq ${String(r.seq)}`);
	return lines.join("\n");
}

function fmtOverview(status: Json, groups: unknown): string {
	const peers = (status.peers as Json[] | undefined) ?? [];
	const lines = peers.map(
		(p) => `${String(p.handle)} ${String(p.presence)} ${String(p.client ?? "-")} unread ${String(p.unread)}`,
	);
	const gs = Array.isArray(groups) ? (groups as Json[]) : [];
	for (const g of gs) lines.push(`${String(g.name)}: ${((g.members as string[]) ?? []).join(", ")}`);
	return lines.join("\n") || "no peers or groups";
}

// ---------------------------------------------------------------------------

export default function murmur(pi: ExtensionAPI): void {
	let client: Client | undefined;
	let closed = false; // session shut down - stop reconnecting
	let busy = false;
	let handle = "";
	let announcedHandle = false;
	let sessionId = "";
	let sessionCtx: ExtensionContext | undefined;
	let host: Host | undefined;
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

	function sessionFrame(event: string): Json {
		const cwd = sessionCtx?.cwd ?? process.cwd();
		const frame: Json = {
			type: "session",
			event,
			client: "hummin",
			cliSessionId: sessionId,
			host,
			cwd,
			listen: true,
		};
		const file = sessionCtx?.sessionManager.getSessionFile();
		if (file) frame.transcriptPath = file;
		return frame;
	}

	function quiet(p: Promise<unknown> | undefined): void {
		p?.catch(() => {});
	}

	function onEvent(f: Json): void {
		const payload = f.payload as Json | Json[] | undefined;
		if (f.event === "push" && payload && !Array.isArray(payload)) {
			const message = payload.message as { id?: number } | undefined;
			if (typeof payload.text !== "string" || typeof message?.id !== "number") return;
			const deliverAs = sessionCtx?.isIdle() === false ? "steer" : "followUp";
			try {
				pi.sendUserMessage(payload.text, { deliverAs, expandPromptTemplates: false });
			} catch {
				return; // not injected: leave it undelivered so the broker can retry
			}
			quiet(client?.request({ type: "delivered", ids: [message.id] }));
		} else if (f.event === "notification" && Array.isArray(payload)) {
			if (sessionCtx?.isIdle() === false) return;
			const details = payload.map((n) => String(n.detail ?? "")).filter(Boolean);
			if (details.length === 0) return;
			try {
				pi.sendUserMessage("[murmur notice] " + details.join("; "), { deliverAs: "followUp", expandPromptTemplates: false });
			} catch {
				// dropped
			}
		}
	}

	function scheduleReconnect(): void {
		if (closed) return;
		if (reconnectTimer) clearTimeout(reconnectTimer);
		reconnectTimer = setTimeout(() => void connectBus(), RECONNECT_MS);
	}

	async function connectBus(): Promise<void> {
		if (closed) return;
		try {
			const c = await openClient(onEvent, () => {
				if (client === c) client = undefined;
				scheduleReconnect();
			});
			client = c;
			const r = (await c.request(sessionFrame("session-start"))) as { handle?: string } | null;
			if (r?.handle) handle = r.handle;
			if (busy) quiet(c.request(sessionFrame("prompt")));
			if (handle && !announcedHandle) {
				announcedHandle = true;
				sessionCtx?.ui.notify(`murmur: joined as ${handle}`, "info");
			}
		} catch {
			client?.close();
			client = undefined;
			scheduleReconnect();
		}
	}

	async function call(body: Json, timeoutMs?: number): Promise<ReturnType<typeof text>> {
		if (!client) return text("murmur broker is not connected (retrying every 5s).", true);
		try {
			const r = await client.request(body, timeoutMs);
			return text(JSON.stringify(r ?? null));
		} catch (e) {
			return text(String(e instanceof Error ? e.message : e), true);
		}
	}

	// one short cli connection for the /murmur command
	async function oneShot(run: (c: Client) => Promise<string>): Promise<string> {
		let c: Client | undefined;
		try {
			c = await openClient(() => {}, () => {});
			await c.request({ type: "register", clientKind: "cli", owner: true, ancestors: [process.pid] });
			return await run(c);
		} catch (e) {
			return `murmur: ${String(e instanceof Error ? e.message : e)}`;
		} finally {
			c?.close();
		}
	}

	// -------------------------------------------------------------------------

	pi.on("session_start", (_event, ctx) => {
		sessionCtx = ctx;
		sessionId = ctx.sessionManager.getSessionId();
		host = readHost();
		void connectBus();
	});

	pi.on("agent_start", () => {
		busy = true;
		if (client) quiet(client.request(sessionFrame("prompt")));
	});

	pi.on("agent_end", () => {
		busy = false;
		if (client) quiet(client.request(sessionFrame("stop")));
	});

	pi.on("session_shutdown", () => {
		closed = true;
		if (reconnectTimer) clearTimeout(reconnectTimer);
		const c = client;
		client = undefined;
		if (!c) return;
		c.request(sessionFrame("session-end"), 2000)
			.catch(() => {})
			.finally(() => c.close());
	});

	pi.registerCommand("murmur", {
		description: "Send to murmur peers or groups: /murmur <targets> <message>; bare shows peers and groups",
		async handler(args, ctx) {
			const trimmed = args.trim();
			const out = await oneShot(async (c) => {
				if (!trimmed) {
					const status = (await c.request({ type: "status" })) as Json;
					const groups = await c.request({ type: "groups", action: "list" });
					return fmtOverview(status, groups);
				}
				const m = /^(\S+)\s+([\s\S]+)$/.exec(trimmed);
				if (!m) return "usage: /murmur <handle|group[,handle|group...]> <message>";
				const to = m[1]!.split(",").filter(Boolean);
				const r = (await c.request({ type: "send", threadId: null, to, body: m[2]! })) as Json;
				return fmtDeliveries(r);
			});
			ctx.ui.notify(out, out.startsWith("murmur:") || out.startsWith("usage:") ? "error" : "info");
		},
	});

	const send = Type.Object({
		to: Type.Array(Type.String(), { description: "Peer handles or group names" }),
		body: Type.String({ minLength: 1 }),
		threadId: Type.Optional(Type.Number({ description: "Existing thread to continue" })),
		references: Type.Optional(Type.Array(Type.String())),
		mode: Type.Optional(Type.Union([Type.Literal("push"), Type.Literal("notify"), Type.Literal("silent")])),
	});
	pi.registerTool({
		name: "murmur_send",
		label: "Murmur Send",
		description: "Send a message to murmur peers or groups, optionally into an existing thread.",
		promptSnippet: "murmur_send: message other agent sessions over the murmur bus",
		parameters: send,
		async execute(_id, p) {
			return call({
				type: "send",
				threadId: p.threadId ?? null,
				to: p.to,
				body: p.body,
				...(p.references ? { references: p.references } : {}),
				...(p.mode ? { mode: p.mode } : {}),
			});
		},
	});

	pi.registerTool({
		name: "murmur_reply",
		label: "Murmur Reply",
		description:
			"Reply to a murmur message; the reply reaches everyone in the thread, so start the body with AGREE, DISAGREE or INFO.",
		promptSnippet: "murmur_reply: answer a [murmur m<id>] message",
		parameters: Type.Object({
			messageId: Type.Number({ description: "Id from the [murmur m<id>] tag" }),
			body: Type.String({ minLength: 1 }),
			verdict: Type.Optional(Type.Union([Type.Literal("agree"), Type.Literal("disagree"), Type.Literal("info")])),
			references: Type.Optional(Type.Array(Type.String())),
		}),
		async execute(_id, p) {
			return call({
				type: "reply",
				messageId: p.messageId,
				body: p.body,
				...(p.verdict ? { verdict: p.verdict } : {}),
				...(p.references ? { references: p.references } : {}),
			});
		},
	});

	pi.registerTool({
		name: "murmur_thread",
		label: "Murmur Thread",
		description: "Read a murmur thread, as a digest or with full bodies.",
		parameters: Type.Object({
			threadId: Type.Number(),
			sinceSeq: Type.Optional(Type.Number()),
			full: Type.Optional(Type.Boolean()),
		}),
		async execute(_id, p) {
			return call({ type: "thread", threadId: p.threadId, sinceSeq: p.sinceSeq ?? null, full: p.full ?? false });
		},
	});

	pi.registerTool({
		name: "murmur_wait",
		label: "Murmur Wait",
		description: "Wait until all or any recipients of a thread have replied (default 120000 ms, max 300000, 0 returns now).",
		parameters: Type.Object({
			threadId: Type.Number(),
			until: Type.Union([Type.Literal("all_replied"), Type.Literal("any_replied")]),
			timeoutMs: Type.Optional(Type.Number({ minimum: 0, maximum: 300_000 })),
		}),
		async execute(_id, p) {
			const timeoutMs = Math.min(p.timeoutMs ?? 120_000, 300_000);
			return call({ type: "wait", threadId: p.threadId, until: p.until, timeoutMs }, timeoutMs + 10_000);
		},
	});

	pi.registerTool({
		name: "murmur_status",
		label: "Murmur Status",
		description: "List murmur peers with presence and unread counts.",
		parameters: Type.Object({}),
		async execute() {
			return call({ type: "status" });
		},
	});

	pi.registerTool({
		name: "murmur_groups",
		label: "Murmur Groups",
		description: "Create, list, edit or delete murmur groups.",
		parameters: Type.Object({
			action: Type.Union([
				Type.Literal("create"),
				Type.Literal("list"),
				Type.Literal("add"),
				Type.Literal("remove"),
				Type.Literal("delete"),
			]),
			name: Type.Optional(Type.String()),
			members: Type.Optional(Type.Array(Type.String())),
		}),
		async execute(_id, p) {
			return call({
				type: "groups",
				action: p.action,
				...(p.name ? { name: p.name } : {}),
				...(p.members ? { members: p.members } : {}),
			});
		},
	});

	pi.registerTool({
		name: "murmur_name",
		label: "Murmur Name",
		description: "Rename this session's murmur handle.",
		parameters: Type.Object({ handle: Type.String({ minLength: 1 }) }),
		async execute(_id, p) {
			const res = await call({ type: "register", clientKind: "adapter", host, handle: p.handle });
			if (!res.isError) {
				try {
					const r = JSON.parse(res.content[0]!.text) as { handle?: string } | null;
					if (r?.handle) handle = r.handle;
				} catch {
					// keep the old handle
				}
			}
			return res;
		},
	});
}
