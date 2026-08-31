// Prompt-turn runtime: spawn agy, poll its DB while it runs, stream updates to
// the client, and finalize. Bridges the agy subprocess and the conversation
// streaming layer.

import { buildAgyArgs, extraArgsFromEnv, spawnAgy } from "../agy/process";
import { POLL_INTERVAL_MS } from "../constants";
import { conversationSnapshot } from "../conversation/scan";
import { StreamPoller } from "../conversation/streaming";
import type { Session } from "../types/session";
import type { AcpClient } from "./client";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export interface PromptOutcome {
	stopReason: "end_turn" | "cancelled";
	conversationId: string | null;
	lastStepIdx: number;
	hadUpdates: boolean;
	/** Set when agy failed to start, or exited non-zero with nothing streamed. */
	error?: string;
}

export interface AdapterConfig {
	binary: string;
	conversationsDir: string;
	workingDir: string;
	skipNarration: boolean;
}

export class Adapter {
	private readonly children = new Map<string, Bun.Subprocess>();
	private readonly cancelled = new Set<string>();

	constructor(private readonly config: AdapterConfig) {}

	/** Gracefully terminate and clean up an active child subprocess for a session. */
	private killChild(sessionId: string): void {
		const child = this.children.get(sessionId);
		if (child) {
			this.children.delete(sessionId);
			try {
				if (process.platform === "win32") {
					child.kill();
				} else {
					child.kill("SIGINT");
					setTimeout(() => {
						try {
							child.kill();
						} catch {}
					}, 500);
				}
			} catch {}
		}
	}

	/** Request cancellation of an in-flight prompt for a session. */
	cancel(sessionId: string): void {
		this.cancelled.add(sessionId);
		this.killChild(sessionId);
	}

	/** Run a prompt turn end-to-end: spawn agy, stream deltas, finalize. */
	async runPrompt(
		sessionId: string,
		session: Session,
		promptText: string,
		client: AcpClient,
	): Promise<PromptOutcome> {
		this.cancelled.delete(sessionId);

		// Clean up any lingering subprocess from a previous turn on this session.
		this.killChild(sessionId);

		// Use the session's cwd if set, otherwise fall back to the server's workingDir.
		const effectiveCwd = session.cwd || this.config.workingDir;

		// Snapshot existing conversations so we can bind the new DB agy creates.
		const snapshot =
			session.conversationId === null
				? conversationSnapshot(this.config.conversationsDir)
				: null;

		const args = buildAgyArgs({
			workingDir: effectiveCwd,
			additionalDirs: session.additionalDirs,
			conversationId: session.conversationId,
			modelId: session.modelId,
			mode: session.permissionMode,
			prompt: promptText,
			extraArgs: extraArgsFromEnv(),
		});

		let child: Bun.Subprocess;
		try {
			child = spawnAgy(this.config.binary, args, effectiveCwd);
		} catch (err) {
			return {
				stopReason: "end_turn",
				conversationId: session.conversationId,
				lastStepIdx: session.lastStepIdx,
				hadUpdates: false,
				error: `failed to run agy: ${(err as Error).message}`,
			};
		}
		this.children.set(sessionId, child);

		// Drain stderr concurrently (resolves when the process exits).
		const stderrPromise = child.stderr
			? new Response(child.stderr as ReadableStream).text()
			: Promise.resolve("");

		const poller = new StreamPoller({
			dir: this.config.conversationsDir,
			conversationId: session.conversationId,
			baseStepIdx: session.lastStepIdx,
			skipNarration: this.config.skipNarration,
			cwd: effectiveCwd,
			snapshot,
		});

		// Serialized poll loop: emit updates in order, never overlapping.
		const pollOnce = async (): Promise<boolean> => {
			const updates = poller.poll();
			for (const update of updates) {
				await client.update(sessionId, update);
			}
			return updates.length > 0;
		};

		let polling = true;
		let childExited = false;
		let exitCode: number | null = null;

		void child.exited.then((code) => {
			childExited = true;
			exitCode = code;
		});

		let quietPollCount = 0;
		while (polling) {
			if (this.cancelled.has(sessionId)) {
				break;
			}

			try {
				const hadNewUpdates = await pollOnce();
				if (hadNewUpdates) {
					quietPollCount = 0;
				} else {
					quietPollCount++;
				}
			} catch (err) {
				console.error(`[agy-acp] poll error: ${(err as Error).message}`);
			}

			if (childExited) {
				break;
			}

			// If turn completion has been recorded in transcript or DB and stream has settled
			// (at least 2 consecutive quiet polls ~400ms without new chunks), end the turn cleanly.
			if (poller.isTurnCompleted() && quietPollCount >= 2) {
				break;
			}

			await sleep(POLL_INTERVAL_MS);
		}

		polling = false;

		// A few trailing polls to catch rows flushed right around the finish boundary.
		for (let attempt = 0; attempt < 3; attempt++) {
			try {
				await pollOnce();
			} catch (err) {
				console.error(`[agy-acp] final poll error: ${(err as Error).message}`);
			}
			if (attempt < 2) await sleep(100);
		}
		poller.close();

		// Gracefully terminate lingering child (e.g. CLI waiting in reactive wakeup for async background tasks)
		this.killChild(sessionId);

		const stderr = (await stderrPromise).trim();
		if (stderr.length > 0) console.error(`[agy-acp] agy stderr: ${stderr}`);

		const wasCancelled = this.cancelled.delete(sessionId);

		const outcome: PromptOutcome = {
			stopReason: wasCancelled ? "cancelled" : "end_turn",
			conversationId: poller.conversationId,
			lastStepIdx: poller.lastStepIdx,
			hadUpdates: poller.hadUpdates,
		};

		if (!wasCancelled && exitCode !== null && exitCode !== 0) {
			console.error(`[agy-acp] WARN: agy exited with status ${exitCode}`);
			if (!poller.hadUpdates) {
				outcome.error =
					stderr.length > 0
						? `agy failed: ${stderr}`
						: `agy exited with status: ${exitCode}`;
			}
		}

		return outcome;
	}
}
