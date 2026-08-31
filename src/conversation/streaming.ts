// Live streaming poller for an in-flight prompt turn. Holds one open DB handle
// for the turn and drives the shared Translator in "stream" mode, emitting only
// newly-appended agent text and not-yet-sent tool steps on each poll.

import * as fs from "node:fs";
import * as path from "node:path";
import type { SessionUpdate } from "@agentclientprotocol/sdk";
import type { StepRow } from "../types";
import { ConversationDb } from "./database";
import { newConversationId } from "./scan";
import { Translator } from "./translator";

export interface StreamOptions {
	dir: string;
	/** Bound conversation id, or null to bind the DB agy creates for a fresh prompt. */
	conversationId: string | null;
	/** Highest idx already delivered to the client before this turn. */
	baseStepIdx: number;
	skipNarration: boolean;
	cwd?: string;
	/** Snapshot of conversation ids before the prompt, for binding a new DB. */
	snapshot: Set<string> | null;
}

export class StreamPoller {
	private readonly translator: Translator;
	private db: ConversationDb | null = null;
	private boundId: string | null;
	private lastRows: StepRow[] = [];

	constructor(private readonly opts: StreamOptions) {
		this.boundId = opts.conversationId;
		this.translator = new Translator({
			mode: "stream",
			skipNarration: opts.skipNarration,
			cwd: opts.cwd,
		});
	}

	get conversationId(): string | null {
		return this.boundId;
	}

	get lastStepIdx(): number {
		return Math.max(this.translator.lastStepIdx, this.opts.baseStepIdx);
	}

	get hadUpdates(): boolean {
		return this.translator.hadUpdates;
	}

	/** Read steps appended since the turn began and translate the new ones. */
	poll(): SessionUpdate[] {
		if (this.boundId === null && this.opts.snapshot !== null) {
			this.boundId = newConversationId(this.opts.dir, this.opts.snapshot);
		}
		if (this.boundId === null) return [];

		if (this.db === null) {
			this.db = ConversationDb.open(this.opts.dir, this.boundId);
			if (this.db === null) return [];
		}

		this.lastRows = this.db.readAfter(this.opts.baseStepIdx);
		return this.translator.translate(this.lastRows);
	}

	/** Path to the transcript.jsonl log for the bound conversation. */
	private transcriptPath(): string | null {
		if (!this.boundId) return null;
		const appDataDir = path.dirname(this.opts.dir);
		return path.join(
			appDataDir,
			"brain",
			this.boundId,
			".system_generated",
			"logs",
			"transcript.jsonl",
		);
	}

	/** Check transcript.jsonl tail for turn completion (PLANNER_RESPONSE done with no pending tool calls). */
	private isTurnCompletedFromTranscript(): boolean {
		const tPath = this.transcriptPath();
		if (!tPath) return false;
		try {
			if (!fs.existsSync(tPath)) return false;
			const stat = fs.statSync(tPath);
			if (stat.size === 0) return false;
			const readSize = Math.min(stat.size, 8192);
			const buf = Buffer.alloc(readSize);
			const fd = fs.openSync(tPath, "r");
			fs.readSync(fd, buf, 0, readSize, stat.size - readSize);
			fs.closeSync(fd);
			const text = buf.toString("utf-8");
			const lines = text.trim().split("\n").filter(Boolean);
			for (let i = lines.length - 1; i >= 0; i--) {
				const line = lines[i];
				if (!line) continue;
				try {
					const obj = JSON.parse(line);
					if (obj && typeof obj.step_index === "number") {
						if (obj.step_index <= this.opts.baseStepIdx) return false;
						const isDone =
							(obj.type === "PLANNER_RESPONSE" &&
								obj.status === "DONE" &&
								(!obj.tool_calls || obj.tool_calls.length === 0)) ||
							obj.type === "CHECKPOINT";
						return isDone;
					}
				} catch {}
			}
			return false;
		} catch {
			return false;
		}
	}

	/** Check SQLite DB steps table for turn completion as fallback. */
	private isTurnCompletedFromDb(): boolean {
		if (!this.lastRows || this.lastRows.length === 0) return false;
		const last = this.lastRows[this.lastRows.length - 1];
		if (!last || last.idx <= this.opts.baseStepIdx) return false;

		// Agent text response completed with non-empty content
		if (last.stepType === 15 && last.status === 3) {
			const text = last.stepPayload.agentText?.text ?? "";
			if (text.length > 0) {
				// Ensure no preceding tool step in this batch is still executing (status 1)
				const hasExecutingTool = this.lastRows.some((r) => {
					const isTool = [5, 7, 8, 9, 17, 21, 31, 33, 127, 132, 138].includes(
						r.stepType,
					);
					return isTool && r.status === 1;
				});
				return !hasExecutingTool;
			}
		}
		// Conversation title update written at the end of a turn
		if (last.stepType === 23 && last.status === 3) {
			return true;
		}
		return false;
	}

	/**
	 * Returns true if the turn has reached completion in either the transcript or SQLite DB,
	 * indicating the agent has finished outputting its response and is waiting for user input
	 * or reactive task wakeup.
	 */
	isTurnCompleted(): boolean {
		if (this.boundId === null) return false;
		if (this.isTurnCompletedFromTranscript()) return true;
		if (this.isTurnCompletedFromDb()) return true;
		return false;
	}

	close(): void {
		this.db?.close();
		this.db = null;
	}
}
