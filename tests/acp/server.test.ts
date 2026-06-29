import { afterEach, describe, expect, mock, spyOn, test } from "bun:test";
import { runAcp } from "../../src/acp/server";

describe("runAcp", () => {
	afterEach(() => {
		mock.restore();
	});

	test("should map Bun.stdin and Bun.stdout to ndJsonStream", async () => {
		let ndJsonStreamArgs: any[] = [];
		mock.module("@agentclientprotocol/sdk", () => {
			const original = require("@agentclientprotocol/sdk");
			return {
				...original,
				ndJsonStream: (...args: any[]) => {
					ndJsonStreamArgs = args;
					return "mocked_stream";
				},
				agent: () => {
					const agentBuilder = {
						onRequest: () => agentBuilder,
						onNotification: () => agentBuilder,
						connect: (_stream: any) => {
							return { closed: Promise.resolve() };
						},
					};
					return agentBuilder;
				},
			};
		});

		// Mock AgyAcpAgent so we don't do real background tasks
		mock.module("../../src/acp/agent", () => {
			return {
				AgyAcpAgent: class MockAgyAcpAgent {},
			};
		});

		// Spy on Bun.stdin/stdout methods
		let writerWritten = false;
		let writerClosed = false;
		const mockWriter = {
			write: () => {
				writerWritten = true;
			},
			flush: () => {},
			end: () => {
				writerClosed = true;
			},
		};

		const stdinSpy = spyOn(Bun.stdin, "stream").mockReturnValue(
			"mocked_stdin_stream" as any,
		);
		const stdoutSpy = spyOn(Bun.stdout, "writer").mockReturnValue(
			mockWriter as any,
		);

		runAcp();

		expect(stdinSpy).toHaveBeenCalled();
		expect(stdoutSpy).toHaveBeenCalled();

		expect(ndJsonStreamArgs.length).toBe(2);
		expect(ndJsonStreamArgs[1]).toBe("mocked_stdin_stream");

		// Test the custom writable stream behavior
		const customWritable = ndJsonStreamArgs[0] as WritableStream;
		expect(customWritable).toBeInstanceOf(WritableStream);

		// Write to it
		const writer = customWritable.getWriter();
		await writer.write(new Uint8Array([1, 2, 3]));
		expect(writerWritten).toBe(true);

		await writer.close();
		expect(writerClosed).toBe(true);
	});
});
