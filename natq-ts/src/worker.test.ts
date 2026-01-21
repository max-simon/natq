import { jest, describe, it, expect } from '@jest/globals';
import { JSONCodec } from "nats";
import {
  NatqWorker,
  TaskType,
  TaskHandler,
} from "./worker.js";
import { Logger } from "./logger.js";

const jc = JSONCodec();

// Silent mock logger
function createMockLogger(): Logger {
  return {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  };
}

describe("NatqWorker", () => {
  describe("registerTask", () => {
    it("should register a sync task with default subject", () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);
      const handler: TaskHandler = async () => ({
        id: "test",
        taskId: "my-task",
        status: 200,
      });

      worker.registerTask("my-task", TaskType.SYNC, handler);

      expect(worker.registry["my-task"]).toBeDefined();
      expect(worker.registry["my-task"].definition.id).toBe("my-task");
      expect(worker.registry["my-task"].definition.type).toBe(TaskType.SYNC);
      expect(worker.registry["my-task"].definition.subject).toBe("natq.req.my-task");
    });

    it("should register an async task with default subject", () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);
      const handler: TaskHandler = async () => ({
        id: "test",
        taskId: "my-job",
        status: 200,
      });

      worker.registerTask("my-job", TaskType.ASYNC, handler);

      expect(worker.registry["my-job"].definition.subject).toBe("natq.job.my-job");
    });

    it("should register a task with custom subject", () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);
      const handler: TaskHandler = async () => ({
        id: "test",
        taskId: "custom",
        status: 200,
      });

      worker.registerTask("custom", TaskType.SYNC, handler, {
        subject: "custom.subject",
      });

      expect(worker.registry["custom"].definition.subject).toBe("custom.subject");
    });

    it("should register a task with schemas", () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);
      const handler: TaskHandler = async () => ({
        id: "test",
        taskId: "with-schema",
        status: 200,
      });

      worker.registerTask("with-schema", TaskType.SYNC, handler, {
        inputSchema: '{"type": "object"}',
        outputSchema: '{"type": "string"}',
      });

      expect(worker.registry["with-schema"].definition.inputSchema).toBe(
        '{"type": "object"}'
      );
      expect(worker.registry["with-schema"].definition.outputSchema).toBe(
        '{"type": "string"}'
      );
    });
  });

  describe("executeTaskCallback", () => {
    it("should execute handler successfully with valid input", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 200,
        data: { received: input },
      });

      worker.registerTask("test-task", TaskType.SYNC, handler);
      const task = worker.registry["test-task"];

      const inputData = { foo: "bar", runId: "custom-run-id" };
      const msgData = jc.encode(inputData);

      const result = await (worker as any).executeTaskCallback(msgData, task);

      expect(result.input).toEqual(inputData);
      expect(result.output.status).toBe(200);
      expect(result.output.id).toBe("custom-run-id");
      expect(result.output.data).toEqual({ received: inputData });
    });

    it("should generate runId if not provided in input", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 200,
      });

      worker.registerTask("test-task", TaskType.SYNC, handler);
      const task = worker.registry["test-task"];

      const inputData = { foo: "bar" };
      const msgData = jc.encode(inputData);

      const result = await (worker as any).executeTaskCallback(msgData, task);

      expect(result.output.id).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
      );
    });

    it("should return 406 status for invalid JSON input", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);

      const handler: TaskHandler = async () => ({
        id: "test",
        taskId: "test-task",
        status: 200,
      });

      worker.registerTask("test-task", TaskType.SYNC, handler);
      const task = worker.registry["test-task"];

      const invalidData = new TextEncoder().encode("not valid json {{{");

      const result = await (worker as any).executeTaskCallback(invalidData, task);

      expect(result.output.status).toBe(406);
      expect(result.output.error).toBe("Invalid JSON input");
      expect(result.output.id).toBe("<unknown>");
    });

    it("should return 500 status when handler throws an error", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);

      const handler: TaskHandler = async () => {
        throw new Error("Handler exploded");
      };

      worker.registerTask("test-task", TaskType.SYNC, handler);
      const task = worker.registry["test-task"];

      const inputData = { runId: "error-run" };
      const msgData = jc.encode(inputData);

      const result = await (worker as any).executeTaskCallback(msgData, task);

      expect(result.output.status).toBe(500);
      expect(result.output.error).toBe("Unhandled exception: Handler exploded");
      expect(result.output.id).toBe("error-run");
    });

    it("should handle non-Error throws", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);

      const handler: TaskHandler = async () => {
        throw "string error";
      };

      worker.registerTask("test-task", TaskType.SYNC, handler);
      const task = worker.registry["test-task"];

      const msgData = jc.encode({});

      const result = await (worker as any).executeTaskCallback(msgData, task);

      expect(result.output.status).toBe(500);
      expect(result.output.error).toBe("Unhandled exception: string error");
    });
  });

  describe("handleSyncMessage", () => {
    it("should respond with success data and headers", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 200,
        data: { result: "success" },
      });

      worker.registerTask("sync-task", TaskType.SYNC, handler);
      const task = worker.registry["sync-task"];

      const respondMock = jest.fn();
      const mockMsg = {
        data: jc.encode({ runId: "sync-run-1" }),
        respond: respondMock,
      };

      await (worker as any).handleSyncMessage(task, mockMsg);

      expect(respondMock).toHaveBeenCalledTimes(1);
      const [responseData, options] = respondMock.mock.calls[0] as any;
      expect(jc.decode(responseData)).toEqual({ result: "success" });
      expect(options.headers.get("status")).toBe("200");
      expect(options.headers.get("error")).toBeFalsy();
    });

    it("should respond with error headers on handler failure", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);

      const handler: TaskHandler = async () => {
        throw new Error("Sync handler failed");
      };

      worker.registerTask("sync-task", TaskType.SYNC, handler);
      const task = worker.registry["sync-task"];

      const respondMock = jest.fn();
      const mockMsg = {
        data: jc.encode({}),
        respond: respondMock,
      };

      await (worker as any).handleSyncMessage(task, mockMsg);

      expect(respondMock).toHaveBeenCalledTimes(1);
      const [responseData, options] = respondMock.mock.calls[0] as any;
      expect(responseData).toBeUndefined();
      expect(options.headers.get("status")).toBe("500");
      expect(options.headers.get("error")).toContain("Unhandled exception");
    });

    it("should respond with 500 when executeTaskCallback throws unexpectedly", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);

      worker.registerTask("sync-task", TaskType.SYNC, async () => ({
        id: "test",
        taskId: "sync-task",
        status: 200,
      }));
      const task = worker.registry["sync-task"];

      // Mock executeTaskCallback to throw
      (worker as any).executeTaskCallback = jest.fn().mockRejectedValue(
        new Error("Unexpected error") as never
      );

      const respondMock = jest.fn();
      const mockMsg = {
        data: jc.encode({}),
        respond: respondMock,
      };

      await (worker as any).handleSyncMessage(task, mockMsg);

      expect(respondMock).toHaveBeenCalledTimes(1);
      const [, options] = respondMock.mock.calls[0] as any;
      expect(options.headers.get("status")).toBe("500");
      expect(options.headers.get("error")).toBe("Internal server error");
    });

    it("should not include data in response when output has no data", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({}, mockLogger);

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 204, // No content
      });

      worker.registerTask("sync-task", TaskType.SYNC, handler);
      const task = worker.registry["sync-task"];

      const respondMock = jest.fn();
      const mockMsg = {
        data: jc.encode({}),
        respond: respondMock,
      };

      await (worker as any).handleSyncMessage(task, mockMsg);

      const [responseData, options] = respondMock.mock.calls[0] as any;
      expect(responseData).toBeUndefined();
      expect(options.headers.get("status")).toBe("204");
    });
  });

  describe("handleAsyncMessage", () => {
    // Helper to create a mock resultsKv
    function createMockResultsKv() {
      return { put: jest.fn<() => Promise<number>>().mockResolvedValue(1) };
    }

    it("should ack message on success (status < 300)", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 100000 }, mockLogger);
      (worker as any).resultsKv = createMockResultsKv();

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 200,
        data: { done: true },
      });

      worker.registerTask("async-task", TaskType.ASYNC, handler);
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: jc.encode({ runId: "async-run-1" }),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 1 },
      };

      await (worker as any).handleAsyncMessage(task, mockMsg);

      expect(mockMsg.ack).toHaveBeenCalledTimes(1);
      expect(mockMsg.nak).not.toHaveBeenCalled();
      expect(mockMsg.term).not.toHaveBeenCalled();
    });

    it("should term message on client error (300-499)", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 100000 }, mockLogger);
      (worker as any).resultsKv = createMockResultsKv();

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 400,
        error: "Bad request",
      });

      worker.registerTask("async-task", TaskType.ASYNC, handler);
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: jc.encode({}),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 1 },
      };

      await (worker as any).handleAsyncMessage(task, mockMsg);

      expect(mockMsg.term).toHaveBeenCalledWith("Bad request");
      expect(mockMsg.ack).not.toHaveBeenCalled();
      expect(mockMsg.nak).not.toHaveBeenCalled();
    });

    it("should nak message with backoff on server error (500+)", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 100000 }, mockLogger);
      (worker as any).resultsKv = createMockResultsKv();

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 500,
        error: "Server error",
      });

      worker.registerTask("async-task", TaskType.ASYNC, handler);
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: jc.encode({}),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 3 },
      };

      await (worker as any).handleAsyncMessage(task, mockMsg);

      expect(mockMsg.nak).toHaveBeenCalledTimes(1);
      // Exponential backoff: 2^3 * 1000 = 8000ms
      expect(mockMsg.nak).toHaveBeenCalledWith(8000);
      expect(mockMsg.ack).not.toHaveBeenCalled();
      expect(mockMsg.term).not.toHaveBeenCalled();
    });

    it("should cap backoff delay at 60 seconds", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 100000 }, mockLogger);
      (worker as any).resultsKv = createMockResultsKv();

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 500,
        error: "Server error",
      });

      worker.registerTask("async-task", TaskType.ASYNC, handler);
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: jc.encode({}),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 10 }, // 2^10 * 1000 = 1024000ms, should cap at 60000
      };

      await (worker as any).handleAsyncMessage(task, mockMsg);

      expect(mockMsg.nak).toHaveBeenCalledWith(60000);
    });

    it("should handle KV write failures gracefully and still ack on success", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 100000 }, mockLogger);
      const mockResultsKv = {
        put: jest.fn<() => Promise<number>>().mockRejectedValue(new Error("KV write failed")),
      };
      (worker as any).resultsKv = mockResultsKv;

      worker.registerTask("async-task", TaskType.ASYNC, async (input, context) => ({
        id: context.runId,
        taskId: "async-task",
        status: 200,
      }));
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: jc.encode({}),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 1 },
      };

      await (worker as any).handleAsyncMessage(task, mockMsg);

      // KV failures are logged but don't prevent message acknowledgment
      expect(mockMsg.ack).toHaveBeenCalled();
      expect(mockMsg.nak).not.toHaveBeenCalled();
    });

    it("should send heartbeat during long processing", async () => {
      jest.useFakeTimers();

      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 1000 }, mockLogger);
      (worker as any).resultsKv = createMockResultsKv();

      let resolveHandler: () => void;
      const handlerPromise = new Promise<void>((resolve) => {
        resolveHandler = resolve;
      });

      const handler: TaskHandler = async (input, context) => {
        await handlerPromise;
        return {
          id: context.runId,
          taskId: context.task.id,
          status: 200,
        };
      };

      worker.registerTask("async-task", TaskType.ASYNC, handler);
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: jc.encode({}),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 1 },
      };

      const promise = (worker as any).handleAsyncMessage(task, mockMsg);

      // Advance time to trigger heartbeats
      jest.advanceTimersByTime(2500);
      expect(mockMsg.working).toHaveBeenCalledTimes(2);

      // Complete the handler
      resolveHandler!();
      await promise;

      expect(mockMsg.ack).toHaveBeenCalled();

      jest.useRealTimers();
    });

    it("should write PROCESSING status to KV before handler execution", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 100000 }, mockLogger);

      const kvPutCalls: { key: string; data: any }[] = [];
      const mockPut = jest.fn<(key: string, data: Uint8Array) => Promise<number>>();
      mockPut.mockImplementation(async (key: string, data: Uint8Array) => {
        kvPutCalls.push({ key, data: jc.decode(data) });
        return 1;
      });
      const mockResultsKv = { put: mockPut };
      (worker as any).resultsKv = mockResultsKv;

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 200,
        data: { result: "success" },
      });

      worker.registerTask("async-task", TaskType.ASYNC, handler);
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: jc.encode({ runId: "kv-run-1" }),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 1 },
      };

      await (worker as any).handleAsyncMessage(task, mockMsg);

      // Should have two KV puts: PROCESSING (100) then final status (200)
      expect(kvPutCalls.length).toBe(2);
      expect(kvPutCalls[0].key).toBe("async-task.kv-run-1");
      expect(kvPutCalls[0].data.status).toBe(100);
      expect(kvPutCalls[1].key).toBe("async-task.kv-run-1");
      expect(kvPutCalls[1].data.status).toBe(200);
      expect(kvPutCalls[1].data.data).toEqual({ result: "success" });
    });

    it("should update KV with error on client error (4xx)", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 100000 }, mockLogger);

      const kvPutCalls: { key: string; data: any }[] = [];
      const mockPut = jest.fn<(key: string, data: Uint8Array) => Promise<number>>();
      mockPut.mockImplementation(async (key: string, data: Uint8Array) => {
        kvPutCalls.push({ key, data: jc.decode(data) });
        return 1;
      });
      const mockResultsKv = { put: mockPut };
      (worker as any).resultsKv = mockResultsKv;

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 400,
        error: "Bad request",
      });

      worker.registerTask("async-task", TaskType.ASYNC, handler);
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: jc.encode({ runId: "error-run" }),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 1 },
      };

      await (worker as any).handleAsyncMessage(task, mockMsg);

      // Should have two KV puts: PROCESSING (100) then error status (400)
      expect(kvPutCalls.length).toBe(2);
      expect(kvPutCalls[0].data.status).toBe(100);
      expect(kvPutCalls[1].data.status).toBe(400);
      expect(kvPutCalls[1].data.error).toBe("Bad request");
      expect(mockMsg.term).toHaveBeenCalled();
    });

    it("should NOT update KV on server error (5xx)", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 100000 }, mockLogger);

      const kvPutCalls: { key: string; data: any }[] = [];
      const mockPut = jest.fn<(key: string, data: Uint8Array) => Promise<number>>();
      mockPut.mockImplementation(async (key: string, data: Uint8Array) => {
        kvPutCalls.push({ key, data: jc.decode(data) });
        return 1;
      });
      const mockResultsKv = { put: mockPut };
      (worker as any).resultsKv = mockResultsKv;

      const handler: TaskHandler = async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 500,
        error: "Server error",
      });

      worker.registerTask("async-task", TaskType.ASYNC, handler);
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: jc.encode({ runId: "server-error-run" }),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 1 },
      };

      await (worker as any).handleAsyncMessage(task, mockMsg);

      // Should only have ONE KV put: PROCESSING (100), NO update after server error
      expect(kvPutCalls.length).toBe(1);
      expect(kvPutCalls[0].data.status).toBe(100);
      expect(mockMsg.nak).toHaveBeenCalled();
    });

    it("should skip KV creation on invalid JSON input", async () => {
      const mockLogger = createMockLogger();
      const worker = new NatqWorker({ heartbeatIntervalMs: 100000 }, mockLogger);

      const mockResultsKv = {
        put: jest.fn(),
      };
      (worker as any).resultsKv = mockResultsKv;

      worker.registerTask("async-task", TaskType.ASYNC, async (input, context) => ({
        id: context.runId,
        taskId: context.task.id,
        status: 200,
      }));
      const task = worker.registry["async-task"];

      const mockMsg = {
        data: new TextEncoder().encode("not valid json {{{"),
        ack: jest.fn(),
        nak: jest.fn(),
        term: jest.fn(),
        working: jest.fn(),
        info: { deliveryCount: 1 },
      };

      await (worker as any).handleAsyncMessage(task, mockMsg);

      // Should NOT call KV put for invalid JSON
      expect(mockResultsKv.put).not.toHaveBeenCalled();
      expect(mockMsg.term).toHaveBeenCalledWith("Invalid JSON input");
    });
  });
});
