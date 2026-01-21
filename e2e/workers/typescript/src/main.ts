import { NatqWorker, TaskType, TaskHandler, LogLevel } from "natq";

// Track retry attempts per runId for e2e-retry task
const retryAttempts: Map<string, number> = new Map();

// e2e-add: Add two numbers
const addHandler: TaskHandler = async (input, context) => {
  const { a, b } = input as { a: number; b: number };

  return {
    id: context.runId,
    taskId: context.task.id,
    status: 200,
    data: { sum: a + b },
  };
};

// e2e-echo: Echo back the input
const echoHandler: TaskHandler = async (input, context) => {
  // Remove runId from output to match expected format
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { runId, ...data } = input as { runId?: string; message: string; nested: { foo: string } };

  return {
    id: context.runId,
    taskId: context.task.id,
    status: 200,
    data: data as { message: string; nested: { foo: string } },
  };
};

// e2e-client-error: Return a 400 error
const clientErrorHandler: TaskHandler = async (input, context) => {
  return {
    id: context.runId,
    taskId: context.task.id,
    status: 400,
    error: "Client requested failure",
  };
};

// e2e-delay: Wait for specified delay then return
const delayHandler: TaskHandler = async (input, context) => {
  const delayMs = (input as { delayMs: number }).delayMs || 1000;

  await new Promise((resolve) => setTimeout(resolve, delayMs));

  return {
    id: context.runId,
    taskId: context.task.id,
    status: 200,
    data: { delayed: true },
  };
};

// e2e-retry: Fail N times then succeed
const retryHandler: TaskHandler = async (input, context) => {
  const failCount = (input as { failCount: number }).failCount || 1;
  const runId = context.runId;

  // Get current attempt count
  const attempts = (retryAttempts.get(runId) || 0) + 1;
  retryAttempts.set(runId, attempts);

  console.log(`[e2e-retry] runId=${runId} attempt=${attempts} failCount=${failCount}`);

  if (attempts <= failCount) {
    // Return server error to trigger retry
    return {
      id: context.runId,
      taskId: context.task.id,
      status: 500,
      error: `Intentional failure ${attempts}/${failCount}`,
    };
  }

  // Success after enough retries
  // Clean up tracking
  retryAttempts.delete(runId);

  return {
    id: context.runId,
    taskId: context.task.id,
    status: 200,
    data: { attempts },
  };
};

// e2e-async-client-error: Return 400 error (should not retry)
const asyncClientErrorHandler: TaskHandler = async (input, context) => {
  return {
    id: context.runId,
    taskId: context.task.id,
    status: 400,
    error: "Async client error",
  };
};

async function main() {
  console.log("Starting natq E2E worker (TypeScript)...");

  const worker = new NatqWorker({ logLevel: LogLevel.INFO });

  // Register sync tasks
  worker.registerTask("e2e-add", TaskType.SYNC, addHandler);
  worker.registerTask("e2e-echo", TaskType.SYNC, echoHandler);
  worker.registerTask("e2e-client-error", TaskType.SYNC, clientErrorHandler);

  // Register async tasks
  worker.registerTask("e2e-delay", TaskType.ASYNC, delayHandler);
  worker.registerTask("e2e-retry", TaskType.ASYNC, retryHandler);
  worker.registerTask("e2e-async-client-error", TaskType.ASYNC, asyncClientErrorHandler);

  await worker.start({ servers: "localhost:4222" });

  console.log("E2E worker started. Press Ctrl+C to stop.");
}

main().catch((err) => {
  console.error("Failed to start worker:", err);
  process.exit(1);
});
