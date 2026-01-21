import {
  connect,
  headers,
  JSONCodec,
  NatsConnection,
  JetStreamClient,
  RetentionPolicy,
  DiscardPolicy,
  KV,
  Subscription,
  ConsumerMessages,
  AckPolicy,
  DeliverPolicy,
  type ConnectionOptions,
  type Msg,
  type JsMsg,
  JetStreamManager
} from "nats";
import { v7 as uuidv7 } from "uuid";
import { Logger, LogLevel, DefaultLogger } from "./logger.js";


// ============================================================================
// Configuration
// ============================================================================

/**
 * Configuration options for the NatqWorker.
 *
 * All options have sensible defaults. Override only what you need:
 *
 * @example
 * ```typescript
 * const worker = new NatqWorker({
 *   logLevel: LogLevel.DEBUG,
 *   heartbeatIntervalMs: 5000,
 * });
 * ```
 */
export interface NatqWorkerOptions {
  /** NATS KV bucket for storing task definitions. Default: `"natq_tasks"` */
  kvBucket: string;

  /** NATS KV bucket for storing async task results. Default: `"natq_results"` */
  resultsBucket: string;

  /** JetStream stream for async job messages. Default: `"natq_jobs"` */
  streamName: string;

  /** Minimum log level for the default logger. Default: `LogLevel.INFO` */
  logLevel: LogLevel;

  /** Subject prefix for sync (request/reply) tasks. Default: `"natq.req."` */
  syncSubjectPrefix: string;

  /** Subject prefix for async (job queue) tasks. Default: `"natq.job."` */
  asyncSubjectPrefix: string;

  /**
   * Interval (ms) for sending heartbeats during async task processing.
   * Heartbeats prevent message timeout while long-running tasks execute.
   * Default: `10000` (10 seconds)
   */
  heartbeatIntervalMs?: number;
}

const DEFAULT_WORKER_OPTIONS: NatqWorkerOptions = {
  kvBucket: "natq_tasks",
  resultsBucket: "natq_results",
  streamName: "natq_jobs",
  logLevel: LogLevel.INFO,
  syncSubjectPrefix: "natq.req.",
  asyncSubjectPrefix: "natq.job.",
  heartbeatIntervalMs: 10000,
};


// ============================================================================
// Task Types & Definitions
// ============================================================================

/**
 * Execution model for a task.
 *
 * - `SYNC`: Request/reply pattern. Caller blocks waiting for response.
 * - `ASYNC`: Job queue pattern. Message persisted in JetStream, processed when a worker is available.
 */
export enum TaskType {
  SYNC = "sync",
  ASYNC = "async",
}

/**
 * Metadata describing a registered task. Stored in NATS KV for discovery.
 *
 * Producers can read task definitions from KV to discover available tasks
 * and their expected input/output schemas.
 */
export interface TaskDefinition {
  /** Unique identifier for this task (e.g., "send-email", "process-order") */
  id: string;

  /** How this task is executed: sync (request/reply) or async (job queue) */
  type: TaskType;

  /** NATS subject where this task receives messages */
  subject: string;

  /** Optional JSON Schema describing expected input structure */
  inputSchema?: string;

  /** Optional JSON Schema describing output structure */
  outputSchema?: string;
}

/**
 * Execution context provided to task handlers.
 *
 * Use this to access run metadata and correlate logs across distributed systems.
 */
export interface TaskContext {
  /** Unique identifier for this execution. Use for logging and tracing. */
  runId: string;

  /** The task definition being executed */
  task: TaskDefinition;

  /** ID of the worker instance processing this task */
  workerId: string;
}


// ============================================================================
// Input/Output Types
// ============================================================================

/**
 * Types that can be serialized to JSON for task communication.
 */
export type JsonSerializable =
  | string
  | number
  | boolean
  | null
  | undefined
  | { [key: string]: JsonSerializable }
  | JsonSerializable[];

/**
 * Input payload received by task handlers.
 *
 * Reserved fields:
 * - `runId`: Custom run ID for tracking. If omitted, a UUID is generated.
 * - `dropResultOnSuccess`: For async tasks only. If true, the result is deleted
 *   from KV immediately after acknowledgment. Useful for fire-and-forget tasks
 *   where results don't need to be persisted.
 */
export type TaskRunInput = JsonSerializable & {
  runId?: string;
  dropResultOnSuccess?: boolean;
};

/**
 * Output structure returned by task handlers.
 *
 * The `status` field determines how async messages are acknowledged:
 * - `< 300`: Success. Message is ACKed and removed from queue.
 * - `300-499`: Client error. Message is TERMinated (no retry).
 * - `>= 500`: Server error. Message is NAKed and retried with backoff.
 *
 * @example
 * ```typescript
 * // Success response
 * return { id: context.runId, taskId: "my-task", status: 200, data: { result: "ok" } };
 *
 * // Error response
 * return { id: context.runId, taskId: "my-task", status: 400, error: "Invalid input" };
 * ```
 */
export interface TaskRunOutput {
  /** Run ID matching the execution context */
  id: string;

  /** Task ID that produced this output */
  taskId: string;

  /** HTTP-style status code indicating success (2xx) or failure (4xx/5xx) */
  status: number;

  /** Result data on success */
  data?: JsonSerializable;

  /** Error message on failure */
  error?: string;
}

/**
 * Common HTTP-style status codes for task responses.
 */
export enum StatusCode {
  PROCESSING = 100,
  OK = 200,
  BAD_REQUEST = 400,
  INTERNAL_ERROR = 500
}


// ============================================================================
// Task Handler
// ============================================================================

/**
 * Function signature for task handlers.
 *
 * Handlers receive the input payload and execution context, and must return
 * a TaskRunOutput. Unhandled exceptions are caught and converted to 500 errors.
 *
 * @example
 * ```typescript
 * const handler: TaskHandler = async (input, context) => {
 *   const data = input as { name: string };
 *   return {
 *     id: context.runId,
 *     taskId: context.task.id,
 *     status: 200,
 *     data: { greeting: `Hello, ${data.name}!` },
 *   };
 * };
 * ```
 */
export type TaskHandler = (
  input: TaskRunInput,
  context: TaskContext
) => Promise<TaskRunOutput>;

/**
 * Internal structure holding a task's definition and handler function.
 */
export interface RegisteredTask {
  definition: TaskDefinition;
  handler: TaskHandler;
}


// ============================================================================
// NatqWorker
// ============================================================================

/**
 * Distributed task queue worker that processes tasks via NATS.
 *
 * ## Lifecycle
 *
 * 1. Create worker: `new NatqWorker(options)`
 * 2. Register tasks: `worker.registerTask(id, type, handler)`
 * 3. Start processing: `await worker.start(natsOptions)`
 * 4. Graceful shutdown: `await worker.stop()`
 *
 * ## Task Types
 *
 * - **Sync tasks** use NATS request/reply. The producer waits for an immediate response.
 * - **Async tasks** use JetStream. Messages are persisted and processed with at-least-once delivery.
 *
 * ## Error Handling
 *
 * - Handler exceptions are caught and returned as 500 errors
 * - Async tasks with status >= 500 are retried with exponential backoff
 * - Async tasks with status 300-499 are terminated (no retry)
 *
 * @example
 * ```typescript
 * const worker = new NatqWorker({ logLevel: LogLevel.DEBUG });
 *
 * worker.registerTask("echo", TaskType.SYNC, async (input, ctx) => ({
 *   id: ctx.runId,
 *   taskId: ctx.task.id,
 *   status: 200,
 *   data: input,
 * }));
 *
 * await worker.start({ servers: "localhost:4222" });
 * ```
 */
export class NatqWorker {
  private options: NatqWorkerOptions;
  private workerId: string;
  private logger: Logger;

  /** Registry of all registered tasks, keyed by task ID. */
  readonly registry: { [taskId: string]: RegisteredTask };

  // NATS connections and clients
  private connection: NatsConnection | null = null;
  private js: JetStreamClient | null = null;
  private jsm: JetStreamManager | null = null;
  private kv: KV | null = null;
  private resultsKv: KV | null = null;
  private readonly jc = JSONCodec();

  // Active subscriptions and consumers for cleanup
  private syncSubscriptions: Subscription[] = [];
  private asyncConsumers: ConsumerMessages[] = [];
  private asyncPromises: Promise<void>[] = [];
  private isRunning = false;

  /**
   * Create a new worker instance.
   *
   * The worker does not connect to NATS until `start()` is called.
   *
   * @param options - Configuration overrides (merged with defaults)
   * @param logger - Custom logger implementation (optional)
   */
  constructor(options: Partial<NatqWorkerOptions> = {}, logger?: Logger) {
    this.options = Object.assign({}, DEFAULT_WORKER_OPTIONS, options);
    this.workerId = uuidv7();
    this.logger = logger ?? new DefaultLogger(this.options.logLevel);
    this.registry = {};
  }

  /**
   * Register a task handler.
   *
   * Must be called before `start()`. Each task ID must be unique.
   *
   * @param id - Unique task identifier
   * @param type - `TaskType.SYNC` for request/reply, `TaskType.ASYNC` for job queue
   * @param handler - Async function that processes the task
   * @param options - Optional: custom subject, input/output schemas
   *
   * @throws Error if called after worker has started
   *
   * @example
   * ```typescript
   * worker.registerTask("process-order", TaskType.ASYNC, async (input, ctx) => {
   *   const order = input as { orderId: string };
   *   await processOrder(order.orderId);
   *   return { id: ctx.runId, taskId: ctx.task.id, status: 200 };
   * });
   * ```
   */
  registerTask(
    id: string,
    type: TaskType,
    handler: TaskHandler,
    options?: Partial<TaskDefinition>
  ): void {
    if (this.isRunning) {
      throw new Error("Cannot register tasks after worker has started");
    }

    const defaultSubject =
      type === "sync"
        ? `${this.options.syncSubjectPrefix}${id}`
        : `${this.options.asyncSubjectPrefix}${id}`;

    const definition: TaskDefinition = {
      id,
      type,
      subject: options?.subject || defaultSubject,
      inputSchema: options?.inputSchema,
      outputSchema: options?.outputSchema,
    };

    this.registry[id] = { definition, handler };

    this.logger.info(`Registered task: ${id}`, { type, subject: definition.subject, workerId: this.workerId });
  }

  /**
   * Connect to NATS and start processing tasks.
   *
   * Startup sequence:
   * 1. Connect to NATS server
   * 2. Create KV bucket and JetStream stream if they don't exist
   * 3. Publish task definitions to KV for discovery
   * 4. Subscribe to sync task subjects
   * 5. Create JetStream consumers for async tasks
   * 6. Register SIGINT/SIGTERM handlers for graceful shutdown
   *
   * @param options - NATS connection options (servers, credentials, etc.)
   * @throws Error if already running or connection fails
   */
  async start(options: ConnectionOptions): Promise<void> {
    if (this.isRunning) {
      throw new Error("Worker is already running");
    }

    this.logger.info("Starting worker...");

    try {
      await this.connectToNats(options);
      this.logger.info("NATS connection established", { workerId: this.workerId });

      await this.ensureNatsObjects();
      this.logger.info("NATS objects ensured", { workerId: this.workerId });

      await this.registerTaskDefinitions();
      this.logger.info("Task definitions registered in KV", { workerId: this.workerId });

      await this.startSyncListeners();
      this.logger.info("Sync listeners started", { workerId: this.workerId });

      await this.startAsyncListeners();
      this.logger.info("Async listeners started", { workerId: this.workerId });

      this.registerSignalHandlers();

      this.isRunning = true;
      this.logger.info("Worker started successfully", {
        taskCount: Object.keys(this.registry).length,
        workerId: this.workerId,
        tasks: Object.keys(this.registry),
      });
    } catch (err) {
      this.logger.error("Failed to start worker", { error: String(err), workerId: this.workerId });
      await this.stop();
      throw err;
    }
  }

  /**
   * Gracefully stop the worker.
   *
   * Shutdown sequence:
   * 1. Stop accepting new async messages
   * 2. Drain sync subscriptions (finish in-flight requests)
   * 3. Wait for async tasks to complete
   * 4. Close NATS connection
   *
   * Safe to call multiple times or when not running.
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.logger.info("Stopping worker...", { workerId: this.workerId });

    // Stop async consumers first to prevent new message delivery
    for (const consumer of this.asyncConsumers) {
      try {
        consumer.stop();
      } catch (err) {
        this.logger.warn("Error stopping async consumer", { error: String(err), workerId: this.workerId });
      }
    }
    this.asyncConsumers = [];

    // Drain sync subscriptions to finish in-flight requests
    for (const sub of this.syncSubscriptions) {
      try {
        await sub.drain();
      } catch (err) {
        this.logger.warn("Error draining sync subscription", { error: String(err), workerId: this.workerId });
      }
    }
    this.syncSubscriptions = [];

    // Wait for all async tasks to complete
    await Promise.allSettled(this.asyncPromises);
    this.asyncPromises = [];
    this.logger.info("Async processing finished", { workerId: this.workerId });

    // Close NATS connection
    if (this.connection) {
      try {
        await this.connection.drain();
        await this.connection.close();
      } catch (err) {
        this.logger.warn("Error closing NATS connection", { error: String(err), workerId: this.workerId });
      }
      this.connection = null;
    }

    this.js = null;
    this.kv = null;
    this.resultsKv = null;
    this.isRunning = false;

    this.logger.info("Worker stopped", { workerId: this.workerId });
  }


  // ==========================================================================
  // Startup Helpers
  // ==========================================================================

  private async connectToNats(options: ConnectionOptions): Promise<void> {
    this.logger.info("Connecting to NATS...", { workerId: this.workerId });
    this.connection = await connect(options);
    this.logger.info("Connected to NATS", { server: this.connection.getServer(), workerId: this.workerId });
  }

  /**
   * Ensure required NATS infrastructure exists (KV buckets, JetStream stream).
   * Creates them if missing; uses existing if present.
   */
  private async ensureNatsObjects(): Promise<void> {
    if (!this.connection) {
      throw new Error("NATS connection not established");
    }

    this.js = this.connection.jetstream();

    // KV bucket stores task definitions for producer discovery
    try {
      this.kv = await this.js.views.kv(this.options.kvBucket);
      this.logger.debug(`KV bucket "${this.options.kvBucket}" already exists`, { workerId: this.workerId });
    } catch {
      this.kv = await this.js.views.kv(this.options.kvBucket, { history: 1 });
      this.logger.info(`Created KV bucket: ${this.options.kvBucket}`, { workerId: this.workerId });
    }

    // JetStream stream and results KV are only needed if we have async tasks
    const hasAsyncTasks = Object.values(this.registry).some(t => t.definition.type === TaskType.ASYNC);

    if (hasAsyncTasks) {
      // Results KV bucket stores async task results
      try {
        this.resultsKv = await this.js.views.kv(this.options.resultsBucket);
        this.logger.debug(`KV bucket "${this.options.resultsBucket}" already exists`, { workerId: this.workerId });
      } catch {
        this.resultsKv = await this.js.views.kv(this.options.resultsBucket, { history: 1 });
        this.logger.info(`Created KV bucket: ${this.options.resultsBucket}`, { workerId: this.workerId });
      }

      // JetStream stream for async job messages
      this.jsm = await this.connection.jetstreamManager();
      const streamSubjects = [`${this.options.asyncSubjectPrefix}>`];

      try {
        await this.jsm.streams.info(this.options.streamName);
        this.logger.debug(`JetStream stream "${this.options.streamName}" already exists`, { workerId: this.workerId });
      } catch {
        await this.jsm.streams.add({
          name: this.options.streamName,
          subjects: streamSubjects,
          retention: RetentionPolicy.Workqueue,
          discard: DiscardPolicy.New
        });
        this.logger.info(`Created JetStream stream: ${this.options.streamName}`, { workerId: this.workerId });
      }
    }
  }

  /**
   * Publish task definitions to KV bucket for producer discovery.
   */
  private async registerTaskDefinitions(): Promise<void> {
    if (!this.kv) {
      throw new Error("KV bucket not initialized");
    }

    for (const task of Object.values(this.registry)) {
      await this.kv.put(task.definition.id, this.jc.encode(task.definition));
      this.logger.debug(`Task definition ${task.definition.id} registered`, { workerId: this.workerId });
    }
  }

  private registerSignalHandlers(): void {
    const shutdown = () => {
      this.logger.info("Received shutdown signal");
      void this.stop();
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  }


  // ==========================================================================
  // Task Execution
  // ==========================================================================

  /**
   * Execute a task handler with proper error handling.
   *
   * - Parses JSON input (returns 406 on invalid JSON)
   * - Creates execution context with runId
   * - Catches handler exceptions (returns 500)
   *
   * @returns Both the parsed input and the handler output for response handling
   */
  private async executeTaskCallback(
    msgData: Uint8Array,
    task: RegisteredTask
  ): Promise<{ input: TaskRunInput; output: TaskRunOutput }> {
    // Parse input - return 406 if invalid JSON
    let input: TaskRunInput;
    try {
      input = this.jc.decode(msgData) as TaskRunInput;
    } catch (error) {
      this.logger.warn("Failed to parse task input", {
        workerId: this.workerId,
        taskId: task.definition.id,
        error: error instanceof Error ? error.message : String(error),
      });
      return {
        input: {},
        output: {
          id: "<unknown>",
          taskId: task.definition.id,
          status: 406,
          error: "Invalid JSON input",
        }
      };
    }

    // Build execution context
    const context: TaskContext = {
      runId: input.runId || uuidv7(),
      task: task.definition,
      workerId: this.workerId,
    };

    this.logger.debug("Executing task", {
      workerId: context.workerId,
      taskId: context.task.id,
      runId: context.runId,
    });

    // Execute handler - catch exceptions and return 500
    try {
      const output = await task.handler(input, context);
      this.logger.debug("Task completed", {
        workerId: context.workerId,
        taskId: context.task.id,
        runId: context.runId,
        status: output.status,
      });
      return { input, output };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error("Task execution failed", {
        workerId: context.workerId,
        taskId: context.task.id,
        runId: context.runId,
        error: errorMessage,
      });
      return {
        input,
        output: {
          id: context.runId,
          taskId: context.task.id,
          status: 500,
          error: `Unhandled exception: ${errorMessage}`,
        }
      };
    }
  }


  // ==========================================================================
  // Sync Task Handling (Request/Reply)
  // ==========================================================================

  /**
   * Start NATS subscriptions for all sync tasks.
   * Each subscription handles request/reply for one task.
   */
  private async startSyncListeners(): Promise<void> {
    if (!this.connection) {
      throw new Error("NATS connection not established");
    }

    const syncTasks = Object.values(this.registry).filter(t => t.definition.type === TaskType.SYNC);

    if (syncTasks.length === 0) {
      this.logger.debug("No sync tasks registered", { workerId: this.workerId });
      return;
    }

    for (const task of syncTasks) {
      const sub = this.connection.subscribe(task.definition.subject, {
        callback: (err, msg) => {
          if (err) {
            this.logger.error("Subscription error", { error: String(err), workerId: this.workerId, taskId: task.definition.id });
            return;
          }
          // Fire-and-forget with error logging
          this.handleSyncMessage(task, msg).catch(handlerErr => {
            this.logger.error("Unhandled error in sync handler", { error: String(handlerErr), workerId: this.workerId, taskId: task.definition.id });
          });
        },
      });

      this.syncSubscriptions.push(sub);
      this.logger.debug(`Listening on subject "${task.definition.subject}"`, { workerId: this.workerId, taskId: task.definition.id });
    }
  }

  /**
   * Handle a sync (request/reply) message.
   * Executes the task and sends the response back to the requester.
   */
  private async handleSyncMessage(task: RegisteredTask, msg: Msg): Promise<void> {
    const responseHeaders = headers();

    try {
      const { output } = await this.executeTaskCallback(msg.data, task);

      responseHeaders.set("status", output.status.toString());
      if (output.error) {
        responseHeaders.set("error", output.error);
      }

      // Only encode data if present
      const responseData = output.data ? this.jc.encode(output.data) : undefined;
      msg.respond(responseData, { headers: responseHeaders });

    } catch (error) {
      // Fallback error response if something goes wrong outside executeTaskCallback
      this.logger.error("Failed to process sync request", {
        workerId: this.workerId,
        taskId: task.definition.id,
        error: error instanceof Error ? error.message : String(error),
      });

      responseHeaders.set("status", "500");
      responseHeaders.set("error", "Internal server error");
      msg.respond(undefined, { headers: responseHeaders });
    }
  }


  // ==========================================================================
  // Async Task Handling (JetStream)
  // ==========================================================================

  /**
   * Start JetStream consumers for all async tasks.
   * Each consumer pulls messages from the stream for one task.
   */
  private async startAsyncListeners(): Promise<void> {
    if (!this.js || !this.jsm) {
      // No async tasks registered, jsm won't be initialized
      const hasAsyncTasks = Object.values(this.registry).some(t => t.definition.type === TaskType.ASYNC);
      if (!hasAsyncTasks) {
        this.logger.debug("No async tasks registered", { workerId: this.workerId });
        return;
      }
      throw new Error("JetStream not initialized");
    }

    const asyncTasks = Object.values(this.registry).filter(t => t.definition.type === TaskType.ASYNC);

    for (const task of asyncTasks) {
      const consumerName = `natq_worker_${task.definition.id}`;

      // Create durable consumer if it doesn't exist
      try {
        await this.jsm.consumers.info(this.options.streamName, consumerName);
        this.logger.debug(`Consumer "${consumerName}" already exists`, { workerId: this.workerId, taskId: task.definition.id });
      } catch {
        await this.jsm.consumers.add(this.options.streamName, {
          durable_name: consumerName,
          filter_subject: task.definition.subject,
          ack_policy: AckPolicy.Explicit,
          deliver_policy: DeliverPolicy.All,
        });
        this.logger.info(`Created consumer: ${consumerName}`, { workerId: this.workerId, taskId: task.definition.id });
      }

      // Start consuming messages
      const consumer = await this.js.consumers.get(this.options.streamName, consumerName);
      const messages = await consumer.consume();
      this.asyncConsumers.push(messages);

      // Process messages in background, track promise for graceful shutdown
      const processingPromise = this.processAsyncMessages(task, messages).catch(err => {
        this.logger.error("Fatal error in async message processor", {
          error: String(err),
          workerId: this.workerId,
          taskId: task.definition.id,
        });
      });
      this.asyncPromises.push(processingPromise);

      this.logger.info(`Consuming from subject: ${task.definition.subject}`, { workerId: this.workerId, taskId: task.definition.id });
    }
  }

  /**
   * Message processing loop for async tasks.
   * Runs until the consumer is stopped during shutdown.
   */
  private async processAsyncMessages(task: RegisteredTask, messages: ConsumerMessages): Promise<void> {
    for await (const msg of messages) {
      // Fire-and-forget with error logging - don't let one bad message stop the loop
      this.handleAsyncMessage(task, msg).catch(err => {
        this.logger.error("Unhandled error in async message handler", {
          error: String(err),
          workerId: this.workerId,
          taskId: task.definition.id,
        });
      });
    }
  }

  /**
   * Write a task result to the results KV bucket.
   */
  private async putResultToKv(
    task: RegisteredTask,
    key: string,
    output: TaskRunOutput
  ): Promise<void> {
    if (!this.resultsKv) {
      this.logger.error("Results KV bucket not initialized", {
        workerId: this.workerId,
        taskId: task.definition.id,
      });
      return;
    }

    try {
      await this.resultsKv.put(key, this.jc.encode(output));

      this.logger.debug("Result written to KV", {
        key,
        status: output.status,
        workerId: this.workerId,
        taskId: task.definition.id,
      });
    } catch (error) {
      // Log but don't throw - KV failure shouldn't crash the worker
      this.logger.error("Failed to write result to KV", {
        key,
        error: error instanceof Error ? error.message : String(error),
        workerId: this.workerId,
        taskId: task.definition.id,
      });
    }
  }

  /**
   * Delete a task result from the results KV bucket.
   * Used for fire-and-forget tasks with dropResultOnSuccess.
   */
  private async deleteResultFromKv(
    task: RegisteredTask,
    key: string
  ): Promise<void> {
    try {
      await this.resultsKv?.delete(key);

      this.logger.debug("Result deleted from KV", {
        key,
        workerId: this.workerId,
        taskId: task.definition.id,
      });
    } catch (error) {
      // Log but don't throw - KV failure shouldn't crash the worker
      this.logger.error("Failed to delete result from KV", {
        key,
        error: error instanceof Error ? error.message : String(error),
        workerId: this.workerId,
        taskId: task.definition.id,
      });
    }
  }

  /**
   * Handle a single async (JetStream) message.
   *
   * Flow:
   * 1. Parse input to get runId
   * 2. Create PROCESSING entry in KV (status 100)
   * 3. Execute handler
   * 4. Update KV with final result (success/client error) or NAK (server error)
   *
   * Acknowledgment strategy based on status code:
   * - `< 300`: ACK - success, update KV with result
   * - `300-499`: TERM - client error, update KV with error
   * - `>= 500`: NAK - server error, do NOT update KV (stays at PROCESSING)
   */
  private async handleAsyncMessage(task: RegisteredTask, msg: JsMsg): Promise<void> {
    // Send periodic heartbeats to prevent message timeout during long tasks
    const heartbeatInterval = setInterval(() => {
      try {
        msg.working();
      } catch {
        // Message may have been acked/nacked already - ignore
      }
    }, this.options.heartbeatIntervalMs);

    let runId: string | undefined;
    let input: TaskRunInput;

    try {
      // Step 1: Parse input to get runId
      try {
        input = this.jc.decode(msg.data) as TaskRunInput;
      } catch (error) {
        clearInterval(heartbeatInterval);
        this.logger.warn("Failed to parse async task input", {
          workerId: this.workerId,
          taskId: task.definition.id,
          error: error instanceof Error ? error.message : String(error),
        });
        // Invalid JSON - terminate without KV entry
        msg.term("Invalid JSON input");
        return;
      }

      // Step 2: Determine runId and KV key
      runId = input.runId || uuidv7();
      const kvKey = `${task.definition.id}.${runId}`;

      // Step 3: Create PROCESSING entry in KV before handler execution
      await this.putResultToKv(task, kvKey, {
        id: runId,
        taskId: task.definition.id,
        status: StatusCode.PROCESSING,
      });

      // Step 4: Build context and execute handler
      const context: TaskContext = {
        runId,
        task: task.definition,
        workerId: this.workerId,
      };

      this.logger.debug("Executing async task", {
        workerId: context.workerId,
        taskId: context.task.id,
        runId: context.runId,
      });

      let output: TaskRunOutput;
      try {
        output = await task.handler(input, context);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        this.logger.error("Async task execution failed", {
          workerId: context.workerId,
          taskId: context.task.id,
          runId: context.runId,
          error: errorMessage,
        });
        output = {
          id: runId,
          taskId: task.definition.id,
          status: StatusCode.INTERNAL_ERROR,
          error: `Unhandled exception: ${errorMessage}`,
        };
      }

      clearInterval(heartbeatInterval);

      // Step 5: Handle based on status
      if (output.status < 300) {
        // Success - update KV and ACK
        await this.putResultToKv(task, kvKey, output);
        this.logger.debug("Async task succeeded", {
          taskId: task.definition.id,
          status: output.status,
          runId: output.id,
          workerId: this.workerId,
        });
        msg.ack();

        // Delete result if fire-and-forget mode
        if (input.dropResultOnSuccess) {
          await this.deleteResultFromKv(task, kvKey);
        }

      } else if (output.status < 500) {
        // Client error - update KV and TERM
        await this.putResultToKv(task, kvKey, output);
        this.logger.warn("Async task client error", {
          taskId: task.definition.id,
          status: output.status,
          runId: output.id,
          workerId: this.workerId,
          error: output.error,
        });
        msg.term(output.error);

      } else {
        // Server error - NAK, do NOT update KV (stays at PROCESSING until retry succeeds)
        const deliveryCount = msg.info.deliveryCount ?? 1;
        const delay = Math.min(Math.pow(2, deliveryCount) * 1000, 60000);
        this.logger.warn("Async task server error, will retry", {
          taskId: task.definition.id,
          status: output.status,
          error: output.error,
          retryDelay: delay,
          deliveryCount,
          runId,
          workerId: this.workerId,
        });
        msg.nak(delay);
      }

    } catch (error) {
      // Unexpected error (e.g., KV write failed) - retry after 5s
      clearInterval(heartbeatInterval);
      this.logger.error("Unexpected error processing async task", {
        taskId: task.definition.id,
        workerId: this.workerId,
        runId,
        error: error instanceof Error ? error.message : String(error),
      });
      msg.nak(5000);
    }
  }
}
