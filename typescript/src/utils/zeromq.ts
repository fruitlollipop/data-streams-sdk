import { Request, Publisher, Subscriber } from "zeromq";

/**
 * ZeroMQ error thrown when operations fail or time out.
 */
export class ZmqError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = "ZmqError";
    Object.setPrototypeOf(this, ZmqError.prototype);
  }
}

// ─── REQ-REP Client ─────────────────────────────────────────────────────────

export interface ReqClientOptions {
  /** REP server endpoint, e.g. "tcp://127.0.0.1:5555" */
  endpoint: string;
  /** Send timeout in ms (-1 = wait forever, 0 = immediate, default -1) */
  sendTimeout?: number;
  /** Receive timeout in ms (-1 = wait forever, 0 = immediate, default -1) */
  receiveTimeout?: number;
}

/**
 * REQ-REP pattern client.
 *
 * Wraps a ZeroMQ Request socket for async request-reply communication.
 *
 * @example
 * ```ts
 * const client = new ZmqReqClient({ endpoint: "tcp://127.0.0.1:5555" });
 * await client.connect();
 * const reply = await client.request("hello");
 * await client.close();
 * ```
 */
export class ZmqReqClient {
  private socket: Request;
  private endpoint: string;
  private connected = false;

  constructor(options: ReqClientOptions) {
    this.endpoint = options.endpoint;
    this.socket = new Request();

    if (options.sendTimeout !== undefined) {
      this.socket.sendTimeout = options.sendTimeout;
    }
    if (options.receiveTimeout !== undefined) {
      this.socket.receiveTimeout = options.receiveTimeout;
    }
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    this.socket.connect(this.endpoint);
    this.connected = true;
  }

  /**
   * Send a request and wait for the reply.
   *
   * @param data - string or Buffer to send
   * @returns reply message parts as Buffer[]
   */
  async request(data: string | Buffer): Promise<Buffer[]> {
    if (!this.connected) {
      throw new ZmqError("Socket not connected. Call connect() first.");
    }
    try {
      await this.socket.send(data);
      const [result] = await this.socket.receive();
      return [result];
    } catch (err) {
      throw new ZmqError(
        "REQ-REP request failed",
        err instanceof Error ? err : undefined
      );
    }
  }

  /**
   * Send a multipart request and wait for the reply.
   */
  async requestMultipart(parts: (string | Buffer)[]): Promise<Buffer[]> {
    if (!this.connected) {
      throw new ZmqError("Socket not connected. Call connect() first.");
    }
    try {
      await this.socket.send(parts);
      return await this.socket.receive();
    } catch (err) {
      throw new ZmqError(
        "REQ-REP multipart request failed",
        err instanceof Error ? err : undefined
      );
    }
  }

  async close(): Promise<void> {
    if (!this.connected) return;
    this.connected = false;
    this.socket.close();
  }
}

// ─── PUB-SUB Publisher ──────────────────────────────────────────────────────

export interface PubOptions {
  /** Bind endpoint, e.g. "tcp://*:5556" */
  endpoint: string;
  /** Max outgoing message queue size (ZMQ_SNDHWM, default 1000) */
  sendHighWaterMark?: number;
}

/**
 * PUB-SUB pattern publisher.
 *
 * @example
 * ```ts
 * const pub = new ZmqPublisher({ endpoint: "tcp://*:5556", sendHighWaterMark: 500 });
 * await pub.bind();
 * await pub.publish("topic", "payload");
 * await pub.close();
 * ```
 */
export class ZmqPublisher {
  private socket: Publisher;
  private endpoint: string;
  private bound = false;

  constructor(options: PubOptions) {
    this.endpoint = options.endpoint;
    this.socket = new Publisher();

    if (options.sendHighWaterMark !== undefined) {
      this.socket.sendHighWaterMark = options.sendHighWaterMark;
    }
  }

  async bind(): Promise<void> {
    if (this.bound) return;
    await this.socket.bind(this.endpoint);
    this.bound = true;
  }

  /**
   * Publish a message on a topic.
   *
   * @param topic - topic prefix for subscriber filtering
   * @param data  - message payload
   */
  async publish(topic: string, data: string | Buffer): Promise<void> {
    if (!this.bound) {
      throw new ZmqError("Socket not bound. Call bind() first.");
    }
    try {
      await this.socket.send([topic, data]);
    } catch (err) {
      throw new ZmqError(
        "Publish failed",
        err instanceof Error ? err : undefined
      );
    }
  }

  async close(): Promise<void> {
    if (!this.bound) return;
    this.bound = false;
    this.socket.close();
  }
}

// ─── PUB-SUB Subscriber ────────────────────────────────────────────────────

export interface SubOptions {
  /** Publisher endpoint to connect to, e.g. "tcp://127.0.0.1:5556" */
  endpoint: string;
  /** Max incoming message queue size (ZMQ_RCVHWM, default 1000) */
  receiveHighWaterMark?: number;
  /** Receive timeout in ms (-1 = wait forever, default 5000) */
  receiveTimeout?: number;
  /** Topics to subscribe to. Empty string subscribes to all. */
  topics?: string[];
}

export interface SubMessage {
  topic: Buffer;
  data: Buffer;
}

/**
 * PUB-SUB pattern subscriber with receive timeout support.
 *
 * @example
 * ```ts
 * const sub = new ZmqSubscriber({
 *   endpoint: "tcp://127.0.0.1:5556",
 *   receiveHighWaterMark: 200,
 *   receiveTimeout: 3000,
 *   topics: ["price"],
 * });
 * await sub.connect();
 *
 * // Iterate messages (throws ZmqError on timeout)
 * for await (const msg of sub.messages()) {
 *   console.log(msg.topic.toString(), msg.data.toString());
 * }
 * ```
 */
export class ZmqSubscriber {
  private socket: Subscriber;
  private endpoint: string;
  private connected = false;
  private closed = false;
  private receiveTimeoutMs: number;

  constructor(options: SubOptions) {
    this.endpoint = options.endpoint;
    this.receiveTimeoutMs = options.receiveTimeout ?? 5000;
    this.socket = new Subscriber();

    if (options.receiveHighWaterMark !== undefined) {
      this.socket.receiveHighWaterMark = options.receiveHighWaterMark;
    }
    this.socket.receiveTimeout = this.receiveTimeoutMs;

    // Subscribe to specified topics (or all if none given)
    const topics = options.topics ?? [""];
    for (const t of topics) {
      this.socket.subscribe(t);
    }
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    this.socket.connect(this.endpoint);
    this.connected = true;
  }

  /**
   * Receive a single message. Returns null if the socket has been closed.
   * Throws ZmqError on receive timeout.
   */
  async receive(): Promise<SubMessage | null> {
    if (this.closed) return null;
    if (!this.connected) {
      throw new ZmqError("Socket not connected. Call connect() first.");
    }
    try {
      const [topic, data] = await this.socket.receive();
      return { topic, data };
    } catch (err: any) {
      if (this.closed) return null;
      if (err?.code === "EAGAIN") {
        throw new ZmqError(
          `Receive timed out after ${this.receiveTimeoutMs}ms`
        );
      }
      throw new ZmqError(
        "Subscriber receive failed",
        err instanceof Error ? err : undefined
      );
    }
  }

  /**
   * Async generator that yields messages until the socket is closed.
   * Throws ZmqError on receive timeout so the caller can decide how to handle it.
   */
  async *messages(): AsyncGenerator<SubMessage, void, unknown> {
    while (!this.closed) {
      const msg = await this.receive();
      if (msg === null) return;
      yield msg;
    }
  }

  /** Subscribe to an additional topic at runtime. */
  subscribe(topic: string): void {
    this.socket.subscribe(topic);
  }

  /** Unsubscribe from a topic at runtime. */
  unsubscribe(topic: string): void {
    this.socket.unsubscribe(topic);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.connected = false;
    this.socket.close();
  }
}
