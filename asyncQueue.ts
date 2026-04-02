/**
 * Concurrency Limiter for heavy QTO tasks.
 * Ensures the Node.js server doesn't crash when multiple users request QTO simultaneously.
 */
export class AsyncQueue {
  private concurrency: number;
  private running: number = 0;
  private queue: Array<() => void> = [];

  constructor(concurrency: number) {
    this.concurrency = concurrency;
  }

  async enqueue<T>(task: () => Promise<T>): Promise<T> {
    if (this.running >= this.concurrency) {
      await new Promise<void>((resolve) => this.queue.push(resolve));
    }
    
    this.running++;
    try {
      return await task();
    } finally {
      this.running--;
      const next = this.queue.shift();
      if (next) {
        next();
      }
    }
  }
}

// Global instance to limit heavy python processes to max 2 concurrent executions
export const qtoEngineQueue = new AsyncQueue(2);
