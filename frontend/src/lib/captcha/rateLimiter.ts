import { Mutex } from 'async-mutex';

interface RateLimitEntry {
  timestamps: number[];
  blocked: boolean;
}

class RateLimiter {
  private ipMap: Map<string, RateLimitEntry>;
  private mutex: Mutex;
  private readonly windowSize: number;
  private readonly maxRequests: number;
  private readonly blockDuration: number;

  constructor(windowSize: number = 60, maxRequests: number = 30, blockDuration: number = 600) {
    this.ipMap = new Map();
    this.mutex = new Mutex();
    this.windowSize = windowSize;
    this.maxRequests = maxRequests;
    this.blockDuration = blockDuration;
  }

  async shouldShowCaptcha(ip: string): Promise<boolean> {
    return await this.mutex.runExclusive(async () => {
      const now = Date.now();
      let entry = this.ipMap.get(ip);

      if (!entry) {
        entry = { timestamps: [now], blocked: false };
        this.ipMap.set(ip, entry);
        return false;
      }

      if (entry.blocked) {
        if (now - entry.timestamps[entry.timestamps.length - 1] > this.blockDuration * 1000) {
          entry.blocked = false;
          entry.timestamps = [now];
          return false;
        } else {
          return true;
        }
      }

      entry.timestamps.push(now);

      // Remove timestamps older than the window size
      while (entry.timestamps.length > 0 && entry.timestamps[0] < now - this.windowSize * 1000) {
        entry.timestamps.shift();
      }

      if (entry.timestamps.length > this.maxRequests) {
        entry.blocked = true;
        return true;
      }

      return false;
    });
  }

  async allow(ip: string): Promise<void> {
    await this.mutex.runExclusive(() => {
      this.ipMap.delete(ip);
    });
  }

  async cleanup(): Promise<void> {
    await this.mutex.runExclusive(() => {
      const now = Date.now();
      for (const [ip, entry] of this.ipMap.entries()) {
        if (
          entry.timestamps.length == 0 ||
          now - entry.timestamps[entry.timestamps.length - 1] > this.windowSize * 1000
        ) {
          this.ipMap.delete(ip);
        }
      }
    });
  }
}

const rateLimiter = new RateLimiter();

export const shouldShowCaptcha = async (ip: string) => {
  return await rateLimiter.shouldShowCaptcha(ip);
};

export const allow = async (ip: string) => {
  return await rateLimiter.allow(ip);
};

export const cleanup = async () => {
  return await rateLimiter.cleanup();
};
