import type { Redis } from "ioredis";

export function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export async function brpopJson<T = any>(
  redis: Redis,
  key: string,
  timeoutSec = 5
): Promise<T | null> {
  const res = await redis.brpop(key, timeoutSec);
  if (!res) return null;
  const [, payload] = res;
  try {
    return JSON.parse(payload) as T;
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error(
      `[collector] JSON parse failed for key=${key}:`,
      (e as any)?.message || e
    );
    return null;
  }
}

export function makeLog(prefix: string) {
  const tag = `[${prefix}]`;
  return {
    info: (m: string, ...a: any[]) => console.log(tag, m, ...a),
    warn: (m: string, ...a: any[]) => console.warn(tag, m, ...a),
    error: (m: string, ...a: any[]) => console.error(tag, m, ...a),
  };
}
