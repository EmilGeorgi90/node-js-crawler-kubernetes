import { Redis } from "ioredis";
import { cfg } from "./config.js";
import { discoverSitemaps, makeLogger } from "./shared.js";
import { request } from "undici";

const log = makeLogger("seeder");

async function seedList(redis: Redis, url: string) {
  await redis.lpush(cfg.QUEUE_KEY, JSON.stringify({ url, depth: 0 }));
}
async function seedStream(redis: Redis, url: string) {
  await redis.xadd(
    cfg.QUEUE_STREAM,
    "*",
    "task",
    JSON.stringify({ url, depth: 0 })
  );
}

async function expandSitemaps(seedUrl: string): Promise<string[]> {
  const maps = await discoverSitemaps(seedUrl);
  const urls: string[] = [];
  for (const sm of maps) {
    try {
      const res = await request(sm);
      if (res.statusCode >= 200 && res.statusCode < 400) {
        const xml = await res.body.text();
        const matches = [...xml.matchAll(/<loc>\s*(.*?)\s*<\/loc>/gi)].map(
          (m) => m[1]
        );
        urls.push(...matches);
      }
    } catch {}
  }
  return Array.from(new Set(urls));
}

(async () => {
  const SEED_URL = process.env.SEED_URL || "https://www.lauritz.com";
  const redis = new Redis(cfg.REDIS_URL);

  // Clean frontier (keep seen)
  if (cfg.FRONTIER_MODE === "stream") {
    await redis.del(cfg.REVISIT_PENDING_KEY);
  } else {
    await redis.del(cfg.QUEUE_KEY, cfg.REVISIT_KEY);
  }

  // Seed root
  if (cfg.FRONTIER_MODE === "stream") await seedStream(redis, SEED_URL);
  else await seedList(redis, SEED_URL);
  log.info(`Seeded: ${SEED_URL}`);

  // Optional: seed from sitemaps (only a handful to avoid a flood)
  if (process.env.SEED_SITEMAPS === "true") {
    const urls = await expandSitemaps(SEED_URL);
    const first = urls.slice(0, 200);
    for (const u of first) {
      if (cfg.FRONTIER_MODE === "stream") await seedStream(redis, u);
      else await seedList(redis, u);
    }
    log.info(`Seeded from sitemaps: ${first.length} urls`);
  }

  process.exit(0);
})();
