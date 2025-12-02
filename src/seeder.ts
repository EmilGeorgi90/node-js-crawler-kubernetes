import { Redis } from "ioredis";

const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
const SEED_URL = process.env.SEED_URL || "https://www.lauritz.com";

(async () => {
  const redis = new Redis(REDIS_URL);
  await redis.del(
    "crawl:queue",
    "ai:revisit",
    "crawl:seen",
    "crawl:inflight",
    "crawl:results_db",
    "crawl:results_raw",
    "crawl:dlq"
  );
  await redis.lpush("crawl:queue", JSON.stringify({ url: SEED_URL, depth: 0 }));
  console.log("Seeded:", SEED_URL);
  process.exit(0);
})();
