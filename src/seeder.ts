import Redis from "ioredis";
import { canon } from "./shared";

const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
const QUEUE_KEY = process.env.QUEUE_KEY || "crawl:queue";
const SEEN_KEY = process.env.SEEN_KEY || "crawl:seen";
const VISITED_KEY = process.env.VISITED_KEY || "crawl:visited";

const SEED_URL = process.env.SEED_URL!; // required

(async () => {
  if (!SEED_URL) throw new Error("SEED_URL is required");
  const redis = new Redis(REDIS_URL);
  const url = canon(SEED_URL);
  const origin = new URL(url).origin;
  await redis.del(QUEUE_KEY, SEEN_KEY, VISITED_KEY);
  await redis.sadd(SEEN_KEY, url);
  await redis.rpush(QUEUE_KEY, JSON.stringify({ url, depth: 0, origin }));
  console.log("Seeded", url);
  process.exit(0);
})();
