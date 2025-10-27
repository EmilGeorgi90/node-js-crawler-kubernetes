import Redis from 'ioredis';
import { canon } from './shared';

const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const QUEUE_KEY = process.env.QUEUE_KEY || 'crawl:queue';

const SEED_URL = process.env.SEED_URL!; // required

(async () => {
  if (!SEED_URL) throw new Error('SEED_URL is required');
  const redis = new Redis(REDIS_URL);
  const url = canon(SEED_URL);
  const origin = new URL(url).origin;
  await redis.del(QUEUE_KEY, 'crawl:visited'); // optional: clear previous run
  await redis.rpush(QUEUE_KEY, JSON.stringify({ url, depth: 0, origin }));
  console.log('Seeded', url);
  process.exit(0);
})();