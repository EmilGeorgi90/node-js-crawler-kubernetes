import Redis from 'ioredis';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const QUEUE_KEY = process.env.QUEUE_KEY || 'crawl:queue';
const SEED_URL  = process.env.SEED_URL  || 'https://www.lauritz.com';

(async () => {
  const redis = new Redis(REDIS_URL);
  const task = { url: SEED_URL, depth: 0 };
  await redis.lpush(QUEUE_KEY, JSON.stringify(task));
  console.log('Seeded:', task);
  process.exit(0);
})();