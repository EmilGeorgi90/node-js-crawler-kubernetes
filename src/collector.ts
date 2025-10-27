import * as fs from 'node:fs';
import * as path from 'node:path';
import Redis from 'ioredis';

const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const RESULTS_KEY = process.env.RESULTS_KEY || 'crawl:results';
const OUT_PATH = process.env.OUT_PATH || '/data/out.jsonl';

(async () => {
  const redis = new Redis(REDIS_URL);
  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  const stream = fs.createWriteStream(OUT_PATH, { flags: 'a' });

  console.log('Collector writing to', OUT_PATH);
  while (true) {
    const res = await redis.brpop(RESULTS_KEY, 5);
    if (!res) continue;
    stream.write(res[1] + '\n');
  }
})();