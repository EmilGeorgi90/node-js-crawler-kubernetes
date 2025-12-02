import { z } from 'zod';

const Env = z.object({
  REDIS_URL: z.string().default('redis://redis:6379'),
  // Frontier: list|stream
  FRONTIER_MODE: z.enum(['list', 'stream']).default('stream'),

  // Redis keys/streams
  QUEUE_KEY: z.string().default('crawl:queue'),
  REVISIT_KEY: z.string().default('ai:revisit'),
  REVISIT_PENDING_KEY: z.string().default('ai:revisit:pending'),
  SEEN_KEY: z.string().default('crawl:seen'),
  INFLIGHT_KEY: z.string().default('crawl:inflight'),
  DLQ_KEY: z.string().default('crawl:dlq'),

  // Streams config (only used when FRONTIER_MODE=stream)
  QUEUE_STREAM: z.string().default('crawl:stream'),
  REVISIT_STREAM: z.string().default('ai:revisit:stream'),
  QUEUE_GROUP: z.string().default('workers'),
  WORKER_CONSUMER: z.string().default(() => `w-${Math.random().toString(36).slice(2, 8)}`),

  // Results (lists)
  RESULTS_DB_KEY: z.string().default('crawl:results_db'),
  RESULTS_RAW_KEY: z.string().default('crawl:results_raw'),

  // Crawl behavior
  MAX_DEPTH: z.coerce.number().int().min(0).max(10).default(3),
  AI_MODE: z.enum(['true', 'false']).default('true'),
  INCLUDE_SELECTORS: z.string().default(''),
  HEADLESS: z.enum(['true', 'false']).default('true'),

  // Fetch timeouts
  NAV_TIMEOUT_MS: z.coerce.number().default(30000),
  OP_TIMEOUT_MS: z.coerce.number().default(15000),
  MAX_NAV_RETRIES: z.coerce.number().int().min(0).max(5).default(2),

  // Cookie (optional)
  COOKIE_URL: z.string().default(''),
  COOKIE_NAME: z.string().default(''),
  COOKIE_VALUE: z.string().default(''),
  COOKIE_DOMAIN: z.string().default(''),
  COOKIE_PATH: z.string().default('/'),

  // Metrics
  METRICS_PORT: z.coerce.number().default(9100),

  // Static-first fetch toggle
  STATIC_FIRST: z.enum(['true','false']).default('true'),

  // Sitemap seeding
  ENABLE_SITEMAP: z.enum(['true','false']).default('true')
});

export type Cfg = z.infer<typeof Env>;
export const cfg: Cfg = Env.parse(process.env);

export const isTrue = (v: string) => v.toLowerCase() === 'true';
