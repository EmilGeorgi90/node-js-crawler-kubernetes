import Redis from "ioredis";
import {
  allowedByRobotsFactory,
  canon,
  configurePage,
  extractSelective,
  launchBrowser,
  navigateStable,
  seedCookie,
} from "./shared";

const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
const QUEUE_KEY = process.env.QUEUE_KEY || "crawl:queue";
const RESULTS_DB_KEY = process.env.RESULTS_DB_KEY || "crawl:results_db";
const RESULTS_RAW_KEY = process.env.RESULTS_RAW_KEY || "crawl:results_raw";
const VISITED_KEY = process.env.VISITED_KEY || "crawl:visited";
const SEEN_KEY = process.env.SEEN_KEY || "crawl:seen";
const HOST_SEM_PREFIX = process.env.HOST_SEM_PREFIX || "crawl:host:"; // + origin + ':inflight'

const SELECTOR = process.env.SELECTOR || "body";
const INCLUDE_SELECTORS = (process.env.INCLUDE_SELECTORS || "").trim();
const MAX_DEPTH = Number(process.env.MAX_DEPTH || "3");
const PER_HOST = Number(process.env.PER_HOST || "6");
const HEADLESS: boolean = ((): any => {
  const v = (process.env.HEADLESS || "true").toLowerCase();
  return v === "true";
})();

const COOKIE_URL = process.env.COOKIE_URL; // optional
const COOKIE_NAME = process.env.COOKIE_NAME; // optional
const COOKIE_VALUE = process.env.COOKIE_VALUE; // optional

const PRODUCT_URL_REGEX = new RegExp(
  process.env.PRODUCT_URL_REGEX ?? String.raw`\/[^\/]+\/(\d+)(?:\?.*)?$`,
  "i"
);

const luaTryAcquire = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local v = tonumber(redis.call('GET', key) or '0')
if v < limit then redis.call('INCR', key) return 1 else return 0 end
`;

const luaEnqueueIfNew = `
local seen = KEYS[1]
local q = KEYS[2]
local task = ARGV[1]
local url = ARGV[2]
if redis.call('SADD', seen, url) == 1 then return redis.call('RPUSH', q, task) else return 0 end
`;

(async () => {
  const redis = new Redis(REDIS_URL);
  const tryAcquireSha = await redis.script("LOAD", luaTryAcquire);
  const enqIfNewSha = await redis.script("LOAD", luaEnqueueIfNew);
  const allowRobots = allowedByRobotsFactory();

  const { browser, context } = await launchBrowser(HEADLESS);
  await seedCookie(
    context,
    COOKIE_URL && COOKIE_NAME && COOKIE_VALUE
      ? { url: COOKIE_URL, name: COOKIE_NAME, value: COOKIE_VALUE }
      : undefined
  );

  const release = async (origin: string) => {
    await redis.decr(`${HOST_SEM_PREFIX}${origin}:inflight`).catch(() => {});
  };

  while (true) {
    const res = await redis.brpop(QUEUE_KEY, 5);
    if (!res) continue;
    const task = JSON.parse(res[1]) as {
      url: string;
      depth: number;
      origin: string;
    };

    try {
      if (task.depth > MAX_DEPTH) continue;
      const url = canon(task.url);
      const origin = new URL(url).origin;
      if (origin !== task.origin) continue;

      const added = await redis.sadd(VISITED_KEY, url);
      if (added === 0) continue;

      while (true) {
        const ok = await redis.evalsha(
          String(tryAcquireSha),
          1,
          `${HOST_SEM_PREFIX}${origin}:inflight`,
          String(PER_HOST)
        );
        if (ok === 1) break;
        await new Promise((r) => setTimeout(r, 50));
      }

      const page = await context.newPage();
      try {
        await configurePage(page);
        if (!(await allowRobots(url))) {
          await release(origin);
          await page.close();
          continue;
        }
        await navigateStable(page, url);

        const { anchors, title, body, html } = await extractSelective(
          page,
          INCLUDE_SELECTORS || SELECTOR
        );

        const m = PRODUCT_URL_REGEX.exec(url);
        const isProduct = !!m;
        const productId = m?.[1] ?? null;
        const meta = {
          fetchedAt: new Date().toISOString(),
          domain: new URL(url).hostname,
          isProduct,
          productId,
        };

        const payload = {
          sitesUrl: url,
          title,
          body: body || undefined,
          html: html || "",
          meta,
        };
        await redis.lpush(RESULTS_DB_KEY, JSON.stringify(payload));
        await redis.lpush(RESULTS_RAW_KEY, JSON.stringify(payload));

        const nextDepth = task.depth + 1;
        if (nextDepth <= MAX_DEPTH) {
          for (const a of anchors as string[]) {
            try {
              const abs = new URL(a, url).toString();
              const cu = canon(abs).split("#")[0];
              const t = JSON.stringify({
                url: cu,
                depth: nextDepth,
                origin: task.origin,
              });
              await redis.evalsha(
                String(enqIfNewSha),
                2,
                SEEN_KEY,
                QUEUE_KEY,
                t,
                cu
              );
            } catch {}
          }
        }
      } finally {
        await page.close().catch(() => {});
        await release(origin);
      }
    } catch (e) {
      // swallow
    }
  }
})();
