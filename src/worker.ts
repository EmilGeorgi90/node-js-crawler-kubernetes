import type { Page } from "puppeteer";
import type { ExtractResult } from "./shared.js";
import {
  env,
  makeLogger,
  createRedis,
  launchBrowser,
  configurePage,
  navigateWithRetries,
  setConsentCookieIfConfigured,
  sameOriginOnly,
  extractSelective,
  extractAiMode,
  sleep,
} from "./shared.js";
import type { Redis } from "ioredis";
import http from "node:http";
import client from "prom-client";

const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });
const pagesProcessed = new client.Counter({
  name: "crawler_pages_processed_total",
  help: "Total pages processed",
  labelNames: ["mode", "queue"],
});
const pagesFailed = new client.Counter({
  name: "crawler_pages_failed_total",
  help: "Total pages failed",
  labelNames: ["reason"],
});
const linksQueued = new client.Counter({
  name: "crawler_links_enqueued_total",
  help: "Links enqueued",
});
const queueDepth = new client.Gauge({
  name: "crawler_queue_depth",
  help: "Frontier depth",
  labelNames: ["queue"],
});
function startMetricsServer(port = Number(process.env.METRICS_PORT || 9100)) {
  const srv = http.createServer(async (_req, res) => {
    if (_req.url === "/metrics") {
      const b = await registry.metrics();
      res.writeHead(200, { "Content-Type": registry.contentType });
      return res.end(b);
    }
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
  });
  srv.listen(port, () => console.log(`[metrics] worker on :${port}`));
}

const QUEUE_KEY = process.env.QUEUE_KEY || "crawl:queue";
const REVISIT_KEY = process.env.REVISIT_KEY || "ai:revisit";
const REVISIT_PENDING_KEY =
  process.env.REVISIT_PENDING_KEY || "ai:revisit:pending";
const RESULTS_DB_KEY = process.env.RESULTS_DB_KEY || "crawl:results_db";
const RESULTS_RAW_KEY = process.env.RESULTS_RAW_KEY || "crawl:results_raw";
const SEEN_KEY = process.env.SEEN_KEY || "crawl:seen";
const INFLIGHT_KEY = process.env.INFLIGHT_KEY || "crawl:inflight";
const DLQ_KEY = process.env.DLQ_KEY || "crawl:dlq";

const MAX_DEPTH = Number(process.env.MAX_DEPTH || "3");
const INCLUDE_SELECTORS = (process.env.INCLUDE_SELECTORS || "").trim();
const AI_MODE = (process.env.AI_MODE || "false").toLowerCase() === "true";

const log = makeLogger("worker");
const redis: Redis = createRedis(process.env.REDIS_URL || env.REDIS_URL);

(async () => {
  const browser = await launchBrowser();
  await setConsentCookieIfConfigured(browser, log);
  startMetricsServer(Number(process.env.METRICS_PORT || 9100));

  log.info(
    `Up. HEADLESS=${env.HEADLESS} AI_MODE=${AI_MODE} SELECTORS="${
      INCLUDE_SELECTORS || "(empty)"
    }"`
  );

  while (true) {
    try {
      let popped = await redis.brpop(REVISIT_KEY, 1);
      if (popped) log.info(`[AI-REVISIT] ${popped[1]}`);
      else {
        popped = await redis.brpop(QUEUE_KEY, 1);
        if (popped) log.info(`[INITIAL] ${popped[1]}`);
      }
      if (!popped) continue;

      try {
        const [qMain, qAi] = await Promise.all([
          redis.llen(QUEUE_KEY),
          redis.llen(REVISIT_KEY),
        ]);
        queueDepth.set({ queue: "crawl" }, qMain);
        queueDepth.set({ queue: "ai_revisit" }, qAi);
      } catch {}

      const task = JSON.parse(popped[1]) as { url: string; depth: number };
      const sourceQueue = popped[0];
      const { url, depth } = task;
      if (!url || depth > MAX_DEPTH) continue;

      const inflightAdded = await redis.sadd(INFLIGHT_KEY, url);
      if (inflightAdded === 0) {
        log.info(`[SKIP] inflight: ${url}`);
        continue;
      }

      const alreadySeen = await redis.sismember(SEEN_KEY, url);
      if (alreadySeen === 1) {
        log.info(`[SKIP] already seen: ${url}`);
        if (sourceQueue === REVISIT_KEY) {
          await redis.srem(REVISIT_PENDING_KEY, url);
        }
        await redis.srem(INFLIGHT_KEY, url);
        continue;
      }

      log.info(`[NAV] -> ${url} (depth=${depth})`);

      const ctx = await browser.createBrowserContext();
      const page: Page = await ctx.newPage();
      try {
        await configurePage(page);
        const ok = await navigateWithRetries(page, url);
        if (!ok) {
          log.warn(`[NAV FAIL] ${url}`);
          pagesFailed.inc({ reason: "navigate" });
          await redis.lpush(
            DLQ_KEY,
            JSON.stringify({ url, depth, reason: "navigate" })
          );
          await redis.srem(INFLIGHT_KEY, url);
          await ctx.close().catch(() => {});
          continue;
        }
        log.info(`[NAV OK] ${url}`);

        const selectorMode = AI_MODE || !INCLUDE_SELECTORS ? "ai" : "css";
        const data: ExtractResult =
          selectorMode === "ai"
            ? await extractAiMode(page)
            : await extractSelective(page, INCLUDE_SELECTORS);
        log.info(
          `[EXTRACT ${selectorMode.toUpperCase()}] title="${
            data.title
          }" anchors=${(data.anchors || []).length}`
        );

        const anchors = sameOriginOnly(data.anchors || [], page.url());
        const nextDepth = depth + 1;
        if (nextDepth <= MAX_DEPTH && anchors.length) {
          for (const link of anchors) {
            const wasSeen = await redis.sismember(SEEN_KEY, link);
            if (wasSeen === 0)
              await redis.lpush(
                QUEUE_KEY,
                JSON.stringify({ url: link, depth: nextDepth })
              );
          }
          linksQueued.inc(anchors.length);
        }

        const payload = {
          sitesUrl: url,
          title: data.title ?? null,
          body: data.body ?? null,
          html: data.html ?? "",
          meta: {
            fetchedAt: new Date().toISOString(),
            domain: new URL(url).hostname,
            selector_mode: selectorMode,
            source_queue: sourceQueue,
          },
        };
        await redis.lpush(RESULTS_DB_KEY, JSON.stringify(payload));
        await redis.lpush(RESULTS_RAW_KEY, JSON.stringify(payload));

        await redis.sadd(SEEN_KEY, url);
        if (sourceQueue === REVISIT_KEY) {
          await redis.srem(REVISIT_PENDING_KEY, url);
        }
        await redis.srem(INFLIGHT_KEY, url);
        pagesProcessed.inc({
          mode: selectorMode,
          queue: sourceQueue === REVISIT_KEY ? "ai-revisit" : "initial",
        });
      } catch (err: any) {
        pagesFailed.inc({ reason: "exception" });
        log.error(`Page error: ${err?.message || err}`);
        await redis.lpush(
          DLQ_KEY,
          JSON.stringify({
            url,
            depth,
            reason: "exception",
            message: err?.message || String(err),
          })
        );
        await redis.srem(INFLIGHT_KEY, url);
      } finally {
        await page.close().catch(() => {});
        await ctx.close().catch(() => {});
      }
    } catch (loopErr: any) {
      log.error(`Loop error: ${loopErr?.message || loopErr}`);
      await sleep(250);
    }
  }
})();
