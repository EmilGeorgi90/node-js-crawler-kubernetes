import type { Page } from 'puppeteer';
import type { ExtractResult } from './shared';
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
} from './shared';
import Redis from 'ioredis';

// ---- metrics (worker) ----
import http from 'node:http';
import client from 'prom-client';
const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });
const pagesProcessed = new client.Counter({
  name: 'crawler_pages_processed_total',
  help: 'Total pages successfully processed',
  labelNames: ['mode', 'queue'], // mode=ai|css, queue=initial|ai-revisit
});
const pagesFailed = new client.Counter({
  name: 'crawler_pages_failed_total',
  help: 'Total pages failed',
  labelNames: ['reason'], // navigate|exception
});
const linksQueued = new client.Counter({
  name: 'crawler_links_enqueued_total',
  help: 'Links enqueued (same-origin)',
});
const queueDepth = new client.Gauge({
  name: 'crawler_queue_depth',
  help: 'Frontier queue depth',
  labelNames: ['queue'], // crawl|ai_revisit
});
registry.registerMetric(pagesProcessed);
registry.registerMetric(pagesFailed);
registry.registerMetric(linksQueued);
registry.registerMetric(queueDepth);

function startMetricsServer(port = Number(process.env.METRICS_PORT || 9100)) {
  const server = http.createServer(async (_req, res) => {
    if (_req.url === '/metrics') {
      const body = await registry.metrics();
      res.writeHead(200, { 'Content-Type': registry.contentType });
      return res.end(body);
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
  });
  server.listen(port, () => console.log(`[metrics] worker listening on :${port}`));
}

// ---- queues/sets ----
const QUEUE_KEY        = process.env.QUEUE_KEY        || 'crawl:queue';
const REVISIT_KEY      = process.env.REVISIT_KEY      || 'ai:revisit';
const RESULTS_DB_KEY   = process.env.RESULTS_DB_KEY   || 'crawl:results_db';
const RESULTS_RAW_KEY  = process.env.RESULTS_RAW_KEY  || 'crawl:results_raw';
const SEEN_KEY         = process.env.SEEN_KEY         || 'crawl:seen';
const INFLIGHT_KEY     = process.env.INFLIGHT_KEY     || 'crawl:inflight';
const DLQ_KEY          = process.env.DLQ_KEY          || 'crawl:dlq';

// ---- behavior ----
const MAX_DEPTH         = Number(process.env.MAX_DEPTH || '3');
const INCLUDE_SELECTORS = (process.env.INCLUDE_SELECTORS || '').trim();
const AI_MODE           = (process.env.AI_MODE || 'false').toLowerCase() === 'true';

// ---- wire up ----
const log = makeLogger('worker');
const redis: Redis = createRedis(process.env.REDIS_URL || env.REDIS_URL);

(async () => {
  const browser = await launchBrowser();
  await setConsentCookieIfConfigured(browser, log);
  startMetricsServer(Number(process.env.METRICS_PORT || 9100));

  log.info(`Up. HEADLESS=${env.HEADLESS} AI_MODE=${AI_MODE} SELECTORS="${INCLUDE_SELECTORS || '(empty)'}"`);

  while (true) {
    try {
      // prefer AI revisit
      let popped = await redis.brpop(REVISIT_KEY, 1);
      if (popped) log.info(`[AI-REVISIT] ${popped[1]}`);
      else {
        popped = await redis.brpop(QUEUE_KEY, 1);
        if (popped) log.info(`[INITIAL] ${popped[1]}`);
      }
      if (!popped) continue;

      // update gauges (best-effort)
      try {
        const [qMain, qAi] = await Promise.all([redis.llen(QUEUE_KEY), redis.llen(REVISIT_KEY)]);
        queueDepth.set({ queue: 'crawl' }, qMain);
        queueDepth.set({ queue: 'ai_revisit' }, qAi);
      } catch {}

      const task = JSON.parse(popped[1]) as { url: string; depth: number };
      const sourceQueue = popped[0]; // 'ai:revisit' or 'crawl:queue'
      const { url, depth } = task;
      if (!url || depth > MAX_DEPTH) continue;

      // in-flight dedupe; mark seen only after success
      const inflightAdded = await redis.sadd(INFLIGHT_KEY, url);
      if (inflightAdded === 0) { log.info(`[SKIP] already inflight: ${url}`); continue; }
      const alreadySeen = await redis.sismember(SEEN_KEY, url);
      if (alreadySeen === 1) { log.info(`[SKIP] already seen: ${url}`); await redis.srem(INFLIGHT_KEY, url); continue; }

      log.info(`[NAV] -> ${url} (depth=${depth})`);

      const ctx = await browser.createBrowserContext();
      const page: Page = await ctx.newPage();
      try {
        await configurePage(page);

        const ok = await navigateWithRetries(page, url);
        if (!ok) {
          log.warn(`[NAV FAIL] ${url}`);
          pagesFailed.inc({ reason: 'navigate' });
          await redis.lpush(DLQ_KEY, JSON.stringify({ url, depth, reason: 'navigate' }));
          await redis.srem(INFLIGHT_KEY, url);
          await ctx.close().catch(() => {});
          continue;
        }

        log.info(`[NAV OK] ${url}`);

        // extract
        const selectorMode = (AI_MODE || !INCLUDE_SELECTORS) ? 'ai' : 'css';
        const data: ExtractResult = selectorMode === 'ai'
          ? await extractAiMode(page)
          : await extractSelective(page, INCLUDE_SELECTORS);

        log.info(`[EXTRACT ${selectorMode.toUpperCase()}] title="${data.title}" anchors=${(data.anchors || []).length}`);

        // enqueue links
        const anchors = sameOriginOnly(data.anchors || [], page.url());
        const nextDepth = depth + 1;
        if (nextDepth <= MAX_DEPTH && anchors.length) {
          for (const link of anchors) {
            const wasNew = await redis.sismember(SEEN_KEY, link) === 0;
            if (wasNew) await redis.lpush(QUEUE_KEY, JSON.stringify({ url: link, depth: nextDepth }));
          }
          linksQueued.inc(anchors.length);
        }

        // publish
        const payload = {
          sitesUrl: url,
          title: data.title ?? null,
          body: data.body ?? null,
          html: data.html ?? '',
          meta: {
            fetchedAt: new Date().toISOString(),
            domain: new URL(url).hostname,
            selector_mode: selectorMode,
            source_queue: sourceQueue,
          },
        };

        await redis.lpush(RESULTS_DB_KEY, JSON.stringify(payload));
        await redis.lpush(RESULTS_RAW_KEY, JSON.stringify(payload));

        // success -> mark seen & clear inflight
        await redis.sadd(SEEN_KEY, url);
        await redis.srem(INFLIGHT_KEY, url);

        pagesProcessed.inc({ mode: selectorMode, queue: sourceQueue === REVISIT_KEY ? 'ai-revisit' : 'initial' });
      } catch (err: any) {
        pagesFailed.inc({ reason: 'exception' });
        log.error(`Page error: ${err?.message || err}`);
        await redis.lpush(DLQ_KEY, JSON.stringify({ url, depth, reason: 'exception', message: err?.message || String(err) }));
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
