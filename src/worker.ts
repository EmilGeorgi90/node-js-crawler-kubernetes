import type { Page } from 'puppeteer';
import client from 'prom-client';
import { cfg } from './config.js';
import { KafkaBus, Task } from './queue-kafka.js';
import { makeLogger, launchBrowser, configurePage, navigateWithRetries, fetchStatic, cheerioExtract, sameOriginOnly, allowedByRobots, startMetricsServer } from './shared.js';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import taskSchema from '../schemas/task.crawl.v1.json' assert { type: 'json' };
import resultSchema from '../schemas/result.page.v1.json' assert { type: 'json' };
import { seenUrlOnce, seenContentOnce } from './dedupe.js';

const log = makeLogger('worker');
const registry = new client.Registry();
const pagesProcessed = new client.Counter({ name: 'crawler_pages_processed_total', help: 'Total pages processed', labelNames: ['mode','queue'], registers: [registry] });
const pagesFailed = new client.Counter({ name: 'crawler_pages_failed_total', help: 'Failures', labelNames: ['reason'], registers: [registry] });
const linksQueued = new client.Counter({ name: 'crawler_links_enqueued_total', help: 'Links enqueued', registers: [registry] });
const activeByOrigin = new client.Gauge({ name: 'crawler_inflight_by_origin', help: 'In-flight per origin', labelNames: ['origin'], registers: [registry] });
const concurrencyGauge = new client.Gauge({ name: 'crawler_worker_concurrency', help: 'Current tabs per worker', registers: [registry] });
startMetricsServer(registry, cfg.METRICS_PORT);

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validateTask = ajv.compile(taskSchema as any);
const validateResult = ajv.compile(resultSchema as any);

const maxPerOrigin = 4;
const inFlight = new Map<string, number>();
function acquire(origin: string): boolean {
  const n = inFlight.get(origin) || 0;
  if (n >= maxPerOrigin) return false;
  inFlight.set(origin, n+1); activeByOrigin.set({ origin }, n+1); return true;
}
function release(origin: string) {
  const n = inFlight.get(origin) || 1; const v = Math.max(0, n-1); inFlight.set(origin, v); activeByOrigin.set({ origin }, v);
}

let maxTabs = 4; concurrencyGauge.set(maxTabs);
setInterval(() => { /* hook Kafka lag here if desired */ }, 5000);

(async () => {
  const bus = new KafkaBus();
  await bus.start();
  const browser = await launchBrowser();

  const consumeTopics = [cfg.TOPIC_PRIORITY, cfg.TOPIC_REVISIT, cfg.TOPIC_INITIAL, cfg.TOPIC_RETRY_30S, cfg.TOPIC_RETRY_5M];

  await bus.runConsumer({
    topics: consumeTopics,
    groupId: cfg.GROUP_WORKER,
    concurrency: 4,
    handler: async (buf) => {
      let task: Task; try { task = JSON.parse(buf.toString()); } catch { pagesFailed.inc({ reason: 'json' }); return 'dlq'; }
      if (!validateTask(task)) { pagesFailed.inc({ reason: 'schema' }); return 'dlq'; }
      const { url, depth } = task; if (depth > cfg.MAX_DEPTH) return 'ok';
      if (!(await allowedByRobots(url))) return 'ok';

      const origin = new URL(url).origin; if (!acquire(origin)) return 'retry';
      try {
        if (seenUrlOnce(url)) return 'ok';
        log.info(`[NAV] depth=${depth} ${url}`);

        let title: string | null = null; let text = ''; let html = ''; let body: any = null; let anchors: string[] = []; let contentHash = ''; let simhash = '';
        let selectorMode = cfg.INCLUDE_SELECTORS ? 'css' : 'ai';

        try {
          if (cfg.STATIC_FIRST) {
            try {
              const htmlStatic = await fetchStatic(url);
              const ex = cheerioExtract(htmlStatic, url, cfg.INCLUDE_SELECTORS);
              ({ title, body, html, anchors, contentHash, simhash } = ex);
            } catch {
              const ctx = await browser.createBrowserContext();
              const page: Page = await ctx.newPage();
              try {
                await configurePage(page);
                const ok = await navigateWithRetries(page, url);
                if (!ok) throw new Error('navigate failed');
                title = (await page.title().catch(()=>null)) ?? null;
                const t = await page.$eval('body', el => (el as HTMLElement).innerText).catch(()=> '');
                text = t.replace(/\s+/g,' ').trim();
                body = text ? { children: [{ text }] } : null;
                html = cfg.INCLUDE_SELECTORS ? await page.$eval('html', el => (el as HTMLElement).outerHTML).catch(()=> '') : '';
                anchors = await page.$$eval('a[href]', as => [...new Set(as.map(a => (a as HTMLAnchorElement).href))]).catch(()=>[]);
                const crypto = await import('node:crypto');
                contentHash = crypto.createHash('sha256').update(text || '').digest('hex');
                simhash = crypto.createHash('sha1').update((text||'').slice(0,5000)).digest('hex').slice(0,16);
              } finally { await page.close().catch(()=>{}); await ctx.close().catch(()=>{}); }
            }
          } else {
            const ctx = await browser.createBrowserContext();
            const page: Page = await ctx.newPage();
            try {
              await configurePage(page);
              const ok = await navigateWithRetries(page, url);
              if (!ok) throw new Error('navigate failed');
              title = (await page.title().catch(()=>null)) ?? null;
              const t = await page.$eval('body', el => (el as HTMLElement).innerText).catch(()=> '');
              text = t.replace(/\s+/g,' ').trim();
              body = text ? { children: [{ text }] } : null;
              html = cfg.INCLUDE_SELECTORS ? await page.$eval('html', el => (el as HTMLElement).outerHTML).catch(()=> '') : '';
              anchors = await page.$$eval('a[href]', as => [...new Set(as.map(a => (a as HTMLAnchorElement).href))]).catch(()=>[]);
              const crypto = await import('node:crypto');
              contentHash = crypto.createHash('sha256').update(text || '').digest('hex');
              simhash = crypto.createHash('sha1').update((text||'').slice(0,5000)).digest('hex').slice(0,16);
            } finally { await page.close().catch(()=>{}); await ctx.close().catch(()=>{}); }
          }
        } catch (e) { pagesFailed.inc({ reason: 'extract' }); return 'retry'; }

        if (seenContentOnce(contentHash)) return 'ok';

        const nextDepth = depth + 1;
        if (nextDepth <= cfg.MAX_DEPTH && anchors.length) {
          const filtered = sameOriginOnly(anchors, url);
          for (const link of filtered) await bus.enqueue(cfg.TOPIC_INITIAL, { url: link, depth: nextDepth });
          if (filtered.length) linksQueued.inc(filtered.length);
        }

        const payload = { sitesUrl: url, title: title ?? null, body: body ?? null, html: cfg.INCLUDE_SELECTORS ? html || '' : '', meta: { fetchedAt: new Date().toISOString(), domain: new URL(url).hostname, selector_mode: selectorMode, contentHash, simhash } };
        if (!validateResult(payload)) { pagesFailed.inc({ reason: 'result_schema' }); return 'dlq'; }

        await bus.publish(cfg.TOPIC_RESULTS_DB, payload, url);
        await bus.publish(cfg.TOPIC_RESULTS_RAW, payload, url);
        pagesProcessed.inc({ mode: selectorMode, queue: 'kafka' });
        return 'ok';
      } finally { release(origin); }
    }
  });
})();