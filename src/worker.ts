import type { Page } from "puppeteer";
import client from "prom-client";
import { appConfig } from "./config.js";
import { KafkaBus, Task } from "./queue-kafka.js";
import {
  makeLogger,
  launchBrowser,
  configurePage,
  navigatePageWithRetries,
  fetchStaticHtml,
  extractTextAndLinksWithCheerio,
  filterSameOriginLinks,
  isAllowedByRobotsTxt,
  startMetricsServer,
} from "./shared.js";
import Ajv from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import taskSchema from "../schemas/task.crawl.v1.json" with { type: "json" };
import resultSchema from "../schemas/result.page.v1.json" with { type: "json" };
import { seenUrlOnce, seenContentOnce } from "./dedupe.js";

const logger = makeLogger("worker");
const metricsRegistry = new client.Registry();
const pagesProcessedCounter = new client.Counter({
  name: "crawler_pages_processed_total",
  help: "Total pages processed",
  labelNames: ["mode", "queue"],
  registers: [metricsRegistry],
});
const pagesFailedCounter = new client.Counter({
  name: "crawler_pages_failed_total",
  help: "Failures",
  labelNames: ["reason"],
  registers: [metricsRegistry],
});
const linksQueuedCounter = new client.Counter({
  name: "crawler_links_enqueued_total",
  help: "Links enqueued",
  registers: [metricsRegistry],
});
const inflightByOriginGauge = new client.Gauge({
  name: "crawler_inflight_by_origin",
  help: "In-flight per origin",
  labelNames: ["origin"],
  registers: [metricsRegistry],
});
const workerConcurrencyGauge = new client.Gauge({
  name: "crawler_worker_concurrency",
  help: "Current tabs per worker",
  registers: [metricsRegistry],
});
startMetricsServer(metricsRegistry, appConfig.METRICS_PORT);

const jsonSchemaValidator = new Ajv({ allErrors: true, strict: false });
addFormats(jsonSchemaValidator);
const validateCrawlTaskSchema = jsonSchemaValidator.compile(taskSchema as any);
const validateCrawlResultSchema = jsonSchemaValidator.compile(resultSchema as any);

const maxPerOrigin = 4;
const inflightByOrigin = new Map<string, number>();
function acquireOriginSlot(origin: string): boolean {
  const n = inflightByOrigin.get(origin) || 0;
  if (n >= maxPerOrigin) return false;
  inflightByOrigin.set(origin, n + 1);
  inflightByOriginGauge.set({ origin }, n + 1);
  return true;
}
function releaseOriginSlot(origin: string) {
  const n = inflightByOrigin.get(origin) || 1;
  const v = Math.max(0, n - 1);
  inflightByOrigin.set(origin, v);
  inflightByOriginGauge.set({ origin }, v);
}

let maxBrowserTabs = 4;
workerConcurrencyGauge.set(maxBrowserTabs);
setInterval(() => {
  /* hook Kafka lag here */
}, 5000);

(async () => {
  const kafkaBus = new KafkaBus();
  await kafkaBus.start();
  const browser = await launchBrowser();

  const consumeTopics = [
    appConfig.TOPIC_PRIORITY,
    appConfig.TOPIC_REVISIT,
    appConfig.TOPIC_INITIAL,
    appConfig.TOPIC_RETRY_30S,
    appConfig.TOPIC_RETRY_5M,
  ];

  await kafkaBus.runConsumer({
    topics: consumeTopics,
    groupId: appConfig.GROUP_WORKER,
    concurrency: 4,
    handler: async (buf) => {
      let task: Task;
      try {
        task = JSON.parse(buf.toString());
      } catch {
        pagesFailedCounter.inc({ reason: "json" });
        return "dlq";
      }
      if (!validateCrawlTaskSchema(task)) {
        pagesFailedCounter.inc({ reason: "schema" });
        return "dlq";
      }
      const { url, depth } = task;
      if (depth > appConfig.MAX_DEPTH) return "ok";
      if (!(await isAllowedByRobotsTxt(url))) return "ok";

      const origin = new URL(url).origin;
      if (!acquireOriginSlot(origin)) return "retry";
      try {
        if (seenUrlOnce(url)) {
          logger.info("[SKIP] seen:", url);
          return "ok";
        }
        logger.info(`[NAV] depth=${depth} ${url}`);

        let title: string | null = null;
        let text = "";
        let html = "";
        let body: any = null;
        let anchors: string[] = [];
        let contentHash = "";
        let simhash = "";
        let selectorMode = appConfig.INCLUDE_SELECTORS ? "css" : "ai";

        try {
          if (appConfig.STATIC_FIRST) {
            try {
              const htmlStatic = await fetchStaticHtml(url);
              const extracted = extractTextAndLinksWithCheerio(htmlStatic, url, appConfig.INCLUDE_SELECTORS);
              ({ title, body, html, anchors, contentHash, simhash } = extracted);
            } catch {
              const ctx = await browser.createBrowserContext();
              const page: Page = await ctx.newPage();
              try {
                await configurePage(page);
                const ok = await navigatePageWithRetries(page, url);
                if (!ok) throw new Error("navigate failed");
                title = (await page.title().catch(() => null)) ?? null;
                const t = await page
                  .$eval("body", (el) => (el as HTMLElement).innerText)
                  .catch(() => "");
                text = t.replace(/\s+/g, " ").trim();
                body = text ? { children: [{ text }] } : null;
                html = appConfig.INCLUDE_SELECTORS
                  ? await page
                      .$eval("html", (el) => (el as HTMLElement).outerHTML)
                      .catch(() => "")
                  : "";
                anchors = await page
                  .$$eval("a[href]", (as) => [
                    ...new Set(as.map((a) => (a as HTMLAnchorElement).href)),
                  ])
                  .catch(() => []);
                const crypto = await import("node:crypto");
                contentHash = crypto
                  .createHash("sha256")
                  .update(text || "")
                  .digest("hex");
                simhash = crypto
                  .createHash("sha1")
                  .update((text || "").slice(0, 5000))
                  .digest("hex")
                  .slice(0, 16);
              } finally {
                await page.close().catch(() => {});
                await ctx.close().catch(() => {});
              }
            }
          } else {
            const ctx = await browser.createBrowserContext();
            const page: Page = await ctx.newPage();
            try {
              await configurePage(page);
              const ok = await navigatePageWithRetries(page, url);
              if (!ok) throw new Error("navigate failed");
              title = (await page.title().catch(() => null)) ?? null;
              const t = await page
                .$eval("body", (el) => (el as HTMLElement).innerText)
                .catch(() => "");
              text = t.replace(/\s+/g, " ").trim();
              body = text ? { children: [{ text }] } : null;
              html = appConfig.INCLUDE_SELECTORS
                ? await page
                    .$eval("html", (el) => (el as HTMLElement).outerHTML)
                    .catch(() => "")
                : "";
              anchors = await page
                .$$eval("a[href]", (as) => [
                  ...new Set(as.map((a) => (a as HTMLAnchorElement).href)),
                ])
                .catch(() => []);
              const crypto = await import("node:crypto");
              contentHash = crypto
                .createHash("sha256")
                .update(text || "")
                .digest("hex");
              simhash = crypto
                .createHash("sha1")
                .update((text || "").slice(0, 5000))
                .digest("hex")
                .slice(0, 16);
            } finally {
              await page.close().catch(() => {});
              await ctx.close().catch(() => {});
            }
          }
        } catch (e) {
          pagesFailedCounter.inc({ reason: "extract" });
          return "retry";
        }

        if (seenContentOnce(contentHash)) {
          logger.info("[SKIP] dup content:", url);
          return "ok";
        }

        const nextDepth = depth + 1;
        if (nextDepth <= appConfig.MAX_DEPTH && anchors.length) {
          const filtered = filterSameOriginLinks(anchors, url);
          for (const link of filtered)
            await kafkaBus.enqueue(appConfig.TOPIC_INITIAL, {
              url: link,
              depth: nextDepth,
            });
          if (filtered.length) linksQueuedCounter.inc(filtered.length);
        }

        const payload = {
          sitesUrl: url,
          title: title ?? null,
          body: body ?? null,
          html: appConfig.INCLUDE_SELECTORS ? html || "" : "",
          meta: {
            fetchedAt: new Date().toISOString(),
            domain: new URL(url).hostname,
            selector_mode: selectorMode,
            contentHash,
            simhash,
          },
        };
        if (!validateCrawlResultSchema(payload)) {
          pagesFailedCounter.inc({ reason: "result_schema" });
          return "dlq";
        }

        await kafkaBus.publish(appConfig.TOPIC_RESULTS_DB, payload, url);
        await kafkaBus.publish(appConfig.TOPIC_RESULTS_RAW, payload, url);
        pagesProcessedCounter.inc({ mode: selectorMode, queue: "kafka" });
        return "ok";
      } finally {
        releaseOriginSlot(origin);
      }
    },
  });
})();
