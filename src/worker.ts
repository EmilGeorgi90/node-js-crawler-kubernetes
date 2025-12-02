// src/worker.ts
import type { Page } from "puppeteer";
import http from "node:http";
import client from "prom-client";

import {
  makeLogger,
  createRedis,
  launchBrowser,
  configurePage,
  navigateWithRetries,
  setConsentCookieIfConfigured,
  sameOriginOnly,
  fetchStatic,
  cheerioExtract,
  extractSelectivePptr,
  allowedByRobots,
} from "./shared.js";

import { cfg } from "./config.js";

// --------------------------- Metrics ---------------------------
const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

const pagesProcessed = new client.Counter({
  name: "crawler_pages_processed_total",
  help: "Total pages processed",
  labelNames: ["mode", "queue"],
  registers: [registry],
});

const pagesFailed = new client.Counter({
  name: "crawler_pages_failed_total",
  help: "Total pages failed",
  labelNames: ["reason"],
  registers: [registry],
});

const linksQueued = new client.Counter({
  name: "crawler_links_enqueued_total",
  help: "Links enqueued",
  registers: [registry],
});

const queueLag = new client.Gauge({
  name: "crawler_stream_lag",
  help: "Pending messages (approx) in streams",
  labelNames: ["stream"],
  registers: [registry],
});

function startMetricsServer(
  port = Number(process.env.METRICS_PORT || cfg.METRICS_PORT)
) {
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

// --------------------------- Worker ---------------------------
type Task = { url: string; depth: number };

const log = makeLogger("worker");
const redis = createRedis(cfg.REDIS_URL);

async function ensureGroups() {
  if (cfg.FRONTIER_MODE === "stream") {
    const mk = async (stream: string, group: string) => {
      try {
        await redis.xgroup("CREATE", stream, group, "$", "MKSTREAM");
      } catch (e: any) {
        // BUSYGROUP means it exists already
        if (!String(e?.message || "").includes("BUSYGROUP")) throw e;
      }
    };
    await mk(cfg.QUEUE_STREAM, cfg.QUEUE_GROUP);
    await mk(cfg.REVISIT_STREAM, cfg.QUEUE_GROUP);
  }
}

async function enqueueStream(stream: string, t: Task) {
  await redis.xadd(stream, "*", "task", JSON.stringify(t));
}
async function enqueueList(listKey: string, t: Task) {
  await redis.lpush(listKey, JSON.stringify(t));
}

(async () => {
  await ensureGroups();

  const browser = await launchBrowser();
  await setConsentCookieIfConfigured(browser, log);
  startMetricsServer();

  log.info(
    `Up. FRONTIER=${cfg.FRONTIER_MODE} HEADLESS=${
      process.env.HEADLESS
    } AI_MODE=${process.env.AI_MODE} SELECTORS="${
      process.env.INCLUDE_SELECTORS || ""
    }"`
  );

  while (true) {
    try {
      // Pull a small batch from streams (AI revisit first) or lists
      let records: Array<{
        stream?: string;
        list?: string;
        id?: string;
        kv: [string, string];
      }> = [];

      if (cfg.FRONTIER_MODE === "stream") {
        const res = await redis.xreadgroup(
          "GROUP",
          cfg.QUEUE_GROUP,
          cfg.WORKER_CONSUMER,
          "COUNT",
          10,
          "BLOCK",
          2000,
          "STREAMS",
          cfg.REVISIT_STREAM,
          cfg.QUEUE_STREAM,
          ">",
          ">"
        );

        if (res && res.length) {
          // res = [ [stream, [ [id, [k,v,k,v...]], ... ]], ... ]
          for (const [stream, entries] of res as any[]) {
            for (const [id, kv] of entries) records.push({ stream, id, kv });
          }
        }

        // stream lag (best-effort)
        try {
          const infoA = (await redis.xpending(
            cfg.REVISIT_STREAM,
            cfg.QUEUE_GROUP
          )) as any;
          const infoB = (await redis.xpending(
            cfg.QUEUE_STREAM,
            cfg.QUEUE_GROUP
          )) as any;
          queueLag.set(
            { stream: "revisit" },
            typeof infoA === "object" ? infoA.count || 0 : 0
          );
          queueLag.set(
            { stream: "queue" },
            typeof infoB === "object" ? infoB.count || 0 : 0
          );
        } catch {}
      } else {
        // list-mode fallback
        const popped =
          (await redis.brpop(cfg.REVISIT_KEY, 1)) ||
          (await redis.brpop(cfg.QUEUE_KEY, 1));
        if (popped) {
          // popped = [key, value]
          records.push({ list: popped[0], kv: ["task", popped[1]] });
        }
      }

      if (!records.length) continue;

      for (const rec of records) {
        const raw = rec.kv[1];
        let task: Task;
        try {
          task = JSON.parse(raw);
        } catch {
          // bad record; ack and skip
          if (rec.stream && rec.id)
            await redis.xack(rec.stream, cfg.QUEUE_GROUP, rec.id);
          continue;
        }

        const { url, depth } = task;
        if (!url || typeof depth !== "number" || depth > cfg.MAX_DEPTH) {
          if (rec.stream && rec.id)
            await redis.xack(rec.stream, cfg.QUEUE_GROUP, rec.id);
          continue;
        }

        // Respect robots.txt
        if (!(await allowedByRobots(url))) {
          if (rec.stream && rec.id)
            await redis.xack(rec.stream, cfg.QUEUE_GROUP, rec.id);
          continue;
        }

        const sourceLabel = rec.stream
          ? rec.stream === cfg.REVISIT_STREAM
            ? "AI-REVISIT"
            : "INITIAL"
          : rec.list && String(rec.list).includes("ai:revisit")
          ? "AI-REVISIT"
          : "INITIAL";

        // ---- NAV log: starting this URL
        log.info(`[NAV] [${sourceLabel}] depth=${depth} ${url}`);

        const selectorMode =
          (process.env.AI_MODE || cfg.AI_MODE) === "true" ||
          !process.env.INCLUDE_SELECTORS
            ? "ai"
            : "css";

        // Extract
        let title: string | null = null;
        let text = "";
        let html = "";
        let body: any = null;
        let anchors: string[] = [];
        let contentHash = "";
        let simhash = "";

        try {
          let usePptr = false;

          // Static-first path
          if ((process.env.STATIC_FIRST || cfg.STATIC_FIRST) === "true") {
            try {
              const htmlStatic = await fetchStatic(url);
              const ex = cheerioExtract(
                htmlStatic,
                url,
                selectorMode === "css"
                  ? process.env.INCLUDE_SELECTORS || cfg.INCLUDE_SELECTORS
                  : ""
              );
              ({ title, body, html, text, anchors, contentHash, simhash } = ex);
            } catch {
              usePptr = true;
            }
          } else {
            usePptr = true;
          }

          if (usePptr) {
            // ---- NAV log: using Puppeteer fallback
            log.info(`[NAV:PPTR] depth=${depth} ${url}`);

            const ctx = await browser.createBrowserContext();
            const page: Page = await ctx.newPage();
            try {
              await configurePage(page);
              const ok = await navigateWithRetries(page, url);
              if (!ok) throw new Error("navigate failed");

              if (selectorMode === "css") {
                const ex = await extractSelectivePptr(
                  page,
                  process.env.INCLUDE_SELECTORS || cfg.INCLUDE_SELECTORS
                );
                ({ title, body, html, text, anchors, contentHash, simhash } =
                  ex);
              } else {
                const t = await page
                  .$eval("body", (el) => (el as HTMLElement).innerText)
                  .catch(() => "");
                text = (t || "").replace(/\s+/g, " ").trim();
                title = (await page.title().catch(() => null)) ?? null;
                body = text ? { children: [{ text }] } : null;
                anchors =
                  (await page
                    .$$eval("a[href]", (as) => [
                      ...new Set(as.map((a) => (a as HTMLAnchorElement).href)),
                    ])
                    .catch(() => [])) || [];
                // inline hashers to avoid cycles
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
              }
            } finally {
              await page.close().catch(() => {});
              await ctx.close().catch(() => {});
            }
          }
        } catch (err: any) {
          pagesFailed.inc({ reason: "extract" });
          // Ack and continue
          if (rec.stream && rec.id)
            await redis.xack(rec.stream, cfg.QUEUE_GROUP, rec.id);
          continue;
        }

        // Enqueue discovered same-origin links
        const nextDepth = depth + 1;
        if (nextDepth <= cfg.MAX_DEPTH && anchors.length) {
          const filtered = sameOriginOnly(anchors, url);
          for (const link of filtered) {
            if (cfg.FRONTIER_MODE === "stream") {
              await enqueueStream(cfg.QUEUE_STREAM, {
                url: link,
                depth: nextDepth,
              });
            } else {
              await enqueueList(cfg.QUEUE_KEY, { url: link, depth: nextDepth });
            }
          }
          if (filtered.length) linksQueued.inc(filtered.length);
        }

        // Publish result (both DB and RAW collectors)
        const payload = {
          sitesUrl: url,
          title: title ?? null,
          body: body ?? null,
          html: selectorMode === "css" ? html || "" : "",
          meta: {
            fetchedAt: new Date().toISOString(),
            domain: new URL(url).hostname,
            selector_mode: selectorMode,
            contentHash,
            simhash,
          },
        };

        await redis.lpush(cfg.RESULTS_DB_KEY, JSON.stringify(payload));
        await redis.lpush(cfg.RESULTS_RAW_KEY, JSON.stringify(payload));

        pagesProcessed.inc({
          mode: selectorMode,
          queue: rec.stream
            ? rec.stream === cfg.REVISIT_STREAM
              ? "ai-revisit"
              : "initial"
            : "list",
        });

        // Ack stream message
        if (rec.stream && rec.id)
          await redis.xack(rec.stream, cfg.QUEUE_GROUP, rec.id);
      }
    } catch (loopErr: any) {
      pagesFailed.inc({ reason: "loop" });
      log.error(`Loop error: ${loopErr?.message || loopErr}`);
    }
  }
})().catch((e) => {
  console.error("[worker] fatal", e);
  process.exit(1);
});
