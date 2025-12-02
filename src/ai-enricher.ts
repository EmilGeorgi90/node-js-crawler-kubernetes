import { MongoClient, ServerApiVersion } from "mongodb";
import { Redis } from "ioredis";
import http from "node:http";
import client from "prom-client";

const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });
const aiLabeled = new client.Counter({
  name: "ai_enricher_labeled_total",
  help: "Docs labeled",
});
const aiFailed = new client.Counter({
  name: "ai_enricher_requests_failed_total",
  help: "AI request fails",
});
const aiRevisitQueued = new client.Counter({
  name: "ai_enricher_revisit_enqueued_total",
  help: "Revisit tasks queued",
});
const aiLatency = new client.Histogram({
  name: "ai_enricher_infer_duration_seconds",
  help: "AI RTT",
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
});

function startMetricsServer(port = Number(process.env.METRICS_PORT || 9102)) {
  const srv = http.createServer(async (_req, res) => {
    if (_req.url === "/metrics") {
      const b = await registry.metrics();
      res.writeHead(200, { "Content-Type": registry.contentType });
      return res.end(b);
    }
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
  });
  srv.listen(port, () => console.log(`[metrics] ai-enricher on :${port}`));
}

const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
const AI_URL = process.env.AI_URL || "http://ai:8001/detect";
const MONGO_URL = process.env.MONGO_URL || "mongodb://mongo:27017";
const DB_NAME = process.env.DB_NAME || "crawler";
const COL_PAGES = process.env.COL_PAGES || "pages";

const REVISIT_KEY = process.env.REVISIT_KEY || "ai:revisit";
const REVISIT_PENDING_KEY =
  process.env.REVISIT_PENDING_KEY || "ai:revisit:pending";
const SEEN_KEY = process.env.SEEN_KEY || "crawl:seen";

const INCLUDE_SELECTORS = (process.env.INCLUDE_SELECTORS || "").trim();
const ENABLE_REVISIT =
  (process.env.ENABLE_REVISIT ?? "true").toLowerCase() !== "false";

const BATCH = Math.max(1, Math.min(16, Number(process.env.AI_BATCH || "12")));
const MIN_CONF = Number(process.env.MIN_CONF || "0.65");
const TEXT_LIMIT = Math.max(
  500,
  Math.min(8000, Number(process.env.AI_TEXT_LIMIT || "4000"))
);

function flattenBody(b: any): string {
  let out = "";
  const visit = (n: any) => {
    if (!n) return;
    if (typeof n.text === "string") out += " " + n.text;
    if (Array.isArray(n.children)) n.children.forEach(visit);
  };
  if (b?.children) b.children.forEach(visit);
  return out.trim().replace(/\s+/g, " ");
}
function sanitizeDoc(d: {
  url: string;
  title?: string;
  text?: string;
  html?: string;
}) {
  const url = String(d.url || "").slice(0, 2048);
  const title = String(d.title || "").slice(0, 512);
  const text = String(d.text || "").slice(0, TEXT_LIMIT);
  return { url, title, text, html: "" };
}
async function postJSON(url: string, body: any) {
  const controller = new AbortController();
  const t = setTimeout(
    () => controller.abort(),
    Number(process.env.AI_TIMEOUT_MS || 15000)
  );
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    const text = await resp.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch {}
    return { ok: resp.ok, status: resp.status, json, text };
  } finally {
    clearTimeout(t);
  }
}

(async () => {
  startMetricsServer();

  const redis = new Redis(REDIS_URL);
  const mongo = new MongoClient(MONGO_URL, { serverApi: ServerApiVersion.v1 });
  await mongo.connect();
  const pages = mongo.db(DB_NAME).collection(COL_PAGES);
  console.log("ai-enricher up (AI_URL=", AI_URL, ")");

  while (true) {
    try {
      // do NOT pull already-queued docs
      const batch = await pages
        .find(
          {
            $and: [
              {
                $or: [
                  { "labels.product.isProduct": { $exists: false } },
                  { "labels.product.confidence": { $lt: MIN_CONF } },
                ],
              },
              { "labels.product.aiQueued": { $ne: true } },
            ],
          },
          {
            projection: {
              url: 1,
              title: 1,
              body: 1,
              "labels.product.aiQueued": 1,
            },
          }
        )
        .limit(BATCH)
        .toArray();

      if (batch.length === 0) {
        await new Promise((r) => setTimeout(r, 2000));
        continue;
      }

      const docs = batch.map((b) =>
        sanitizeDoc({
          url: b.url,
          title: b.title || "",
          text: flattenBody(b.body || null),
        })
      );

      let results: any[] | null = null;
      let lastErr: string | null = null;
      for (let attempt = 0; attempt < 3; attempt++) {
        const stop = aiLatency.startTimer();
        const resp = await postJSON(AI_URL, { docs });
        stop();
        if (resp.ok && resp.json && Array.isArray(resp.json.results)) {
          results = resp.json.results;
          break;
        }
        aiFailed.inc();
        lastErr = `status=${resp.status} body=${(resp.text || "").slice(
          0,
          500
        )}`;
        console.error(`[AI ERROR] attempt=${attempt} ${lastErr}`);
        await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
      }
      if (!results) {
        console.error(`[AI GAVE UP] ${lastErr}`);
        continue;
      }

      for (const r of results) {
        if (!r || typeof r.url !== "string") continue;

        // 1) Update labels
        await pages.updateOne(
          { url: r.url },
          {
            $set: {
              "labels.product": {
                isProduct: !!r.isProduct,
                confidence: Number.isFinite(r.confidence) ? r.confidence : 0,
                fields: r.fields || null,
                updatedAt: new Date(),
              },
            },
          }
        );
        aiLabeled.inc();

        // 2) One-time enqueue logic (guarded)
        if (
          ENABLE_REVISIT &&
          !INCLUDE_SELECTORS &&
          Number(r.confidence) >= MIN_CONF
        ) {
          const [seen, pending] = await Promise.all([
            redis.sismember(SEEN_KEY, r.url),
            redis.sismember(REVISIT_PENDING_KEY, r.url),
          ]);
          if (seen === 0 && pending === 0) {
            const added = await redis.sadd(REVISIT_PENDING_KEY, r.url);
            if (added === 1) {
              await redis.rpush(
                REVISIT_KEY,
                JSON.stringify({ url: r.url, depth: 0 })
              );
              aiRevisitQueued.inc();
              // mark in Mongo so we don't re-enqueue again
              await pages.updateOne(
                { url: r.url },
                {
                  $set: {
                    "labels.product.aiQueued": true,
                    "labels.product.aiQueuedAt": new Date(),
                  },
                }
              );
            }
          }
        }
      }
      console.log(`AI-labeled: ${results.length}`);
    } catch (e: any) {
      aiFailed.inc();
      console.error("ai-enricher loop error:", e?.message || e);
      await new Promise((r) => setTimeout(r, 1500));
    }
  }
})();
