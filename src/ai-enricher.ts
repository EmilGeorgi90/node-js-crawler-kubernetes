import { MongoClient, ServerApiVersion } from 'mongodb';
import Redis from 'ioredis';

// ---- metrics ----
import http from 'node:http';
import client from 'prom-client';
const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });
const aiLabeled = new client.Counter({
  name: 'ai_enricher_labeled_total',
  help: 'Documents labeled by AI',
});
const aiFailed = new client.Counter({
  name: 'ai_enricher_requests_failed_total',
  help: 'AI requests failed',
});
const aiRevisitQueued = new client.Counter({
  name: 'ai_enricher_revisit_enqueued_total',
  help: 'AI revisit tasks enqueued',
});
const aiLatency = new client.Histogram({
  name: 'ai_enricher_infer_duration_seconds',
  help: 'AI inference round-trip time',
  buckets: [0.05,0.1,0.25,0.5,1,2,5,10],
});
registry.registerMetric(aiLabeled);
registry.registerMetric(aiFailed);
registry.registerMetric(aiRevisitQueued);
registry.registerMetric(aiLatency);

function startMetricsServer(port = Number(process.env.METRICS_PORT || 9102)) {
  const srv = http.createServer(async (_req, res) => {
    if (_req.url === '/metrics') {
      const body = await registry.metrics();
      res.writeHead(200, { 'Content-Type': registry.contentType });
      return res.end(body);
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
  });
  srv.listen(port, () => console.log(`[metrics] ai-enricher on :${port}`));
}

// ---- env ----
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const AI_URL = process.env.AI_URL || 'http://ai-detector:8001/detect';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://mongo:27017';
const DB_NAME = process.env.DB_NAME || 'crawler';
const COL_PAGES = process.env.COL_PAGES || 'pages';
const REVISIT_KEY = process.env.REVISIT_KEY || 'ai:revisit';
const INCLUDE_SELECTORS = (process.env.INCLUDE_SELECTORS || '').trim();
const ENABLE_REVISIT = (process.env.ENABLE_REVISIT ?? 'true').toLowerCase() !== 'false';
const BATCH = Number(process.env.AI_BATCH || '24');
const MIN_CONF = Number(process.env.MIN_CONF || '0.65');

function flattenBody(b: any): string {
  let out = '';
  const visit = (n: any) => { if (!n) return; if (typeof n.text === 'string') out += ' ' + n.text; if (Array.isArray(n.children)) n.children.forEach(visit); };
  if (b?.children) b.children.forEach(visit);
  return out.trim().replace(/\s+/g, ' ');
}

(async () => {
  startMetricsServer();

  const redis = new Redis(REDIS_URL);
  const mongo = new MongoClient(MONGO_URL, { serverApi: ServerApiVersion.v1 });
  await mongo.connect();
  const pages = mongo.db(DB_NAME).collection(COL_PAGES);
  console.log('ai-enricher up');

  while (true) {
    const batch = await pages.find({
      $or: [
        { 'labels.product.isProduct': { $exists: false } },
        { 'labels.product.confidence': { $lt: MIN_CONF } }
      ]
    }, { projection: { url: 1, title: 1, body: 1 } }).limit(BATCH).toArray();

    if (batch.length === 0) { await new Promise(r => setTimeout(r, 2000)); continue; }

    const docs = batch.map(b => ({ url: b.url, title: b.title || '', text: flattenBody(b.body || null), html: '' }));

    const stop = aiLatency.startTimer();
    const resp = await fetch(AI_URL, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ docs }) });
    stop();

    if (!resp.ok) {
      aiFailed.inc();
      console.error('AI error', resp.status, await resp.text());
      await new Promise(r => setTimeout(r, 2000));
      continue;
    }
    const { results } = await resp.json();

    for (const r of results) {
      await pages.updateOne({ url: r.url }, {
        $set: { 'labels.product': { isProduct: !!r.isProduct, confidence: r.confidence, fields: r.fields, updatedAt: new Date() } }
      });
      aiLabeled.inc();

      if (ENABLE_REVISIT && !INCLUDE_SELECTORS) {
        await redis.rpush(REVISIT_KEY, JSON.stringify({ url: r.url, depth: 0 }));
        aiRevisitQueued.inc();
      }
    }
    console.log(`AI-labeled: ${results.length}`);
  }
})();
