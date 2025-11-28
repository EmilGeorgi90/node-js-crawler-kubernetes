import { MongoClient, ServerApiVersion } from 'mongodb';
import Redis from 'ioredis';

// ---- metrics ----
import http from 'node:http';
import client from 'prom-client';
const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });
const docsIngested = new client.Counter({
  name: 'collector_mongo_docs_ingested_total',
  help: 'Total documents ingested into Mongo',
});
const productsUpserted = new client.Counter({
  name: 'collector_mongo_products_upserted_total',
  help: 'Total product docs upserted',
});
const writeFailures = new client.Counter({
  name: 'collector_mongo_write_failures_total',
  help: 'Mongo write failures',
});
const backlogGauge = new client.Gauge({
  name: 'collector_mongo_backlog_depth',
  help: 'Redis backlog depth for crawl:results_db',
});
const writeDuration = new client.Histogram({
  name: 'collector_mongo_write_duration_seconds',
  help: 'Mongo write duration',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2],
});
registry.registerMetric(docsIngested);
registry.registerMetric(productsUpserted);
registry.registerMetric(writeFailures);
registry.registerMetric(backlogGauge);
registry.registerMetric(writeDuration);

function startMetricsServer(port = Number(process.env.METRICS_PORT || 9101)) {
  const srv = http.createServer(async (_req, res) => {
    if (_req.url === '/metrics') {
      const body = await registry.metrics();
      res.writeHead(200, { 'Content-Type': registry.contentType });
      return res.end(body);
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
  });
  srv.listen(port, () => console.log(`[metrics] collector-mongo on :${port}`));
}

// ---- env ----
const REDIS_URL   = process.env.REDIS_URL   || 'redis://redis:6379';
const RESULTS_KEY = process.env.RESULTS_DB_KEY || 'crawl:results_db';
const MONGO_URL   = process.env.MONGO_URL   || 'mongodb://mongo:27017';
const DB_NAME     = process.env.DB_NAME     || 'crawler';
const COL_PAGES   = process.env.COL_PAGES   || 'pages';
const COL_PRODUCTS= process.env.COL_PRODUCTS|| 'products';
const PRODUCT_URL_REGEX = new RegExp(process.env.PRODUCT_URL_REGEX ?? String.raw`\/[^\/]+\/(\d+)(?:\?.*)?$`, 'i');

(async () => {
  startMetricsServer();

  const redis = new Redis(REDIS_URL);
  const mongo = new MongoClient(MONGO_URL, { serverApi: ServerApiVersion.v1 });
  await mongo.connect();
  const db = mongo.db(DB_NAME);
  const pages = db.collection(COL_PAGES);
  const products = db.collection(COL_PRODUCTS);

  await pages.createIndex({ url: 1 }, { unique: true });
  await pages.createIndex({ detectedType: 1, 'meta.fetchedAt': -1 });
  await products.createIndex({ productId: 1 }, { sparse: true });
  await products.createIndex({ slug: 1 }, { sparse: true });

  console.log('collector-mongo up');
  while (true) {
    try {
      // backlog gauge (best-effort)
      try { backlogGauge.set(await redis.llen(RESULTS_KEY)); } catch {}

      const res = await redis.brpop(RESULTS_KEY, 5);
      if (!res) continue;

      const doc = JSON.parse(res[1]) as any;
      const url: string = doc.sitesUrl;
      const now = new Date(doc?.meta?.fetchedAt ?? Date.now());

      const m = PRODUCT_URL_REGEX.exec(url);
      const isProduct = !!m; const productId = m?.[1] ?? null;

      const pageDoc = {
        url,
        title: doc.title ?? null,
        body: doc.body ?? null,
        detectedType: isProduct ? 'product' : 'page',
        meta: { ...(doc.meta||{}), fetchedAt: now },
      };

      const end = writeDuration.startTimer();
      await pages.updateOne({ url }, { $set: pageDoc }, { upsert: true });
      if (isProduct) {
        const parts = url.split('?')[0].split('/').filter(Boolean);
        const slug = parts.at(-2) ?? null;
        await products.updateOne(
          { url },
          { $set: { url, slug, productId, title: doc.title ?? null, fetchedAt: now } },
          { upsert: true }
        );
        productsUpserted.inc();
      }
      end();
      docsIngested.inc();
    } catch (e) {
      writeFailures.inc();
      console.error('collector-mongo error:', (e as any)?.message || e);
    }
  }
})();
