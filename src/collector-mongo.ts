import { MongoClient, ServerApiVersion } from 'mongodb';
import client from 'prom-client';
import { KafkaBus } from './queue-kafka.js';
import { cfg } from './config.js';
import { startMetricsServer } from './shared.js';

const registry = new client.Registry();
const inserted = new client.Counter({ name: 'collector_mongo_inserted_total', help: 'docs upserted', registers: [registry] });
startMetricsServer(registry, Number(process.env.METRICS_PORT || '9101'));

(async () => {
  const mongo = new MongoClient(process.env.MONGO_URL || 'mongodb://mongo:27017', { serverApi: ServerApiVersion.v1 });
  await mongo.connect();
  const db = mongo.db(process.env.DB_NAME || 'crawler');
  const pages = db.collection(process.env.COL_PAGES || 'pages');
  const products = db.collection(process.env.COL_PRODUCTS || 'products');
  await pages.createIndex({ url: 1 }, { unique: true });
  await pages.createIndex({ 'meta.contentHash': 1 }, { unique: false });
  await pages.createIndex({ detectedType: 1, 'meta.fetchedAt': -1 });
  await products.createIndex({ productId: 1 }, { sparse: true });
  await products.createIndex({ slug: 1 }, { sparse: true });

  const bus = new KafkaBus();
  await bus.start();

  await bus.runConsumer({
    topics: [cfg.TOPIC_ENRICHED],
    groupId: cfg.GROUP_DB,
    concurrency: 3,
    handler: async (buf) => {
      const doc = JSON.parse(buf.toString());
      const url: string = doc.sitesUrl;
      const now = new Date(doc?.meta?.fetchedAt || Date.now());
      const pageDoc = { url, title: doc.title ?? null, body: doc.body ?? null, detectedType: doc.detectedType || 'page', meta: { ...(doc.meta||{}), fetchedAt: now } };
      await pages.updateOne({ url }, { $set: pageDoc }, { upsert: true });
      inserted.inc();
      const m = /\/[^/]+\/(\d+)(?:\?.*)?$/.exec(url);
      if (m) {
        const productId = m[1];
        const parts = url.split('?')[0].split('/').filter(Boolean);
        const slug = parts.at(-2) ?? null;
        await products.updateOne(
          { url },
          { $set: { url, slug, productId, title: doc.title ?? null, fetchedAt: now } },
          { upsert: true }
        );
      }
      return 'ok';
    }
  });
})();