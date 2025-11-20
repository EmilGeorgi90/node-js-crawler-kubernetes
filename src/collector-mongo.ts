import { MongoClient, ServerApiVersion } from "mongodb";
import Redis from "ioredis";

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";
const RESULTS_KEY = process.env.RESULTS_DB_KEY || "crawl:results_db";

const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017";
const DB_NAME = process.env.DB_NAME || "crawler";
const COL_PAGES = process.env.COL_PAGES || "pages";
const COL_PRODUCTS = process.env.COL_PRODUCTS || "products";
const PRODUCT_URL_REGEX = new RegExp(
  process.env.PRODUCT_URL_REGEX ?? String.raw`\/[^\/]+\/(\d+)(?:\?.*)?$`,
  "i"
);

(async () => {
  const redis = new Redis(REDIS_URL);
  const mongo = new MongoClient(MONGO_URL, { serverApi: ServerApiVersion.v1 });
  await mongo.connect();
  const db = mongo.db(DB_NAME);
  const pages = db.collection(COL_PAGES);
  const products = db.collection(COL_PRODUCTS);

  await pages.createIndex({ url: 1 }, { unique: true });
  await pages.createIndex({ detectedType: 1, "meta.fetchedAt": -1 });
  await products.createIndex({ productId: 1 }, { sparse: true });
  await products.createIndex({ slug: 1 }, { sparse: true });

  while (true) {
    const res = await redis.brpop(RESULTS_KEY, 5);
    if (!res) continue;
    const doc = JSON.parse(res[1]) as any;
    const url: string = doc.sitesUrl;
    const now = new Date(doc?.meta?.fetchedAt ?? Date.now());

    const m = PRODUCT_URL_REGEX.exec(url);
    const isProduct = !!m;
    const productId = m?.[1] ?? null;

    const pageDoc = {
      url,
      title: doc.title ?? null,
      body: doc.body ?? null,
      detectedType: isProduct ? "product" : "page",
      meta: { ...(doc.meta || {}), fetchedAt: now },
    };

    await pages.updateOne({ url }, { $set: pageDoc }, { upsert: true });

    if (isProduct) {
      const parts = url.split("?")[0].split("/").filter(Boolean);
      const slug = parts.at(-2) ?? null;
      await products.updateOne(
        { url },
        {
          $set: {
            url,
            slug,
            productId,
            title: doc.title ?? null,
            fetchedAt: now,
          },
        },
        { upsert: true }
      );
    }
  }
})();
