import { MongoClient, ServerApiVersion } from "mongodb";
import { Client as PgClient } from "pg";

const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017";
const DB_NAME = process.env.DB_NAME || "crawler";
const COL_PAGES = process.env.COL_PAGES || "pages";

const PG_HOST = process.env.PG_HOST || "localhost";
const PG_PORT = Number(process.env.PG_PORT || "5432");
const PG_DB = process.env.PG_DB || "vector";
const PG_USER = process.env.PG_USER || "postgres";
const PG_PASS = process.env.PG_PASS || "postgres";

const EMBEDDER_URL = process.env.EMBEDDER_URL || "http://localhost:8000/embed"; // local FastAPI

// Helper: make a compact text from your page doc
function toText(b: any): string {
  const title = b.title ? String(b.title) : "";
  // pull text leaves from your {children} tree if present
  let bodyText = "";
  try {
    const collect = (node: any) => {
      if (!node) return;
      if (node.text) bodyText += " " + node.text;
      if (Array.isArray(node.children)) node.children.forEach(collect);
    };
    if (b.body?.children) b.body.children.forEach(collect);
  } catch {}
  const text = (title + " " + bodyText).trim().replace(/\s+/g, " ");
  return text.slice(0, 4000); // keep it short-ish
}

async function embedLocal(texts: string[]): Promise<number[][]> {
  const resp = await fetch(EMBEDDER_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ input: texts }),
  });
  if (!resp.ok)
    throw new Error(`Embedder ${resp.status}: ${await resp.text()}`);
  const json: any = await resp.json();
  return json.data as number[][];
}

(async () => {
  // Mongo
  const mongo = new MongoClient(MONGO_URL, { serverApi: ServerApiVersion.v1 });
  await mongo.connect();
  const db = mongo.db(DB_NAME);
  const pages = db.collection(COL_PAGES);

  // Postgres
  const pg = new PgClient({
    host: PG_HOST,
    port: PG_PORT,
    database: PG_DB,
    user: PG_USER,
    password: PG_PASS,
  });
  await pg.connect();

  console.log("Local embedder collector running…");

  while (true) {
    // Small batches; only those not embedded yet
    const batch = await pages
      .find({ "meta.isEmbedded": { $ne: true } })
      .project({ url: 1, title: 1, detectedType: 1, body: 1 })
      .limit(32)
      .toArray();

    if (batch.length === 0) {
      await new Promise((r) => setTimeout(r, 2000));
      continue;
    }

    const texts = batch.map(toText);
    const vecs = await embedLocal(texts);

    for (let i = 0; i < batch.length; i++) {
      const b = batch[i];
      const v = vecs[i];
      await pg.query(
        `INSERT INTO page_embeddings (url, title, detected_type, embedding, updated_at)
         VALUES ($1,$2,$3,$4, now())
         ON CONFLICT (url)
         DO UPDATE SET title=EXCLUDED.title,
                       detected_type=EXCLUDED.detected_type,
                       embedding=EXCLUDED.embedding,
                       updated_at=now()`,
        [b.url, b.title || null, b.detectedType || null, v]
      );
    }

    const ids = batch.map((b) => b._id);
    await pages.updateMany(
      { _id: { $in: ids } },
      { $set: { "meta.isEmbedded": true, "meta.embeddedAt": new Date() } }
    );

    console.log(`Embedded batch: ${batch.length}`);
  }
})();
