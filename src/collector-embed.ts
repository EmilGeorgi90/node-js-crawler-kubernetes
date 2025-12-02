import { MongoClient, ServerApiVersion } from "mongodb";
import { Client as PgClient } from "pg";
import http from "node:http";
import client from "prom-client";

const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });
const embeddedPages = new client.Counter({
  name: "collector_embed_pages_embedded_total",
  help: "Pages embedded -> pgvector",
});
const embedFailures = new client.Counter({
  name: "collector_embed_failures_total",
  help: "Embedding/writes failed",
});
const embedLatency = new client.Histogram({
  name: "collector_embed_duration_seconds",
  help: "Embedding+write duration",
  buckets: [0.02, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
});
function startMetricsServer(port = Number(process.env.METRICS_PORT || 9103)) {
  const srv = http.createServer(async (_req, res) => {
    if (_req.url === "/metrics") {
      const b = await registry.metrics();
      res.writeHead(200, { "Content-Type": registry.contentType });
      return res.end(b);
    }
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
  });
  srv.listen(port, () => console.log(`[metrics] collector-embed on :${port}`));
}

const MONGO_URL = process.env.MONGO_URL || "mongodb://mongo:27017";
const DB_NAME = process.env.DB_NAME || "crawler";
const COL_PAGES = process.env.COL_PAGES || "pages";

const PG_HOST = process.env.PG_HOST || "postgres";
const PG_PORT = Number(process.env.PG_PORT || "5432");
const PG_DB = process.env.PG_DB || "vector";
const PG_USER = process.env.PG_USER || "postgres";
const PG_PASS = process.env.PG_PASS || "postgres";

const EMBEDDER_URL = process.env.EMBEDDER_URL || ""; // optional external
const BATCH_SIZE = Number(process.env.EMBED_BATCH || "24");
const VECTOR_DIM = Math.max(
  1,
  Math.min(4096, Number(process.env.VECTOR_DIM || "384"))
);
const INLINE_VECTOR_SQL =
  (process.env.INLINE_VECTOR_SQL ?? "true").toLowerCase() !== "false";

function toText(b: any): string {
  const title = b?.title ? String(b.title) : "";
  let bodyText = "";
  try {
    const collect = (n: any) => {
      if (!n) return;
      if (typeof n.text === "string") bodyText += " " + n.text;
      if (Array.isArray(n.children)) n.children.forEach(collect);
    };
    if (b?.body?.children) b.body.children.forEach(collect);
  } catch {}
  return (title + " " + bodyText).trim().replace(/\s+/g, " ").slice(0, 4000);
}
async function embedLocal(texts: string[]) {
  if (!EMBEDDER_URL) {
    // stub zero vector for local demo
    return texts.map(() => Array.from({ length: VECTOR_DIM }, () => 0));
  }
  const resp = await fetch(EMBEDDER_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ input: texts }),
  });
  if (!resp.ok)
    throw new Error(`Embedder ${resp.status}: ${await resp.text()}`);
  const j: any = await resp.json();
  return j.data as number[][];
}
function toArrayCast(v: number[], dim: number): string {
  return `ARRAY[${v
    .map((x) => (Number.isFinite(x) ? x : 0))
    .join(",")}]::vector(${dim})`;
}
function toVectorLiteral(v: number[]): string {
  return `[${v.map((x) => (Number.isFinite(x) ? x : 0)).join(",")}]`;
}

(async () => {
  startMetricsServer();

  const mongo = new MongoClient(MONGO_URL, { serverApi: ServerApiVersion.v1 });
  await mongo.connect();
  const db = mongo.db(DB_NAME);
  const pages = db.collection(COL_PAGES);

  const pg = new PgClient({
    host: PG_HOST,
    port: PG_PORT,
    database: PG_DB,
    user: PG_USER,
    password: PG_PASS,
  });
  await pg.connect();
  await pg.query(`CREATE TABLE IF NOT EXISTS page_embeddings (
    url TEXT PRIMARY KEY,
    title TEXT,
    detected_type TEXT,
    embedding vector(${VECTOR_DIM}),
    updated_at TIMESTAMP DEFAULT now()
  )`);

  console.log("collector-embed up");

  while (true) {
    try {
      const batch = await pages
        .find({ "meta.isEmbedded": { $ne: true } })
        .project({ url: 1, title: 1, detectedType: 1, body: 1 })
        .limit(BATCH_SIZE)
        .toArray();
      if (batch.length === 0) {
        await new Promise((r) => setTimeout(r, 2000));
        continue;
      }

      const texts = batch.map(toText);
      const vecs = await embedLocal(texts);
      if (vecs.length !== batch.length) throw new Error("embed count mismatch");

      for (let i = 0; i < batch.length; i++) {
        const b = batch[i];
        const v = vecs[i];
        if (!Array.isArray(v) || v.length !== VECTOR_DIM)
          throw new Error(`dim ${v?.length} != ${VECTOR_DIM} for ${b.url}`);

        const stop = embedLatency.startTimer();
        try {
          if (INLINE_VECTOR_SQL) {
            const vecExpr = toArrayCast(v, VECTOR_DIM);
            await pg.query(
              `INSERT INTO page_embeddings (url, title, detected_type, embedding, updated_at)
               VALUES ($1,$2,$3,${vecExpr},now())
               ON CONFLICT (url) DO UPDATE SET
                 title=EXCLUDED.title, detected_type=EXCLUDED.detected_type,
                 embedding=EXCLUDED.embedding, updated_at=now()`,
              [b.url, b.title || null, b.detectedType || null]
            );
          } else {
            const lit = toVectorLiteral(v);
            await pg.query(
              `INSERT INTO page_embeddings (url, title, detected_type, embedding, updated_at)
               VALUES ($1,$2,$3,$4::vector(${VECTOR_DIM}),now())
               ON CONFLICT (url) DO UPDATE SET
                 title=EXCLUDED.title, detected_type=EXCLUDED.detected_type,
                 embedding=EXCLUDED.embedding, updated_at=now()`,
              [b.url, b.title || null, b.detectedType || null, lit]
            );
          }
          stop();
          embeddedPages.inc();
        } catch (e) {
          stop();
          embedFailures.inc();
          console.error("embed write error:", (e as any)?.message || e);
        }
      }
      const ids = batch.map((b) => b._id);
      await pages.updateMany(
        { _id: { $in: ids } },
        { $set: { "meta.isEmbedded": true, "meta.embeddedAt": new Date() } }
      );
      console.log(`Embedded batch: ${batch.length}`);
    } catch (e) {
      embedFailures.inc();
      console.error("collector-embed error:", (e as any)?.message || e);
      await new Promise((r) => setTimeout(r, 1500));
    }
  }
})();
