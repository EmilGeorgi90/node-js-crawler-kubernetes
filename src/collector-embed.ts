import { KafkaBus } from './queue-kafka.js';
import { Client } from 'pg';
import client from 'prom-client';
import { cfg } from './config.js';
import { startMetricsServer } from './shared.js';

const registry = new client.Registry();
const embedded = new client.Counter({ name: 'collector_embed_inserted_total', help: 'embeddings inserted', registers: [registry] });
startMetricsServer(registry, Number(process.env.METRICS_PORT || '9104'));

const dim = Number(process.env.EMBEDDING_DIM || '384');
const EMB_URL = process.env.EMBEDDINGS_URL || 'http://embeddings:8002';
const EMB_PATH = process.env.EMBEDDINGS_PATH || '/embed';

const pgClient = new Client({ host: process.env.PGHOST || 'localhost', port: Number(process.env.PGPORT || '5432'), user: process.env.PGUSER || 'postgres', password: process.env.PGPASSWORD || 'postgres', database: process.env.PGDATABASE || 'vector' });

async function ensureSchema() {
  await pgClient.query('CREATE EXTENSION IF NOT EXISTS vector');
  await pgClient.query(`CREATE TABLE IF NOT EXISTS doc_embeddings (id serial PRIMARY KEY, url text UNIQUE, title text, embedding vector(${dim}))`);
  await pgClient.query(`CREATE INDEX IF NOT EXISTS doc_embeddings_idx ON doc_embeddings USING ivfflat (embedding vector_l2_ops) WITH (lists = 100)`);
}

async function embedRemote(text: string): Promise<number[]> {
  const { default: fetch } = await import('node-fetch');
  const r = await fetch(`${EMB_URL}${EMB_PATH}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ text }) });
  if (!r.ok) throw new Error(`Embeddings error ${r.status}: ${await r.text().catch(()=> '')}`);
  const j = await r.json() as any;
  if (!Array.isArray(j?.embedding)) throw new Error('Bad embedding payload');
  if (j.embedding.length !== dim) throw new Error(`Embedding dim mismatch: ${j.embedding.length} != ${dim}`);
  return j.embedding.map((x: any) => Number(x));
}

(async () => {
  const bus = new KafkaBus();
  await bus.start();
  await pgClient.connect();
  await ensureSchema();

  await bus.runConsumer({
    topics: [cfg.TOPIC_ENRICHED],
    groupId: cfg.GROUP_EMB,
    concurrency: 2,
    handler: async (buf) => {
      const doc = JSON.parse(buf.toString());
      const text: string = doc?.body?.children?.[0]?.text || '';
      if (!text) return 'ok';
      const vec = await embedRemote(text);
      const pgVec = '[' + vec.join(',') + ']';
      await pgClient.query('INSERT INTO doc_embeddings (url, title, embedding) VALUES ($1,$2,$3::vector) ON CONFLICT (url) DO UPDATE SET title=EXCLUDED.title, embedding=EXCLUDED.embedding', [doc.sitesUrl, doc.title || null, pgVec]);
      embedded.inc();
      return 'ok';
    }
  });
})();