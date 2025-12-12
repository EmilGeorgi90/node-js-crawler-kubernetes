import client from 'prom-client';
import { KafkaBus } from './queue-kafka.js';
import { cfg } from './config.js';
import { startMetricsServer } from './shared.js';

const registry = new client.Registry();
const labeled = new client.Counter({ name: 'ai_enricher_labeled_total', help: 'Docs labeled', registers: [registry] });
startMetricsServer(registry, Number(process.env.METRICS_PORT || '9102'));

const AI_URL = process.env.AI_URL || 'http://ai:8001';
const AI_PATH = process.env.AI_PATH || '/detect';

async function classifyRemote(url: string, text: string) {
  const { default: fetch } = await import('node-fetch');
  const r = await fetch(`${AI_URL}${AI_PATH}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ url, text }) });
  if (!r.ok) throw new Error(`AI classify error ${r.status}: ${await r.text().catch(()=> '')}`);
  const j = await r.json() as any;
  if (!j?.label) throw new Error('AI response missing label');
  return j as { label: 'product'|'page'; confidence: number };
}

(async () => {
  const bus = new KafkaBus();
  await bus.start();

  await bus.runConsumer({
    topics: [cfg.TOPIC_RESULTS_DB],
    groupId: cfg.GROUP_AI,
    concurrency: 2,
    handler: async (buf) => {
      const doc = JSON.parse(buf.toString());
      const url: string = doc.sitesUrl;
      const text: string = doc?.body?.children?.[0]?.text || '';
      const out = await classifyRemote(url, text);
      const enriched = { ...doc, detectedType: out.label, meta: { ...(doc.meta||{}), aiConfidence: out.confidence } };
      await bus.publish(cfg.TOPIC_ENRICHED, enriched, url);
      if (out.label === 'product') await bus.enqueue(cfg.TOPIC_PRIORITY, { url, depth: 0, priority: 'high' });
      labeled.inc();
      return 'ok';
    }
  });
})();