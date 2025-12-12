import { KafkaBus } from './queue-kafka.js';
import * as fs from 'node:fs';
import * as path from 'node:path';
import client from 'prom-client';
import { cfg } from './config.js';
import { startMetricsServer } from './shared.js';

const registry = new client.Registry();
const written = new client.Counter({ name: 'collector_s3_written_total', help: 'raw records written', registers: [registry] });
startMetricsServer(registry, Number(process.env.METRICS_PORT || '9103'));

function keyFor(url: string, fetchedAt?: string) {
  const d = fetchedAt ? new Date(fetchedAt) : new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const ts = d.toISOString().replace(/[:.]/g, '-');
  const domain = new URL(url).hostname.replace(/[^a-zA-Z0-9.-]/g, '_');
  return `lake/dt=${yyyy}/${mm}/${dd}/domain=${domain}/${ts}.ndjson`;
}

(async () => {
  const bus = new KafkaBus();
  await bus.start();

  await bus.runConsumer({
    topics: [cfg.TOPIC_RESULTS_RAW],
    groupId: cfg.GROUP_RAW,
    concurrency: 3,
    handler: async (buf) => {
      const json = buf.toString();
      const obj = JSON.parse(json);
      const key = keyFor(obj.sitesUrl, obj?.meta?.fetchedAt);
      const base = process.env.S3_DIR || './data/lake';
      const full = path.join(base, key);
      fs.mkdirSync(path.dirname(full), { recursive: true });
      fs.appendFileSync(full, json + '\n');
      written.inc();
      return 'ok';
    }
  });
})();