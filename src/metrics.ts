import http from 'node:http';
import client from 'prom-client';

const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

export const pagesProcessed = new client.Counter({
  name: 'crawler_pages_processed_total',
  help: 'Total pages successfully processed',
  labelNames: ['mode', 'queue'], // mode=ai|css, queue=initial|ai-revisit
});
export const pagesFailed = new client.Counter({
  name: 'crawler_pages_failed_total',
  help: 'Total pages failed',
  labelNames: ['reason'], // reason=navigate|exception
});
export const linksQueued = new client.Counter({
  name: 'crawler_links_enqueued_total',
  help: 'Links enqueued (same-origin)',
});
export const queueDepth = new client.Gauge({
  name: 'crawler_queue_depth',
  help: 'Current frontier queue depth',
  labelNames: ['queue'], // queue=crawl|ai_revisit
});

registry.registerMetric(pagesProcessed);
registry.registerMetric(pagesFailed);
registry.registerMetric(linksQueued);
registry.registerMetric(queueDepth);

export function startMetricsServer(port = Number(process.env.METRICS_PORT || 9100)) {
  const server = http.createServer(async (_req, res) => {
    if (_req.url === '/metrics') {
      const body = await registry.metrics();
      res.writeHead(200, { 'Content-Type': registry.contentType });
      return res.end(body);
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
  });
  server.listen(port, () => {
    console.log(`[metrics] listening on :${port}`);
  });
}