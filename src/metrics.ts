import http from "node:http";
import client from "prom-client";

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

export type Cfg = {
  port?: number;
  serviceName?: string;
};

export function startMetricsServer({
  port = 9100,
  serviceName = "service",
}: Cfg = {}) {
  const server = http.createServer(async (req, res) => {
    if (req.url === "/metrics") {
      const body = await registry.metrics();
      res.writeHead(200, { "Content-Type": registry.contentType });
      return res.end(body);
    }
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
  });
  server.listen(port, () => {
    // eslint-disable-next-line no-console
    console.log(`[metrics] ${serviceName} listening on :${port}`);
  });
}

export function counter(name: string, help: string, labelNames: string[] = []) {
  const c = new client.Counter({ name, help, labelNames });
  registry.registerMetric(c);
  return c;
}
export function gauge(name: string, help: string, labelNames: string[] = []) {
  const g = new client.Gauge({ name, help, labelNames });
  registry.registerMetric(g);
  return g;
}
export function histogram(
  name: string,
  help: string,
  buckets: number[] = [0.05, 0.1, 0.25, 0.5, 1, 2, 5],
  labelNames: string[] = []
) {
  const h = new client.Histogram({ name, help, buckets, labelNames });
  registry.registerMetric(h);
  return h;
}
