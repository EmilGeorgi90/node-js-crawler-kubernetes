import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import * as fs from "node:fs";
import * as path from "node:path";
import { Redis } from "ioredis";
import http from "node:http";
import client from "prom-client";

const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });
const rawWritten = new client.Counter({
  name: "collector_s3_raw_written_total",
  help: "NDJSON records written",
});
function startMetricsServer(port = Number(process.env.METRICS_PORT || 9104)) {
  const srv = http.createServer(async (_req, res) => {
    if (_req.url === "/metrics") {
      const b = await registry.metrics();
      res.writeHead(200, { "Content-Type": registry.contentType });
      return res.end(b);
    }
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
  });
  srv.listen(port, () => console.log(`[metrics] collector-s3 on :${port}`));
}

const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
const RESULTS_KEY = process.env.RESULTS_RAW_KEY || "crawl:results_raw";
const S3_BUCKET = process.env.S3_BUCKET;
const S3_PREFIX = process.env.S3_PREFIX || "lake";
const S3_REGION = process.env.AWS_REGION || "eu-west-1";
const S3_DIR = process.env.S3_DIR || "./data/lake";

const s3 = S3_BUCKET ? new S3Client({ region: S3_REGION }) : null;

const keyFor = (url: string, fetchedAt?: string) => {
  const d = fetchedAt ? new Date(fetchedAt) : new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const domain = new URL(url).hostname.replace(/[^a-zA-Z0-9.-]/g, "_");
  const ts = d.toISOString().replace(/[:.]/g, "-");
  return `${S3_PREFIX}/dt=${yyyy}/${mm}/${dd}/domain=${domain}/${ts}.ndjson`;
};

(async () => {
  startMetricsServer();
  const redis = new Redis(REDIS_URL);

  while (true) {
    const res = await redis.brpop(RESULTS_KEY, 5);
    if (!res) continue;
    const json = res[1];
    const obj = JSON.parse(json);
    const key = keyFor(obj.sitesUrl, obj?.meta?.fetchedAt);

    if (s3 && S3_BUCKET) {
      await s3.send(
        new PutObjectCommand({
          Bucket: S3_BUCKET,
          Key: key,
          Body: json + "\n",
          ContentType: "application/x-ndjson",
        })
      );
    } else {
      const full = path.join(S3_DIR, key);
      fs.mkdirSync(path.dirname(full), { recursive: true });
      fs.appendFileSync(full, json + "\n");
    }
    rawWritten.inc();
  }
})();
