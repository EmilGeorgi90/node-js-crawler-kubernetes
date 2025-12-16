import client from "prom-client";
import { KafkaBus } from "./queue-kafka.js";
import { appConfig } from "./config.js";
import { startMetricsServer } from "./shared.js";

const metricsRegistry = new client.Registry();
const labeled = new client.Counter({
  name: "ai_enricher_labeled_total",
  help: "Docs labeled",
  registers: [metricsRegistry],
});
startMetricsServer(metricsRegistry, Number(process.env.METRICS_PORT || "9102"));

const AI_URL = process.env.AI_URL || "http://ai:8001";
const AI_PATH = process.env.AI_PATH || "/detect";

async function classifyRemote(url: string, text: string) {
  const { default: fetch } = await import("node-fetch");
  const httpResponse = await fetch(`${AI_URL}${AI_PATH}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ url, text }),
  });
  if (!httpResponse.ok)
    throw new Error(
      `AI classify error ${httpResponse.status}: ${await httpResponse.text().catch(() => "")}`
    );
  const jsonBody = await httpResponse.json() as any;
  if (!jsonBody?.label) throw new Error("AI response missing label");
  return jsonBody as { label: "product" | "page"; confidence: number };
}

(async () => {
  const kafkaBus = new KafkaBus();
  await kafkaBus.start();

  await kafkaBus.runConsumer({
    topics: [appConfig.TOPIC_RESULTS_DB],
    groupId: appConfig.GROUP_AI,
    concurrency: 2,
    handler: async (buf) => {
      const doc = JSON.parse(buf.toString());
      const url: string = doc.sitesUrl;
      const text: string = doc?.body?.children?.[0]?.text || "";
      const out = await classifyRemote(url, text);
      const enriched = {
        ...doc,
        detectedType: out.label,
        meta: { ...(doc.meta || {}), aiConfidence: out.confidence },
      };
      await kafkaBus.publish(appConfig.TOPIC_ENRICHED, enriched, url);
      if (out.label === "product")
        await kafkaBus.enqueue(appConfig.TOPIC_PRIORITY, {
          url,
          depth: 0,
          priority: "high",
        });
      labeled.inc();
      return "ok";
    },
  });
})();
