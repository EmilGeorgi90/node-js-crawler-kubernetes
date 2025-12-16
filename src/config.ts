export const appConfig = {
  KAFKA_BROKERS: (process.env.KAFKA_BROKERS || "redpanda:9092").split(","),
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID || "crawler",

  TOPIC_INITIAL: process.env.TOPIC_INITIAL || "crawl.initial",
  TOPIC_REVISIT: process.env.TOPIC_REVISIT || "crawl.revisit",
  TOPIC_PRIORITY: process.env.TOPIC_PRIORITY || "crawl.priority",
  TOPIC_RESULTS_DB: process.env.TOPIC_RESULTS_DB || "results.db",
  TOPIC_RESULTS_RAW: process.env.TOPIC_RESULTS_RAW || "results.raw",
  TOPIC_ENRICHED: process.env.TOPIC_ENRICHED || "results.enriched",
  TOPIC_RETRY_30S: process.env.TOPIC_RETRY_30S || "crawl.initial.retry.30s",
  TOPIC_RETRY_5M: process.env.TOPIC_RETRY_5M || "crawl.initial.retry.5m",
  TOPIC_DLQ: process.env.TOPIC_DLQ || "crawl.initial.dlq",

  GROUP_WORKER: process.env.KAFKA_GROUP_WORKER || "workers",
  GROUP_DB: process.env.KAFKA_GROUP_DB || "collector-mongo",
  GROUP_RAW: process.env.KAFKA_GROUP_RAW || "collector-raw",
  GROUP_EMB: process.env.KAFKA_GROUP_EMB || "collector-embed",
  GROUP_AI: process.env.KAFKA_GROUP_AI || "ai-enricher",

  HEADLESS: (process.env.HEADLESS || "true") === "true",
  STATIC_FIRST: (process.env.STATIC_FIRST || "true") === "true",
  INCLUDE_SELECTORS: process.env.INCLUDE_SELECTORS || "",
  MAX_DEPTH: Number(process.env.MAX_DEPTH || "3"),

  METRICS_PORT: Number(process.env.METRICS_PORT || "9100"),
};
