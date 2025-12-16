import { Kafka, logLevel, Consumer, Producer } from "kafkajs";
import normalizeUrl from "normalize-url";
import { appConfig } from "./config.js";

export type Task = { url: string; depth: number; priority?: "normal" | "high" };

function keyFor(url: string) {
  try {
    return new URL(url).origin;
  } catch {
    try {
      return normalizeUrl(url, { stripHash: true });
    } catch {
      return "unknown";
    }
  }
}

export class KafkaBus {
  private kafka = new Kafka({
    clientId: appConfig.KAFKA_CLIENT_ID,
    brokers: appConfig.KAFKA_BROKERS,
    logLevel: logLevel.NOTHING,
  });
  private producer: Producer = this.kafka.producer({
    allowAutoTopicCreation: true,
  });

  async start() {
    await this.producer.connect();
  }
  async stop() {
    await this.producer.disconnect();
  }

  async enqueue(topic: string, task: Task) {
    await this.producer.send({
      topic,
      messages: [{ key: keyFor(task.url), value: JSON.stringify(task) }],
    });
  }

  async publish(topic: string, payload: unknown, keyUrl?: string) {
    await this.producer.send({
      topic,
      messages: [
        {
          key: keyUrl ? keyFor(keyUrl) : undefined,
          value: JSON.stringify(payload),
        },
      ],
    });
  }

  newConsumer(groupId: string) {
    return this.kafka.consumer({ groupId, allowAutoTopicCreation: true });
  }

  async runConsumer(opts: {
    topics: string[];
    groupId: string;
    concurrency?: number;
    handler: (
      value: Buffer,
      headers?: Record<string, any>
    ) => Promise<"ok" | "retry" | "dlq">;
  }) {
    const c: Consumer = this.newConsumer(opts.groupId);
    await c.connect();
    for (const t of opts.topics)
      await c.subscribe({ topic: t, fromBeginning: false });

    await c.run({
      partitionsConsumedConcurrently: opts.concurrency ?? 1,
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        const h: Record<string, any> = {};
        for (const [k, v] of Object.entries(message.headers || {}))
          h[k] = (v as any)?.toString?.() || v;
        const res = await opts.handler(message.value, h);
        if (res === "ok") return;
        const attempts = Number(h["attempts"] || "0") + 1;
        const value = message.value.toString();
        if (res === "retry") {
          const nextTopic =
            attempts >= 3
              ? process.env.TOPIC_RETRY_5M || "crawl.initial.retry.5m"
              : process.env.TOPIC_RETRY_30S || "crawl.initial.retry.30s";
          await this.producer.send({
            topic: nextTopic,
            messages: [
              {
                key: message.key?.toString(),
                value,
                headers: { attempts: Buffer.from(String(attempts)) },
              },
            ],
          });
        } else {
          const dlq = process.env.TOPIC_DLQ || "crawl.initial.dlq";
          await this.producer.send({
            topic: dlq,
            messages: [
              {
                key: message.key?.toString(),
                value,
                headers: { attempts: Buffer.from(String(attempts)) },
              },
            ],
          });
        }
        throw new Error("requeued");
      },
    });

    return c;
  }
}
