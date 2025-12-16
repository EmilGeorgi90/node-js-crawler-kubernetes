import { KafkaBus } from "./queue-kafka.js";
import { appConfig } from "./config.js";

const seed = process.env.SEED_URL || "https://www.lauritz.com";
const depth = Number(process.env.SEED_DEPTH || "0");

(async () => {
  const kafkaBus = new KafkaBus();
  await kafkaBus.start();
  await kafkaBus.enqueue(appConfig.TOPIC_INITIAL, {
    url: seed,
    depth,
    priority: "normal",
  });
  console.log("Seeded:", seed);
  process.exit(0);
})();
