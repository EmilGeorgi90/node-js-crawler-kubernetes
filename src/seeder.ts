import { KafkaBus } from './queue-kafka.js';
import { cfg } from './config.js';

const seed = process.env.SEED_URL || 'https://www.lauritz.com';
const depth = Number(process.env.SEED_DEPTH || '0');

(async () => {
  const bus = new KafkaBus();
  await bus.start();
  await bus.enqueue(cfg.TOPIC_INITIAL, { url: seed, depth, priority: 'normal' });
  console.log('Seeded:', seed);
  process.exit(0);
})();