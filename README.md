# Crawler-kubernetes
a horizontal scaling version of the nest.js crawler

# Build local image
docker build -t your-registry/crawler-cluster:latest .

# Start Redis + 4 workers + collector
docker compose up -d --scale worker=4

# Seed the queue (one-off)
docker run --rm --network host \
  -e REDIS_URL=redis://localhost:6379 \
  -e SEED_URL=https://www.lauritz.com/da/auctions \
  your-registry/crawler-cluster:latest node dist/seeder.js
