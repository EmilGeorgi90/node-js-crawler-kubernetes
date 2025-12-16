# Event‑Driven Crawler

Fully containerized, event‑driven web crawler using **Kafka (Redpanda)**, **MongoDB**, **Postgres/pgvector**, and **Prometheus/Grafana**. The pipeline enforces an external **AI classifier** and **Embeddings** service:

* **Raw NDJSON** to a local data lake (or S3 later)
* **Structured pages/products** to **MongoDB**
* **Embeddings** to **Postgres (pgvector)** for similarity search

A Redpanda Console, Mongo Express, pgAdmin, Grafana & Prometheus are included.

---

## Architecture

```
[crawl.initial]  [crawl.priority]  [crawl.revisit]  [retries...]
        \            |                   /
                     v
                 worker (Puppeteer/cheerio)
                        |                     \
                        v                      v
                  results.db  -------------> ai_enricher (mandatory)
                        |                         |
                        v                         v
                 collector_mongo            results.enriched
                        |                         |
                        v                         v
                  MongoDB (pages, products)   collector_embed (mandatory)
                                                |
                                                v
                                       Postgres + pgvector

   results.raw  <---------------- worker
        |
        v
  collector_s3  -> local data lake (NDJSON)
```

**Topics (Kafka):**

* `crawl.initial`, `crawl.revisit`, `crawl.priority` (tasks)
* `crawl.initial.retry.30s`, `crawl.initial.retry.5m`, `crawl.initial.dlq` (retries/DLQ)
* `results.db` (worker → AI enricher & Mongo collector)
* `results.raw` (worker → raw data lake)
* `results.enriched` (AI enricher → embed collector & Mongo collector)

**Schemas (AJV):** see `schemas/`:

* `task.crawl.v1.json`
* `result.page.v1.json`
* `result.enriched.v1.json`

---

## Services

| Service                 |  Port | Purpose                            |
| ----------------------- | ----: | ---------------------------------- |
| **Redpanda (Kafka)**    |  9092 | Message broker                     |
| **Redpanda Console**    |  8080 | Kafka UI                           |
| **MongoDB**             | 27017 | Document DB                        |
| **Mongo Express**       |  8081 | Mongo GUI                          |
| **Postgres (pgvector)** |  5432 | Vectors DB                         |
| **pgAdmin**             |  8082 | Postgres GUI                       |
| **Prometheus**          |  9090 | Metrics                            |
| **Grafana**             |  3000 | Dashboards                         |
| **AI**                  |  8001 | Mandatory classifier API `/detect` |
| **Embeddings**          |  8002 | Mandatory embedding API `/embed`   |
| **worker**              |  9100 | Crawling + extraction + publish    |
| **ai_enricher**         |  9102 | Calls AI → labels/enqueues         |
| **collector_mongo**     |  9101 | Upserts pages/products             |
| **collector_s3**        |  9103 | Writes NDJSON lake                 |
| **collector_embed**     |  9104 | Gets vectors → pgvector            |

---

## Prerequisites

* Docker Desktop (WSL2 recommended on Windows)
* Node 22 + pnpm (for local builds): `corepack enable && pnpm i`

---

## Quick Start

```bash
# 1) Install & build TS
pnpm i
pnpm build

# 2) Start everything
docker compose up -d --build

# 3) Enable pgvector (first run only)
docker compose exec postgres \
  psql -U postgres -d vector -c "CREATE EXTENSION IF NOT EXISTS vector;"

# 4) Seed a URL into the crawl queue
docker compose run --rm worker node dist/src/seeder.js
# (env) SEED_URL defaults to https://www.lauritz.com
```

### GUIs

* **Redpanda Console:** [http://localhost:8080](http://localhost:8080)
* **Mongo Express:** [http://localhost:8081](http://localhost:8081) (admin / admin123)
* **pgAdmin:** [http://localhost:8082](http://localhost:8082) (login: admin@local / admin123)

  * Add server: Host `postgres`, Port `5432`, User `postgres`, Pass `postgres`
* **Prometheus:** [http://localhost:9090](http://localhost:9090) (Targets should be UP)
* **Grafana:** [http://localhost:3000](http://localhost:3000) (admin / admin123)

---

## Configuration

Most knobs are set via environment variables in `docker-compose.yml`. Key ones:

**Worker**

* `HEADLESS=true` — Chromium headless
* `STATIC_FIRST=true` — try static fetch before JS
* `INCLUDE_SELECTORS=""` — optional CSV of CSS selectors to extract only specific content (anchors still collected page‑wide)
* `MAX_DEPTH=3` — crawl depth
* `PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium` — system Chromium in container

**AI Enricher (mandatory)**

* `AI_URL=http://ai:8001`
* `AI_PATH=/detect`

**Collector Embed (mandatory)**

* `EMBEDDINGS_URL=http://embeddings:8002`
* `EMBEDDINGS_PATH=/embed`
* `EMBEDDING_DIM=384`

**Mongo**

* `DB_NAME=crawler`, collections: `pages`, `products`

**Data Lake**

* `collector_s3`: writes to `./data/lake` by default (NDJSON partitioned by date/domain)

---

## Development Scripts

* `pnpm build` — TypeScript → `dist/`
* `pnpm dev:worker` — run worker with tsx
* `pnpm dev:ai` — run AI enricher with tsx (still calls external AI service)
* `pnpm dev:collector:*` — run collectors with tsx

---

## Data Models (high level)

**`result.page.v1.json` payload (worker → results.db):**

```jsonc
{
  "sitesUrl": "https://example.com/path",
  "title": "Page title",
  "body": { "children": [{ "text": "flattened page text ..." }] },
  "html": "...optional...",
  "meta": {
    "fetchedAt": "2025-12-12T12:34:56.000Z",
    "domain": "example.com",
    "selector_mode": "css|ai",
    "contentHash": "sha256...",
    "simhash": "..."
  }
}
```

**AI enricher output → `results.enriched`:** adds `{ detectedType: "product"|"page", meta.aiConfidence }`.

**Embeddings:** `collector_embed` converts text to vector (via `/embed`) and upserts into `doc_embeddings(url, title, embedding vector(384))` with IVFFLAT index.

---

## Troubleshooting

### Puppeteer/Chromium not found

* We ship Chromium in the container and set `PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium`.
* If you see `Could not find Chrome`, rebuild images: `docker compose build --no-cache worker`.

### pgvector errors (vector syntax)

* Ensure bracket format: `[$1,$2,...]` — the code already does this.
* Enable extension: `CREATE EXTENSION IF NOT EXISTS vector;`

### Prometheus shows no data

* Check targets: [http://localhost:9090/targets](http://localhost:9090/targets)
* Inside container, verify: `wget -qO- http://worker:9100/metrics | head`
* Grafana: add Prometheus datasource `http://prometheus:9090`

### Windows/WSL2 specifics

* Run Docker Desktop with WSL2 backend.
* Avoid mounting Windows paths into Linux containers for Chromium cache; we don’t cache Chromium.

### Clean slate

```bash
docker compose down -v
pnpm clean || true   # if you add a clean script
rm -rf node_modules dist data
pnpm i && pnpm build
docker compose up -d --build
```

---

## Roadmap / Notes

* Replace toy `ai` & `embeddings` with real local models (e.g., **onnxruntime**/**sentence-transformers** server)
* Add K8s manifests (Deployments/Services/ConfigMaps/Secrets, plus KEDA autoscaling)
* Optional: switch raw lake to S3/MinIO
* Add Kafka consumer lag into worker autoscaling

---

## License

MIT (c) 2025 Lauritz.com
