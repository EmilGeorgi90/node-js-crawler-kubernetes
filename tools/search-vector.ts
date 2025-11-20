import { Client as PgClient } from "pg";

const PG_HOST = process.env.PG_HOST || "localhost";
const PG_PORT = Number(process.env.PG_PORT || "5432");
const PG_DB = process.env.PG_DB || "vector";
const PG_USER = process.env.PG_USER || "postgres";
const PG_PASS = process.env.PG_PASS || "postgres";

(async () => {
  const pg = new PgClient({
    host: PG_HOST,
    port: PG_PORT,
    database: PG_DB,
    user: PG_USER,
    password: PG_PASS,
  });
  await pg.connect();

  // Example: search by a reference URL's vector
  const ref = process.env.REF_URL;
  if (ref) {
    const { rows } = await pg.query(
      `WITH q AS (SELECT embedding FROM page_embeddings WHERE url=$1)
SELECT url, title, detected_type,
1 - (embedding <=> (SELECT embedding FROM q)) AS cosine_sim
FROM page_embeddings
WHERE url <> $1
ORDER BY embedding <=> (SELECT embedding FROM q) ASC
LIMIT 10`,
      [ref]
    );
    console.log(rows);
    process.exit(0);
  }

  // Example: search by raw vector (you can compute via OpenAI too)
  console.log("Set REF_URL env to search by a page URL");
  process.exit(0);
})();
