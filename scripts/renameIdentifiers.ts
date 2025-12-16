import { Project, SyntaxKind, Identifier } from "ts-morph";

const RENAME_MAP: Record<string, string> = {
  cfg: "appConfig",
  bus: "kafkaBus",
  ajv: "jsonSchemaValidator",
  validateTask: "validateCrawlTaskSchema",
  validateResult: "validateCrawlResultSchema",
  registry: "metricsRegistry",
  pagesProcessed: "pagesProcessedCounter",
  pagesFailed: "pagesFailedCounter",
  linksQueued: "linksQueuedCounter",
  activeByOrigin: "inflightByOriginGauge",
  concurrencyGauge: "workerConcurrencyGauge",
  log: "logger",
  maxTabs: "maxBrowserTabs",
  inFlight: "inflightByOrigin",
  acquire: "acquireOriginSlot",
  release: "releaseOriginSlot",
  dim: "embeddingDimension",
  ex: "extracted",
  r: "httpResponse",
  j: "jsonBody",
  navigateWithRetries: "navigatePageWithRetries",
  fetchStatic: "fetchStaticHtml",
  cheerioExtract: "extractTextAndLinksWithCheerio",
  sameOriginOnly: "filterSameOriginLinks",
  allowedByRobots: "isAllowedByRobotsTxt",
};

const project = new Project({
  tsConfigFilePath: "tsconfig.json",
});

project.addSourceFilesAtPaths("src/**/*.ts");

const renameIdentifier = (id: Identifier, newName: string) => {
  try {
    id.rename(newName);
  } catch {
    // If it fails (shadowing/reserved/etc), skip gracefully
  }
};

for (const sf of project.getSourceFiles()) {
  const ids = sf.getDescendantsOfKind(SyntaxKind.Identifier);

  ids.forEach((id) => {
    const text = id.getText();
    const newName = RENAME_MAP[text];
    if (!newName) return;

    // Skip property names in dotted access like obj.cfg or obj["cfg"]
    // but allow variable/function/parameter/class identifiers.
    const parent = id.getParent();
    const isPropertyAccess =
      parent?.getKind() === SyntaxKind.PropertyAccessExpression &&
      (parent as any).getNameNode?.() === id;

    if (isPropertyAccess) return;

    renameIdentifier(id, newName);
  });
}

project.save().then(() => {
  console.log("✅ Identifier rename pass complete.");
});
