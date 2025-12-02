import puppeteer, {
  Browser,
  BrowserContext,
  Page,
  LaunchOptions,
} from "puppeteer";
import { Redis } from "ioredis";
import normalizeUrl from "normalize-url";
import { request } from "undici";
import * as crypto from "node:crypto";
import { load as cheerioLoad } from "cheerio";
import robotsParserImport from "robots-parser";
import { cfg, isTrue } from "./config.js";

export function makeLogger(prefix: string) {
  const tag = `[${prefix}]`;
  return {
    info: (m: string, ...a: any[]) => console.log(tag, m, ...a),
    warn: (m: string, ...a: any[]) => console.warn(tag, m, ...a),
    error: (m: string, ...a: any[]) => console.error(tag, m, ...a),
  };
}

export function createRedis(url = cfg.REDIS_URL) {
  return new Redis(url);
}
export function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export function canon(url: string) {
  try {
    return normalizeUrl(url, {
      stripHash: true,
      stripWWW: false,
      removeQueryParameters: [/^utm_\w+/i, "fbclid", "gclid"],
      sortQueryParameters: true,
      removeTrailingSlash: false,
    });
  } catch {
    return url;
  }
}

export function sameOriginOnly(links: string[], pageUrl: string) {
  const origin = new URL(pageUrl).origin;
  const out = new Set<string>();
  for (const href of links) {
    try {
      const abs = new URL(href, pageUrl).toString();
      if (new URL(abs).origin === origin) out.add(abs.split("#")[0]);
    } catch {}
  }
  return Array.from(out);
}

export function csvToSelectors(csv: string): string[] {
  return (csv || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export type ExtractResult = {
  title: string | null;
  body: any;
  html: string;
  text: string;
  anchors: string[];
  simhash?: string;
  contentHash?: string;
};

export async function fetchStatic(url: string) {
  const resp = await request(url, {
    maxRedirections: 2,
    headers: {
      "User-Agent":
        "Mozilla/5.0 (compatible; Crawler/1.0; +https://example.invalid)",
      "Accept-Language": "da-DK,da;q=0.9,en-US;q=0.8,en;q=0.7",
    },
  });
  if (resp.statusCode < 200 || resp.statusCode >= 400)
    throw new Error(`HTTP ${resp.statusCode}`);
  const ctype = String(resp.headers["content-type"] || "");
  if (!ctype.includes("text/html")) throw new Error(`Not HTML: ${ctype}`);
  const html = await resp.body.text();
  return html;
}

export function cheerioExtract(
  html: string,
  baseUrl: string,
  selectorsCsv: string
): ExtractResult {
  // Cheerio v1.1.x – options object is optional; deprecated keys removed
  const $ = cheerioLoad(html);

  const title = ($("title").first().text() || "").trim() || null;
  const selectors = csvToSelectors(selectorsCsv);

  let bodyTree: any = null;
  let bodyHtml = "";

  if (selectors.length) {
    const uniq: any[] = [];
    for (const sel of selectors) {
      $(sel).each((_i, el) => {
        if (!uniq.includes(el)) uniq.push(el);
      });
    }
    bodyHtml = uniq.map((el) => $.html(el)).join("\n");
    const asText = uniq
      .map((el) => $(el).text())
      .join("\n")
      .replace(/\s+/g, " ")
      .trim();
    bodyTree = asText ? { children: [{ text: asText }] } : null;
  } else {
    const textAll = $("body").text().replace(/\s+/g, " ").trim();
    bodyTree = textAll ? { children: [{ text: textAll }] } : null;
  }

  const anchors = [
    ...new Set(
      $("a[href]")
        .map((_, a) => $(a).attr("href") || "")
        .get()
    ),
  ]
    .filter(Boolean)
    .map((h) => new URL(h, baseUrl).toString());

  const text = bodyTree?.children?.[0]?.text || "";
  const contentHash = sha256(text);
  const simhash = toSimhash(text);

  return {
    title,
    body: bodyTree,
    html: bodyHtml,
    text,
    anchors,
    contentHash,
    simhash,
  };
}

export function sha256(s: string): string {
  return crypto
    .createHash("sha256")
    .update(s || "")
    .digest("hex");
}
export function toSimhash(text: string): string {
  // very small/deterministic “simhash-like” signature (ok for change gating)
  return crypto
    .createHash("sha1")
    .update((text || "").slice(0, 5000))
    .digest("hex")
    .slice(0, 16);
}

export async function launchBrowser(
  opts: Partial<LaunchOptions> = {}
): Promise<Browser> {
  const args = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
  ];
  const base: LaunchOptions = { headless: isTrue(cfg.HEADLESS), args };
  const exec = (process.env.PUPPETEER_EXECUTABLE_PATH || "") as any;
  if (exec) (base as any).executablePath = exec;
  return puppeteer.launch({ ...base, ...opts });
}
export async function newContext(b: Browser): Promise<BrowserContext> {
  return b.createBrowserContext();
}

export async function configurePage(page: Page) {
  page.setDefaultNavigationTimeout(cfg.NAV_TIMEOUT_MS);
  page.setDefaultTimeout(cfg.OP_TIMEOUT_MS);
  await page.setViewport({ width: 1360, height: 900, deviceScaleFactor: 1 });
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const t = req.resourceType();
    if (["document", "xhr", "fetch", "script"].includes(t)) req.continue();
    else req.abort();
  });
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
  );
  await page.setExtraHTTPHeaders({
    "Accept-Language": "da-DK,da;q=0.9,en-US;q=0.8,en;q=0.7",
  });
}

export async function navigateWithRetries(
  page: Page,
  url: string,
  retries = cfg.MAX_NAV_RETRIES
): Promise<boolean> {
  for (let i = 0; i <= retries; i++) {
    try {
      const resp = await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: cfg.NAV_TIMEOUT_MS,
      });
      const status = resp?.status() ?? 0;
      if (status >= 200 && status < 400) return true;
      if (status === 429 || status >= 500 || status === 0)
        throw new Error(`HTTP ${status}`);
      return false;
    } catch {
      if (i === retries) return false;
      await sleep(500 * Math.pow(2, i));
    }
  }
  return false;
}

export async function setConsentCookieIfConfigured(
  browser: Browser,
  log = makeLogger("cookie")
) {
  const { COOKIE_NAME, COOKIE_VALUE, COOKIE_DOMAIN, COOKIE_URL, COOKIE_PATH } =
    cfg;
  if (!COOKIE_NAME || !COOKIE_VALUE || !COOKIE_DOMAIN || !COOKIE_URL) return;
  const ctx = await browser.createBrowserContext();
  const page = await ctx.newPage();
  try {
    await page
      .goto(COOKIE_URL, { waitUntil: "domcontentloaded", timeout: 15000 })
      .catch(() => {});
    await page.setCookie({
      name: COOKIE_NAME,
      value: COOKIE_VALUE,
      domain: COOKIE_DOMAIN,
      path: COOKIE_PATH || "/",
    } as any);
    log.info(`Set consent cookie for domain=${COOKIE_DOMAIN}`);
  } catch (err: any) {
    log.warn(`Failed to set consent cookie: ${err?.message || err}`);
  } finally {
    await page.close().catch(() => {});
    await ctx.close().catch(() => {});
  }
}

export async function extractSelectivePptr(
  page: Page,
  selectorsCsv: string
): Promise<ExtractResult> {
  const selectors = csvToSelectors(selectorsCsv);
  const title = await page.title().catch(() => null);
  if (selectors.length === 0)
    return { title, body: null, html: "", text: "", anchors: [] };

  const result = await page.evaluate((sels: string[]) => {
    function parse(node: any): any {
      if (!node) return null;
      if (["STYLE", "SCRIPT", "IFRAME", "NOSCRIPT"].includes(node.tagName))
        return null;
      if (node.nodeType === Node.TEXT_NODE) {
        const t = node.textContent?.trim();
        return t ? { text: t } : null;
      }
      const kids: any[] = [];
      for (const c of (node as Element).childNodes) {
        const p = parse(c);
        if (p) kids.push(p);
      }
      if (kids.length === 1) return kids[0];
      return kids.length ? { children: kids } : null;
    }
    const seen = new Set<Element>();
    const uniq: Element[] = [];
    for (const s of sels)
      document.querySelectorAll(s).forEach((el) => {
        if (!seen.has(el)) {
          seen.add(el);
          uniq.push(el);
        }
      });
    const children: any[] = [];
    let html = "";
    let text = "";
    for (const el of uniq) {
      const p = parse(el);
      if (p) children.push(p);
      html += (el as HTMLElement).innerHTML + "\n";
      text += (el as HTMLElement).innerText + "\n";
    }
    const anchors = Array.from(document.querySelectorAll("a[href]"))
      .map((a) => (a as HTMLAnchorElement).href)
      .filter(Boolean);
    return {
      body: children.length ? { children } : null,
      html,
      text: text.replace(/\s+/g, " ").trim(),
      anchors: Array.from(new Set(anchors)),
    };
  }, selectors);

  const contentHash = sha256(result.text || "");
  const simhash = toSimhash(result.text || "");
  return {
    title,
    body: result.body,
    html: result.html,
    text: result.text || "",
    anchors: result.anchors as string[],
    contentHash,
    simhash,
  };
}

// --- robots.txt helper (with explicit types) ---
interface Robots {
  isAllowed: (url: string, ua?: string) => boolean;
}
type RobotsFactory = (url: string, body: string) => Robots;
const robotsFactory = robotsParserImport as unknown as RobotsFactory;

const robotsCache = new Map<string, Robots>();

export async function allowedByRobots(url: string): Promise<boolean> {
  try {
    const u = new URL(url);
    const robotsUrl = `${u.origin}/robots.txt`;
    let robots = robotsCache.get(u.origin);
    if (!robots) {
      const res = await request(robotsUrl);
      const body =
        res.statusCode >= 200 && res.statusCode < 300
          ? await res.body.text()
          : "";
      robots = robotsFactory(robotsUrl, body);
      robotsCache.set(u.origin, robots);
    }
    return robots.isAllowed(url, "*") ?? true;
  } catch {
    return true;
  }
}

// Simple sitemap discovery (optional)
export async function discoverSitemaps(startUrl: string): Promise<string[]> {
  if (!isTrue(process.env.ENABLE_SITEMAP || cfg.ENABLE_SITEMAP)) return [];
  try {
    const origin = new URL(startUrl).origin;
    const robotsUrl = `${origin}/robots.txt`;
    const resp = await request(robotsUrl);
    if (resp.statusCode < 200 || resp.statusCode >= 400) return [];
    const txt = await resp.body.text();
    const maps = [...txt.matchAll(/^sitemap:\s*(.+)$/gim)].map((m) =>
      m[1].trim()
    );
    return maps.filter(Boolean);
  } catch {
    return [];
  }
}
