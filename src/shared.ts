import puppeteer, {
  Browser,
  BrowserContext,
  Page,
  LaunchOptions,
} from "puppeteer";
import { Redis } from "ioredis";
import normalizeUrl from "normalize-url";

// ---------- ENV HELPERS ----------
export const env = {
  REDIS_URL: process.env.REDIS_URL || "redis://redis:6379",

  // Browser
  HEADLESS: (process.env.HEADLESS || "true").toLowerCase() === "true",
  PUPPETEER_EXECUTABLE_PATH: process.env.PUPPETEER_EXECUTABLE_PATH,

  // Timeouts
  NAV_TIMEOUT_MS: Number(process.env.NAV_TIMEOUT_MS || "30000"),
  OP_TIMEOUT_MS: Number(process.env.OP_TIMEOUT_MS || "15000"),

  // Optional consent cookie
  COOKIE_URL: process.env.COOKIE_URL || "",
  COOKIE_NAME: process.env.COOKIE_NAME || "",
  COOKIE_VALUE: process.env.COOKIE_VALUE || "",
  COOKIE_DOMAIN: process.env.COOKIE_DOMAIN || "",
  COOKIE_PATH: process.env.COOKIE_PATH || "/",

  // Retries
  MAX_NAV_RETRIES: Number(process.env.MAX_NAV_RETRIES || "2"),
};

// ---------- LOGGING ----------
export function makeLogger(prefix: string) {
  const tag = `[${prefix}]`;
  return {
    info: (msg: string, ...a: any[]) => console.log(tag, msg, ...a),
    warn: (msg: string, ...a: any[]) => console.warn(tag, msg, ...a),
    error: (msg: string, ...a: any[]) => console.error(tag, msg, ...a),
    debug: (msg: string, ...a: any[]) => {
      if ((process.env.DEBUG || "").toLowerCase() === "true")
        console.log(`${tag} [debug]`, msg, ...a);
    },
  };
}

// ---------- REDIS ----------
export function createRedis(url = env.REDIS_URL) {
  return new Redis(url);
}

// ---------- SLEEP ----------
export function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------- URL UTILS ----------
export function canonUrl(url: string): string {
  try {
    return normalizeUrl(url, {
      stripHash: true,
      stripWWW: false,
      removeTrailingSlash: false,
      sortQueryParameters: true,
      removeQueryParameters: [/^utm_\w+/i, "fbclid", "gclid"],
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
    } catch {
      /* ignore */
    }
  }
  return Array.from(out);
}

export function csvToSelectors(csv: string): string[] {
  return csv
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

// ---------- BROWSER LAUNCH ----------
export async function launchBrowser(
  opts: Partial<LaunchOptions> = {}
): Promise<Browser> {
  const args = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
  ];
  const base: LaunchOptions = { headless: env.HEADLESS, args };
  if (env.PUPPETEER_EXECUTABLE_PATH)
    (base as any).executablePath = env.PUPPETEER_EXECUTABLE_PATH;
  return puppeteer.launch({ ...base, ...opts });
}

export async function newContext(browser: Browser): Promise<BrowserContext> {
  return browser.createBrowserContext();
}

// ---------- PAGE SETUP & NAV ----------
export async function configurePage(page: Page) {
  page.setDefaultNavigationTimeout(env.NAV_TIMEOUT_MS);
  page.setDefaultTimeout(env.OP_TIMEOUT_MS);

  await page.setViewport({ width: 1360, height: 900, deviceScaleFactor: 1 });

  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const type = req.resourceType();
    if (["document", "xhr", "fetch", "script"].includes(type)) req.continue();
    else req.abort();
  });

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
  );
  await page.setExtraHTTPHeaders({
    "Accept-Language": "da-DK,da;q=0.9,en-US;q=0.8,en;q=0.7",
    "Upgrade-Insecure-Requests": "1",
  });
}

export async function navigateWithRetries(
  page: Page,
  url: string,
  retries = env.MAX_NAV_RETRIES
): Promise<boolean> {
  for (let i = 0; i <= retries; i++) {
    try {
      const resp = await page.goto(url, {
        waitUntil: "domcontentloaded", // change to 'networkidle2' if needed
        timeout: env.NAV_TIMEOUT_MS,
      });
      const status = resp?.status() ?? 0;
      const finalUrl = resp?.url() || page.url();
      console.log(
        `[NAV TRY #${i}] status=${status} url=${url} final=${finalUrl}`
      );
      if (status >= 200 && status < 400) return true;
      if (status === 429 || status >= 500 || status === 0)
        throw new Error(`HTTP ${status}`);
      return false; // 4xx non-retry
    } catch (e: any) {
      console.warn(`[NAV ERR #${i}] ${url} -> ${e?.message || e}`);
      if (i === retries) return false;
      await sleep(500 * Math.pow(2, i)); // 0.5s,1s,2s...
    }
  }
  return false;
}

// ---------- CONSENT COOKIE (OPTIONAL) ----------
export async function setConsentCookieIfConfigured(
  browser: Browser,
  log = makeLogger("cookie")
) {
  const { COOKIE_NAME, COOKIE_VALUE, COOKIE_DOMAIN, COOKIE_URL } = env;
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
      path: env.COOKIE_PATH || "/",
    } as any);
    log.info(`Set consent cookie for domain=${COOKIE_DOMAIN}`);
  } catch (err: any) {
    log.warn(`Failed to set consent cookie: ${err?.message || err}`);
  } finally {
    await page.close().catch(() => {});
    await ctx.close().catch(() => {});
  }
}

// ---------- EXTRACTION HELPERS ----------
export interface ExtractResult {
  title: string | null;
  body: any; // normalized node tree or {children:[{text}]} in AI mode
  html: string; // concatenated innerHTML (selective) or '' (AI mode)
  anchors: string[];
}

/** Extracts only from allowed selectors (anchors are still global). */
export async function extractSelective(
  page: Page,
  selectorsCsv: string
): Promise<ExtractResult> {
  const selectors = csvToSelectors(selectorsCsv);
  const hasBody = await page.$("body").catch(() => null);
  if (!hasBody || selectors.length === 0) {
    return {
      title: await page.title().catch(() => null),
      body: null,
      html: "",
      anchors: [],
    };
  }

  const result = await page.evaluate((sels: string[]) => {
    function parse(node: any): any {
      if (!node) return null;
      if (["STYLE", "SCRIPT", "IFRAME", "NOSCRIPT"].includes(node.tagName))
        return null;
      if (node.nodeType === Node.TEXT_NODE) {
        const t = node.textContent?.trim();
        return t ? { text: t } : null;
      }
      const element: any = {};
      if ((node as Element).attributes?.length) {
        element.attr = {};
        for (const attr of (node as Element).attributes)
          element.attr[attr.name] = attr.value;
      }
      const kids: any[] = [];
      for (const c of (node as Element).childNodes) {
        const p = parse(c);
        if (p) kids.push(p);
      }
      if (kids.length === 1) {
        const ch = kids[0];
        if (element.attr) ch.attr = { ...(ch.attr || {}), ...element.attr };
        return ch;
      }
      return kids.length ? { ...element, children: kids } : null;
    }

    const nodes: HTMLElement[] = [];
    for (const s of sels)
      document
        .querySelectorAll(s)
        .forEach((el) => nodes.push(el as HTMLElement));
    const uniq = Array.from(new Set(nodes));
    const children: any[] = [];
    let html = "";
    for (const el of uniq) {
      const p = parse(el);
      if (p) children.push(p);
      html += (el as HTMLElement).innerHTML + "\n";
    }

    const anchors = Array.from(document.querySelectorAll("a[href]"))
      .map((a) => (a as HTMLAnchorElement).href)
      .filter(Boolean);

    return {
      body: children.length ? { children } : null,
      html,
      anchors: Array.from(new Set(anchors)),
    };
  }, selectors);

  const title = await page.title().catch(() => null);
  return {
    title,
    body: result.body,
    html: result.html,
    anchors: result.anchors as string[],
  };
}

/** AI mode: capture full body text + all anchors; no innerHTML. */
export async function extractAiMode(page: Page): Promise<ExtractResult> {
  const title = await page.title().catch(() => null);
  const text = await page
    .$eval("body", (el) => (el as HTMLElement).innerText)
    .catch(() => "");
  const anchors = await page
    .$$eval("a[href]", (as) => [
      ...new Set(as.map((a) => (a as HTMLAnchorElement).href)),
    ])
    .catch(() => []);
  const body = text ? { children: [{ text }] } : null;
  return { title, body, html: "", anchors: anchors as string[] };
}
export async function applyStealth(page: Page) {
  // Basic stealth: remove webdriver flag, set languages/plugins/hardware concurrency
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
    // @ts-ignore
    window.chrome = { runtime: {} };
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, "languages", {
      get: () => ["da-DK", "da", "en-US", "en"],
    });
    Object.defineProperty(navigator, "hardwareConcurrency", { get: () => 8 });
  });
}
