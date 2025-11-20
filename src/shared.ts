import puppeteer, { BrowserContext, Page } from "puppeteer";
import robotsParser from "robots-parser";
import normalizeUrl from "normalize-url";

export const canon = (url: string) => {
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
};

export const navigateStable = async (page: Page, url: string) => {
  const nav = page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
  await Promise.race([
    nav,
    page
      .waitForNavigation({ waitUntil: "domcontentloaded", timeout: 5000 })
      .catch(() => null),
  ]);
  await page
    .waitForFunction(() => document.readyState === "complete", {
      timeout: 15000,
    })
    .catch(() => {});
  // @ts-ignore
  await page
    .waitForNetworkIdle?.({ idleTime: 500, timeout: 10000 })
    .catch(() => {});
};

export const configurePage = async (page: Page) => {
  page.setDefaultNavigationTimeout(30000);
  page.setDefaultTimeout(15000);
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const type = req.resourceType();
    if (["document", "xhr", "fetch", "script", "stylesheet"].includes(type))
      req.continue();
    else req.abort();
  });
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
  );
};

export const isContextLoss = (e: any) =>
  /Execution context was destroyed|Cannot find (?:execution )?context with specified id/i.test(
    String(e?.message || e || "")
  );

export const withDomRetry = async <T>(
  fn: () => Promise<T>,
  retries = 2
): Promise<T> => {
  for (let i = 0; i <= retries; i++) {
    try {
      return await fn();
    } catch (e) {
      if (i === retries || !isContextLoss(e)) throw e;
      await new Promise((r) => setTimeout(r, 350 * (i + 1)));
    }
  }
  throw new Error("withDomRetry failed");
};

// Selective content extraction (allow-listed selectors), while collecting anchors from the whole document
export const extractSelective = async (
  page: Page,
  includeSelectorsCSV: string
) => {
  const include = includeSelectorsCSV
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  return await withDomRetry(() =>
    page.evaluate((selectors: string[]) => {
      const anchors = Array.from(document.querySelectorAll("a[href]"))
        .map((a) => {
          try {
            return new URL(
              (a as HTMLAnchorElement).href,
              location.href
            ).toString();
          } catch {
            return null as any;
          }
        })
        .filter(Boolean) as string[];

      function parse(node: any): any {
        if (["STYLE", "SCRIPT", "IFRAME", "NOSCRIPT"].includes(node.tagName))
          return null;
        if (node.nodeType === Node.TEXT_NODE) {
          const text = node.textContent?.trim();
          return text ? { text } : null;
        }
        const element: any = {};
        if ((node as Element).attributes?.length) {
          element.attr = {};
          for (const attr of (node as Element).attributes)
            (element.attr as any)[attr.name] = attr.value;
        }
        const children: any[] = [];
        for (const child of (node as any).childNodes) {
          const p = parse(child);
          if (p) children.push(p);
        }
        if (children.length === 1) {
          const child = children[0];
          if (element.attr)
            child.attr = { ...(child.attr || {}), ...element.attr };
          return child;
        }
        return children.length ? { ...element, children } : null;
      }

      const blocks = selectors.length
        ? selectors.flatMap((sel) => Array.from(document.querySelectorAll(sel)))
        : [document.body];
      const uniqueBlocks = Array.from(new Set(blocks));
      const parsedBlocks = uniqueBlocks.map((el) => parse(el)).filter(Boolean);

      return {
        anchors,
        title: document.title || null,
        body: parsedBlocks.length ? { children: parsedBlocks } : null,
        html: "",
      };
    }, include)
  );
};

export const allowedByRobotsFactory = () => {
  const cache = new Map<string, ReturnType<typeof robotsParser>>();
  return async (url: string): Promise<boolean> => {
    try {
      const u = new URL(url);
      const robotsUrl = `${u.origin}/robots.txt`;
      let robots = cache.get(u.origin);
      if (!robots) {
        const res = await fetch(robotsUrl);
        const body = res.ok ? await res.text() : "";
        robots = robotsParser(robotsUrl, body);
        cache.set(u.origin, robots);
      }
      return robots.isAllowed(url, "*") ?? true;
    } catch {
      return true;
    }
  };
};

export const launchBrowser = async (headless: boolean) => {
  const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
  const browser = await puppeteer.launch({
    headless,
    executablePath: executablePath || undefined,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
    ],
  });
  const context = await browser.createBrowserContext();
  return { browser, context };
};

export const seedCookie = async (
  context: BrowserContext,
  cookie?: { url: string; name: string; value: string; path?: string }
) => {
  if (!cookie) return;
  const p = await context.newPage();
  await p
    .setCookie({
      url: cookie.url,
      name: cookie.name,
      value: cookie.value,
      path: cookie.path ?? "/",
    })
    .catch(() => {});
  await p.close().catch(() => {});
};
