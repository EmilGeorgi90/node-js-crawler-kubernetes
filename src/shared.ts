import puppeteer, { Browser, Page, LaunchOptions } from 'puppeteer';
import normalizeUrl from 'normalize-url';
import robotsParser from 'robots-parser';
import * as cheerio from 'cheerio';
import client from 'prom-client';
import http from 'node:http';

export function makeLogger(ns: string) {
  return {
    info: (m: string, ...a: any[]) => console.log(`[${ns}] ${m}`, ...a),
    warn: (m: string, ...a: any[]) => console.warn(`[${ns}] ${m}`, ...a),
    error: (m: string, ...a: any[]) => console.error(`[${ns}] ${m}`, ...a),
  };
}

export async function launchBrowser(): Promise<Browser> {
  const opts: LaunchOptions = {
    headless: (process.env.HEADLESS || 'true') === 'true',
    args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage'],
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH,
  } as any;
  return puppeteer.launch(opts);
}

export async function configurePage(page: Page) {
  page.setDefaultNavigationTimeout(30000);
  page.setDefaultTimeout(15000);
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const t = req.resourceType();
    if (['document','xhr','fetch','script'].includes(t)) req.continue();
    else req.abort();
  });
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');
}

export async function navigateWithRetries(page: Page, url: string, retries=2) {
  for (let i=0;i<=retries;i++) {
    try {
      const r = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      const s = r?.status() ?? 0;
      if (s >= 200 && s < 400) return true;
      if (s === 429 || s >= 500) throw new Error(`HTTP ${s}`);
      return false;
    } catch (e) {
      if (i===retries) return false;
      await new Promise(r => setTimeout(r, 1000*Math.pow(2,i)));
    }
  }
  return false;
}

export async function fetchStatic(url: string): Promise<string> {
  const { default: fetch } = await import('node-fetch');
  const r = await fetch(url, { redirect: 'follow', headers: { 'user-agent': 'Mozilla/5.0' } });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.text();
}

export function cheerioExtract(html: string, url: string, includeSelectors = '') {
  const $ = cheerio.load(html);
  const title = $('title').first().text().trim() || null;
  let text = '';
  let body: any = null;
  let anchors: string[] = [];

  if (includeSelectors) {
    const parts = includeSelectors.split(',').map(s=>s.trim()).filter(Boolean);
    const chunks: string[] = [];
    for (const sel of parts) $(sel).each((_, el) => { const t = $(el).text(); if (t) chunks.push(t); });
    text = chunks.join(' ').replace(/\s+/g,' ').trim();
    body = text ? { children: [{ text }] } : null;
    anchors = parts.flatMap(sel => $(sel).find('a[href]').map((_,a)=>$(a).attr('href')||'').get());
  } else {
    text = $('body').text().replace(/\s+/g,' ').trim();
    body = text ? { children: [{ text }] } : null;
    anchors = $('a[href]').map((_,a)=>$(a).attr('href')||'').get();
  }

  const crypto = require('node:crypto');
  const contentHash = crypto.createHash('sha256').update(text || '').digest('hex');
  const simhash = crypto.createHash('sha1').update((text||'').slice(0,5000)).digest('hex').slice(0,16);

  const abs = new Set<string>();
  for (const href of anchors) { try { abs.add(new URL(href, url).toString()); } catch {} }

  return { title, text, body, html, anchors: [...abs], contentHash, simhash };
}

export function sameOriginOnly(links: string[], baseUrl: string): string[] {
  const origin = new URL(baseUrl).origin;
  const out = new Set<string>();
  for (const href of links) {
    try { const u = new URL(href, baseUrl); if (u.origin === origin) out.add(normalizeUrl(u.toString(), { stripHash: true, removeTrailingSlash: false })); } catch {}
  }
  return [...out];
}

const robotsCache = new Map<string, ReturnType<typeof robotsParser>>();
export async function allowedByRobots(url: string): Promise<boolean> {
  try {
    const u = new URL(url);
    const robotsUrl = `${u.origin}/robots.txt`;
    let r = robotsCache.get(u.origin);
    if (!r) {
      const { default: fetch } = await import('node-fetch');
      const res = await fetch(robotsUrl);
      const body = res.ok ? await res.text() : '';
      r = robotsParser(robotsUrl, body);
      robotsCache.set(u.origin, r);
    }
    return r.isAllowed(url, '*') ?? true;
  } catch { return true; }
}

export function startMetricsServer(registry: client.Registry, port: number) {
  client.collectDefaultMetrics({ register: registry });
  const srv = http.createServer(async (_req, res) => {
    if (_req.url === '/metrics') {
      const b = await registry.metrics();
      res.writeHead(200, { 'Content-Type': registry.contentType });
      return res.end(b);
    }
    res.end('ok');
  });
  srv.listen(port, () => console.log(`[metrics] up :${port}`));
}