#!/usr/bin/env node
// Submits every docs URL to IndexNow (Bing, Seznam, Naver, Yep share submissions).
// Google does not support IndexNow; it discovers the site through the sitemap submitted in
// Search Console. Run after a docs deploy: the key file must already be live for IndexNow to
// accept the request.
//
//   node scripts/submit-search-index.mjs            # read the live sitemap and submit
//   node scripts/submit-search-index.mjs --dry-run  # print what would be submitted
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ENDPOINT = 'https://api.indexnow.org/indexnow';
// IndexNow keys are public by design; this one is served from packages/docs/public/<key>.txt.
const key = '40479e9f399aa7b9c93aae40d3080564';

const dryRun = process.argv.includes('--dry-run');
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const site = JSON.parse(readFileSync(path.join(root, 'packages/docs/package.json'), 'utf8')).homepage;
const keyLocation = new URL(`${key}.txt`, site).href;

async function fetchText(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`GET ${url} -> HTTP ${response.status}`);
  return response.text();
}

const [sitemap, servedKey] = await Promise.all([fetchText(new URL('sitemap.xml', site).href), fetchText(keyLocation)]);
const urlList = [...sitemap.matchAll(/<loc>([^<]+)<\/loc>/g)].map(([, loc]) => loc);
if (urlList.length === 0) throw new Error('sitemap.xml has no <loc> entries');
// IndexNow answers 202 even when the key file is missing, so check it here to fail loudly.
if (servedKey.trim() !== key) {
  throw new Error(`${keyLocation} does not serve the key; deploy the docs site first`);
}

const payload = { host: new URL(site).host, key, keyLocation, urlList };
if (dryRun) {
  console.log(JSON.stringify(payload, null, 2));
  process.exit(0);
}

const response = await fetch(ENDPOINT, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json; charset=utf-8' },
  body: JSON.stringify(payload),
});
// 200 = accepted, 202 = accepted pending key validation; anything else is a rejection.
if (response.status !== 200 && response.status !== 202) {
  throw new Error(`IndexNow rejected the submission: HTTP ${response.status} ${await response.text()}`);
}
console.log(`IndexNow accepted ${urlList.length} URLs (HTTP ${response.status})`);
