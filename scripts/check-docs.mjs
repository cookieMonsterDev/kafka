import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.join(root, 'packages/docs/dist');
function files(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(directory, entry.name);
    return entry.isDirectory() ? files(full) : [full];
  });
}
assert.ok(existsSync(output), 'Build docs before running docs:check');
const htmlFiles = files(output).filter((file) => file.endsWith('.html'));
const completion = readFileSync(path.join(output, 'docs/cli/reference/commands/completion/index.html'), 'utf8');
assert.ok(/bash\|zsh\|fish/.test(completion), 'Completion argument lost shell alternatives in a Markdown table');
const manifest = JSON.parse(readFileSync(path.join(output, 'docs-index.json'), 'utf8'));
const firstUrl = new URL(manifest.pages[0].url);
const base = firstUrl.pathname.slice(0, firstUrl.pathname.indexOf('/docs/'));
const origin = firstUrl.origin;
const problems = new Set();
const contents = new Map();
function text(file) {
  if (!contents.has(file)) contents.set(file, readFileSync(file, 'utf8'));
  return contents.get(file);
}
function checkLink(raw, source, pageUrl) {
  if (/^(?:mailto:|tel:|data:|javascript:)/.test(raw)) return;
  const url = new URL(raw.replaceAll('&amp;', '&'), pageUrl);
  if (url.origin !== origin) return;
  if (base && url.pathname !== base && !url.pathname.startsWith(`${base}/`)) {
    problems.add(`${source}: link escapes deployment base: ${raw}`);
    return;
  }
  const relative = decodeURIComponent(url.pathname.slice(base.length)).replace(/^\//, '');
  let target = path.join(output, relative);
  if (url.pathname.endsWith('/')) target = path.join(target, 'index.html');
  if (!existsSync(target)) {
    problems.add(`${source}: missing ${url.pathname}`);
    return;
  }
  if (url.hash && target.endsWith('.html')) {
    const anchor = decodeURIComponent(url.hash.slice(1));
    if (!text(target).includes(`id="${anchor}"`)) problems.add(`${source}: missing anchor ${url.pathname}${url.hash}`);
  }
}
for (const file of htmlFiles) {
  const relative = path.relative(output, file).replaceAll(path.sep, '/');
  const pageUrl = new URL(`${base}/${relative.replace(/index\.html$/, '')}`, origin);
  for (const match of text(file).matchAll(/\b(?:href|src)="([^"<>]*)"/g)) {
    checkLink(match[1], relative, pageUrl);
  }
}
for (const page of manifest.pages) {
  checkLink(page.url, 'manifest', origin);
  checkLink(page.markdown, 'manifest', origin);
  const relative = new URL(page.markdown).pathname.slice(base.length).replace(/^\//, '');
  const markdown = text(path.join(output, relative));
  assert.ok(markdown.includes(`Canonical: ${page.url}`), `Missing canonical for ${page.id}`);
  assert.ok(markdown.includes(`Source revision: ${manifest.revision}`), `Missing source revision for ${page.id}`);
}
// Runnable examples have a single authoritative file; their tutorial copies must stay exact.
for (const file of files(path.join(root, 'packages/docs/src/content/docs')).filter((file) => file.endsWith('.md'))) {
  const body = readFileSync(file, 'utf8');
  for (const match of body.matchAll(/<!-- example: ([^>]+) -->\s*```\w*\n([\s\S]*?)\n```/g)) {
    const source = readFileSync(path.join(root, 'packages/docs/examples', match[1].trim()), 'utf8').trim();
    assert.equal(match[2].trim(), source, `${file}: example ${match[1]} drifted`);
  }
}
if (problems.size) {
  console.error([...problems].join('\n'));
  process.exitCode = 1;
} else {
  console.log(
    `Docs check passed: ${htmlFiles.length} HTML files, ${manifest.pages.length} Markdown pages, deployment base ${base || '/'}, runnable examples`,
  );
}
