#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const DOCS_CSS = 'packages/docs/src/styles/global.css';
const STUDIO_CSS = 'packages/studio/src/web/styles/theme.css';
// Both apps are dark-only, so the whole palette lives in `:root` and there is no `.dark` block.
const BLOCKS = ['@theme inline', ':root'];

// The studio's theme tokens are copied by hand from docs (see packages/studio/README.md); this
// only guards against the copy silently drifting, it never syncs the files itself.
function extractBlock(source, selector, file) {
  const headerIndex = source.indexOf(`${selector} {`);
  if (headerIndex === -1) throw new Error(`${file}: could not find a "${selector} {" block`);

  const openBrace = source.indexOf('{', headerIndex);
  let depth = 0;
  let i = openBrace;
  for (; i < source.length; i++) {
    if (source[i] === '{') depth++;
    else if (source[i] === '}') {
      depth--;
      if (depth === 0) break;
    }
  }
  if (depth !== 0) throw new Error(`${file}: unterminated "${selector}" block`);

  return source.slice(openBrace, i + 1);
}

function normalize(block) {
  return block.replace(/\s+/g, ' ').trim();
}

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const docsCss = readFileSync(path.join(root, DOCS_CSS), 'utf8');
const studioCss = readFileSync(path.join(root, STUDIO_CSS), 'utf8');

const problems = [];
for (const selector of BLOCKS) {
  const docsBlock = normalize(extractBlock(docsCss, selector, DOCS_CSS));
  const studioBlock = normalize(extractBlock(studioCss, selector, STUDIO_CSS));
  if (docsBlock !== studioBlock) {
    problems.push(`"${selector}" differs between ${DOCS_CSS} and ${STUDIO_CSS}`);
  }
}

if (problems.length > 0) {
  console.error('Theme drift check failed:\n');
  for (const problem of problems) console.error(`  - ${problem}`);
  process.exitCode = 1;
} else {
  console.log(`Theme drift check passed — ${BLOCKS.length} token block(s) match.`);
}
