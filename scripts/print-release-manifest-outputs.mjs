#!/usr/bin/env node
import { appendFileSync } from 'node:fs';
import { RELEASE_PACKAGES } from './resolve-release-package.mjs';

// Emits the release manifest as GitHub Actions step outputs, so release.yml derives
// detect-release-packages' `packages`/`filters` inputs from RELEASE_PACKAGES instead of naming
// each package literally. Run inside a workflow step; requires $GITHUB_OUTPUT.
const githubOutput = process.env.GITHUB_OUTPUT;
if (!githubOutput) {
  throw new Error('GITHUB_OUTPUT is not set — run this inside a GitHub Actions step');
}

const packages = RELEASE_PACKAGES.map((pkg) => pkg.name).join(' ');
const filters = RELEASE_PACKAGES.map((pkg) => `${pkg.name}:\n  - 'packages/${pkg.name}/**'`).join('\n');

const delimiter = `EOF_${process.pid}`;
appendFileSync(githubOutput, `packages=${packages}\n` + `filters<<${delimiter}\n${filters}\n${delimiter}\n`, 'utf8');
