#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import path from 'node:path';

// Writes the exchanged token to a fixed path under RUNNER_TEMP — 'npm-oidc.npmrc', the same name
// release-chain.mjs derives independently — rather than exporting it via $GITHUB_ENV: this script
// runs as a child process of release-chain.mjs, which itself runs as a single workflow step
// covering the whole chain. $GITHUB_ENV is only re-read by the Actions runner *between* steps, so
// a write here would never reach a sibling child process (the npm publish this same step goes on
// to run) — only a later, separate step. The caller reads the fixed path back out itself and
// passes it as NPM_CONFIG_USERCONFIG to that child process directly.

const packageName = process.argv[2];

if (!packageName) {
  throw new Error('Usage: exchange-npm-oidc-token.mjs <package-name>');
}

const requestUrl = process.env.ACTIONS_ID_TOKEN_REQUEST_URL;
const requestToken = process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN;
const runnerTemp = process.env.RUNNER_TEMP;

if (!requestUrl || !requestToken || !runnerTemp) {
  throw new Error('GitHub Actions OIDC environment is unavailable');
}

const idTokenUrl = new URL(requestUrl);
idTokenUrl.searchParams.set('audience', 'npm:registry.npmjs.org');

const idTokenResponse = await fetch(idTokenUrl, {
  headers: {
    accept: 'application/json',
    authorization: `Bearer ${requestToken}`,
  },
});
const idTokenBody = await idTokenResponse.json();

if (!idTokenResponse.ok || typeof idTokenBody.value !== 'string') {
  throw new Error(`GitHub OIDC token request failed (${idTokenResponse.status})`);
}

const exchangeUrl =
  `https://registry.npmjs.org/-/npm/v1/oidc/token/exchange/package/` + encodeURIComponent(packageName);
const exchangeResponse = await fetch(exchangeUrl, {
  method: 'POST',
  headers: {
    authorization: `Bearer ${idTokenBody.value}`,
  },
});
const exchangeBody = await exchangeResponse.json();

if (!exchangeResponse.ok || typeof exchangeBody.token !== 'string') {
  const detail = typeof exchangeBody.message === 'string' ? `: ${exchangeBody.message}` : '';
  throw new Error(`npm OIDC token exchange failed (${exchangeResponse.status})${detail}`);
}

process.stdout.write(`::add-mask::${exchangeBody.token}\n`);
const npmrcPath = path.join(runnerTemp, 'npm-oidc.npmrc');
await writeFile(npmrcPath, `//registry.npmjs.org/:_authToken=${exchangeBody.token}\n`, {
  encoding: 'utf8',
  mode: 0o600,
});
