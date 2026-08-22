#!/usr/bin/env node
import { appendFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

const packageName = process.argv[2];

if (!packageName) {
  throw new Error('Usage: exchange-npm-oidc-token.mjs <package-name>');
}

const requestUrl = process.env.ACTIONS_ID_TOKEN_REQUEST_URL;
const requestToken = process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN;
const githubEnv = process.env.GITHUB_ENV;
const runnerTemp = process.env.RUNNER_TEMP;

if (!requestUrl || !requestToken || !githubEnv || !runnerTemp) {
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
await appendFile(githubEnv, `NPM_CONFIG_USERCONFIG=${npmrcPath}\n`, 'utf8');
