#!/usr/bin/env node
import { appendFile } from 'node:fs/promises';

const packageName = process.argv[2];

if (!packageName) {
  throw new Error('Usage: exchange-npm-oidc-token.mjs <package-name>');
}

const requestUrl = process.env.ACTIONS_ID_TOKEN_REQUEST_URL;
const requestToken = process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN;
const githubEnv = process.env.GITHUB_ENV;

if (!requestUrl || !requestToken || !githubEnv) {
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
await appendFile(githubEnv, `NPM_TOKEN=${exchangeBody.token}\n`, 'utf8');
