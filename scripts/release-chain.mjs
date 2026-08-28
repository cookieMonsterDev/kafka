#!/usr/bin/env node
import { appendFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { RELEASE_PACKAGES } from './resolve-release-package.mjs';

// Walks RELEASE_PACKAGES in publish-dependency order (D19), releasing only the packages the
// caller reports as changed. Each package after the first fast-forwards to the version commit
// the previous package's release just pushed, so it builds against what actually shipped.
const dryRun = process.argv.includes('--dry-run');
const changes = JSON.parse(process.env.RELEASE_CHANGES ?? '{}');

let released = false;

// A mid-chain failure still leaves earlier packages' version-bump commits on master needing a
// master -> develop sync, so report whether anything released, success or not — the caller
// (release.yml's sync-develop job) reads this even when the step itself fails.
function reportReleasedAndExit(code) {
  const githubOutput = process.env.GITHUB_OUTPUT;
  if (githubOutput) {
    appendFileSync(githubOutput, `released=${released}\n`, 'utf8');
  }
  process.exit(code);
}

function run(command, args, extraEnv) {
  const result = spawnSync(command, args, {
    stdio: 'inherit',
    env: extraEnv ? { ...process.env, ...extraEnv } : process.env,
  });
  if (result.status !== 0) {
    reportReleasedAndExit(result.status ?? 1);
  }
}

for (const pkg of RELEASE_PACKAGES) {
  if (!changes[pkg.name]) continue;

  console.log(`::group::release ${pkg.name}`);

  if (released) {
    run('git', ['pull', '--ff-only', 'origin', 'master']);
  }

  // `${npmName}...` builds the package plus its workspace dependencies (e.g. core needs
  // config's dist for tsc to resolve its types) — needed even for a publishable package,
  // since a manual/partial chain run can reach it without that dependency's own build step
  // having run first in this job.
  run('pnpm', ['--filter', `${pkg.npmName}...`, 'build'], pkg.buildEnv);
  // The exchanged npm token's path can't travel back to us via $GITHUB_ENV — that only takes
  // effect for a *later workflow step*, and exchange-npm-oidc-token.mjs runs as our own child
  // process within this single step. Compute the same fixed path it writes to and hand it
  // directly to the semantic-release child below as NPM_CONFIG_USERCONFIG.
  let npmAuthEnv;
  if (pkg.publishesToNpm) {
    run('pnpm', ['--filter', pkg.npmName, 'test']);
    if (!dryRun) {
      run('node', ['scripts/exchange-npm-oidc-token.mjs', pkg.npmName]);
      npmAuthEnv = { NPM_CONFIG_USERCONFIG: path.join(process.env.RUNNER_TEMP, 'npm-oidc.npmrc') };
    }
  }

  const releaseArgs = ['scripts/run-semantic-release.mjs', pkg.name];
  if (dryRun) releaseArgs.push('--dry-run');
  run('node', releaseArgs, npmAuthEnv);

  console.log('::endgroup::');
  released = true;
}

if (!released) {
  console.log('No packages changed; nothing to release.');
}

reportReleasedAndExit(0);
