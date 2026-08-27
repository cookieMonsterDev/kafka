#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { RELEASE_PACKAGES } from './resolve-release-package.mjs';

// Walks RELEASE_PACKAGES in publish-dependency order (D19), releasing only the packages the
// caller reports as changed. Each package after the first fast-forwards to the version commit
// the previous package's release just pushed, so it builds against what actually shipped.
const dryRun = process.argv.includes('--dry-run');
const changes = JSON.parse(process.env.RELEASE_CHANGES ?? '{}');

function run(command, args, extraEnv) {
  const result = spawnSync(command, args, {
    stdio: 'inherit',
    env: extraEnv ? { ...process.env, ...extraEnv } : process.env,
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

let released = false;

for (const pkg of RELEASE_PACKAGES) {
  if (!changes[pkg.name]) continue;

  console.log(`::group::release ${pkg.name}`);

  if (released) {
    run('git', ['pull', '--ff-only', 'origin', 'master']);
  }

  if (pkg.publishesToNpm) {
    run('pnpm', ['--filter', pkg.npmName, 'build']);
    run('pnpm', ['--filter', pkg.npmName, 'test']);
    if (!dryRun) {
      run('node', ['scripts/exchange-npm-oidc-token.mjs', pkg.npmName]);
    }
  } else {
    run('pnpm', ['--filter', `${pkg.npmName}...`, 'build'], pkg.buildEnv);
  }

  const releaseArgs = ['scripts/run-semantic-release.mjs', pkg.name];
  if (dryRun) releaseArgs.push('--dry-run');
  run('node', releaseArgs);

  console.log('::endgroup::');
  released = true;
}

if (!released) {
  console.log('No packages changed; nothing to release.');
}
