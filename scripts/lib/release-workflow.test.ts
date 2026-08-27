import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'yaml';
import { describe, expect, it } from 'vitest';
import { RELEASE_PACKAGES } from './resolve-release-package.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');

function loadWorkflow(relativePath) {
  return parse(readFileSync(path.join(root, relativePath), 'utf8'));
}

const releaseWorkflow = loadWorkflow('.github/workflows/release.yml');
const ciWorkflow = loadWorkflow('.github/workflows/ci.yml');

describe('release.yml', () => {
  const changesJob = releaseWorkflow.jobs.changes;
  const filterStep = changesJob.steps.find((step) => step.uses?.startsWith('dorny/paths-filter'));

  it('detects packages with dorny/paths-filter', () => {
    expect(filterStep).toBeDefined();
  });

  it.each([...RELEASE_PACKAGES])('has a paths-filter entry for %s', (pkg) => {
    expect(filterStep.with.filters).toMatch(new RegExp(`(^|\\n)\\s*${pkg}:`));
  });

  it.each([...RELEASE_PACKAGES])('exposes a changes output for %s', (pkg) => {
    expect(changesJob.outputs).toHaveProperty(pkg);
  });

  it.each([...RELEASE_PACKAGES])('has a release-%s job', (pkg) => {
    expect(releaseWorkflow.jobs).toHaveProperty(`release-${pkg}`);
  });

  it.each([...RELEASE_PACKAGES])('gates a manually dispatched %s release on the package existing on disk', (pkg) => {
    // workflow_dispatch with an explicit `package` bypasses dorny/paths-filter (see the
    // step's `if:`), so a package chosen before it exists (e.g. cli before T4 scaffolds
    // packages/cli) must be re-gated here — otherwise the job runs instead of skipping.
    const flagsStep = changesJob.steps.find((step) => step.id === 'flags');
    expect(flagsStep.run).toMatch(new RegExp(`-f packages/${pkg}/package\\.json`));
  });

  const releaseJobNames = Object.keys(releaseWorkflow.jobs).filter((name) => name.startsWith('release-'));

  it('the release jobs form a single total order', () => {
    expect(releaseJobNames).toHaveLength(RELEASE_PACKAGES.size);

    const releaseJobSet = new Set(releaseJobNames);
    const predecessorOf = new Map();
    for (const name of releaseJobNames) {
      const needs = [releaseWorkflow.jobs[name].needs ?? []].flat();
      const releaseNeeds = needs.filter((need) => releaseJobSet.has(need));
      expect(releaseNeeds.length).toBeLessThanOrEqual(1);
      predecessorOf.set(name, releaseNeeds[0] ?? null);
    }

    const heads = releaseJobNames.filter((name) => predecessorOf.get(name) === null);
    expect(heads).toHaveLength(1);

    const successorOf = new Map(releaseJobNames.map((name) => [name, null]));
    for (const [name, predecessor] of predecessorOf) {
      if (predecessor === null) continue;
      expect(successorOf.get(predecessor)).toBeNull();
      successorOf.set(predecessor, name);
    }

    const visited = [];
    let current = heads[0];
    while (current !== null && current !== undefined && visited.length <= releaseJobNames.length) {
      visited.push(current);
      current = successorOf.get(current);
    }

    expect(new Set(visited)).toEqual(new Set(releaseJobNames));
    expect(visited).toHaveLength(releaseJobNames.length);
  });

  it('every non-first release job fast-forwards to origin/master first', () => {
    const releaseJobSet = new Set(releaseJobNames);
    const heads = releaseJobNames.filter((name) => {
      const needs = [releaseWorkflow.jobs[name].needs ?? []].flat();
      return !needs.some((need) => releaseJobSet.has(need));
    });

    for (const name of releaseJobNames) {
      const job = releaseWorkflow.jobs[name];
      const hasFastForward = job.steps.some((step) => step.name === 'Fast-forward to origin/master');
      if (heads.includes(name)) {
        expect(hasFastForward).toBe(false);
      } else {
        expect(hasFastForward).toBe(true);
      }
    }
  });
});

describe('ci.yml', () => {
  const unitJob = ciWorkflow.jobs.unit;
  const unitRunCommands = unitJob.steps.map((step) => step.run).filter(Boolean);

  it('does not hardcode a package filter for typecheck or test', () => {
    for (const run of unitRunCommands) {
      expect(run).not.toMatch(/--filter\s+@cookiemonsterdev\/kafka-core\b/);
    }
  });

  it('runs typecheck, test, and test:scripts generically', () => {
    expect(unitRunCommands.some((run) => /\bpnpm\s+(--filter\s+\S+\s+)?typecheck\b/.test(run))).toBe(true);
    expect(unitRunCommands).toContain('pnpm test');
    expect(unitRunCommands).toContain('pnpm test:scripts');
  });
});
