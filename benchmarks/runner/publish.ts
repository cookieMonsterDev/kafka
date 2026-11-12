import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';

import { aaSeries, aggregateSession } from './aggregate.ts';
import type { AggregateOptions, SessionSummary } from './aggregate.ts';
import { isAncestorOfOriginDevelop } from './core-source.ts';
import { validateSummary } from './schema.ts';
import { writeJson } from './session.ts';

export interface PublishOptions extends AggregateOptions {
  publishedDir: string;
  repoRoot: string;
  minRepetitions: number;
}

/** Every reason the session must not be published; empty when it may be. */
export async function findPublishBlockers(summary: SessionSummary, options: PublishOptions): Promise<string[]> {
  const blockers: string[] = [];
  const { manifest, environment } = summary;

  if (!environment) {
    blockers.push('environment.json is missing, so the doctor result is unknown');
  } else if (environment.forced || environment.warnings.length > 0) {
    blockers.push(`doctor reported warnings: ${environment.warnings.join(', ') || 'run was forced'}`);
  }

  if (manifest.core.isWorktree || manifest.core.isDirty) {
    blockers.push('kafka-core was built from the working tree, not from a commit on origin/develop');
  } else if (!(await isAncestorOfOriginDevelop(options.repoRoot, manifest.core.sha))) {
    blockers.push(`kafka-core SHA ${manifest.core.sha} is not on origin/develop`);
  }

  if (manifest.tier === 'smoke') {
    blockers.push('smoke sessions only check the harness; run the standard or full tier');
  }
  if (manifest.series.includes(aaSeries)) {
    blockers.push('A/A sessions measure the noise floor; pass the result to report/publish with --noise-floor');
  }
  if (manifest.finishedAt === null) {
    blockers.push('the session did not finish');
  }
  if (summary.failures.length > 0) {
    blockers.push(`${summary.failures.length} run(s) failed or did not verify`);
  }
  if (manifest.repetitions < options.minRepetitions) {
    blockers.push(`${manifest.repetitions} repetitions configured, at least ${options.minRepetitions} required`);
  }
  const shortCells = summary.cells.filter((cell) => cell.passedRuns < options.minRepetitions);
  if (shortCells.length > 0) {
    const names = shortCells.map((cell) => `${cell.scenario}/${cell.series}/${cell.runtime}/${cell.profile}`);
    blockers.push(`cells with fewer than ${options.minRepetitions} passed runs: ${names.join(', ')}`);
  }

  for (const error of await validateSummary(summary)) {
    blockers.push(`schema: ${error}`);
  }
  return blockers;
}

/**
 * Re-aggregates a raw session, checks every publishing guard, then writes
 * `<publishedDir>/<sessionId>.json` and `<publishedDir>/latest.json`. Throws with all blockers when
 * the session is not publishable. Returns the path of the session's published file.
 */
export async function publishSession(sessionDir: string, options: PublishOptions): Promise<string> {
  const summary = await aggregateSession(sessionDir, {
    noiseThresholdCv: options.noiseThresholdCv,
    noiseFloor: options.noiseFloor,
  });
  const blockers = await findPublishBlockers(summary, options);
  if (blockers.length > 0) {
    throw new Error(`Session ${summary.sessionId} cannot be published:\n- ${blockers.join('\n- ')}`);
  }

  const published = { ...summary, publishedAt: new Date().toISOString() };
  await mkdir(options.publishedDir, { recursive: true });
  const sessionPath = join(options.publishedDir, `${summary.sessionId}.json`);
  await writeJson(sessionPath, published);
  await writeJson(join(options.publishedDir, 'latest.json'), published);
  return sessionPath;
}
