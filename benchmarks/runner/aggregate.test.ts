import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { compareStats, findClearBest } from './aggregate.ts';
import type { ComparedStat, ComparisonRule } from './aggregate.ts';

const lowerIsBetter: ComparisonRule = { noiseThresholdCv: 0.05, noiseFloor: 0, isLowerBetter: true };
const higherIsBetter: ComparisonRule = { ...lowerIsBetter, isLowerBetter: false };

function stat(median: number, min: number, max: number, cv = 0.01): ComparedStat {
  return { median, min, max, cv };
}

describe('compareStats', () => {
  it('names a verdict from the metric direction when ranges separate', () => {
    const fast = stat(1.0, 0.98, 1.02);
    const slow = stat(1.5, 1.45, 1.55);

    assert.equal(compareStats(fast, slow, lowerIsBetter)?.verdict, 'better');
    assert.equal(compareStats(slow, fast, lowerIsBetter)?.verdict, 'worse');
    assert.equal(compareStats(fast, slow, higherIsBetter)?.verdict, 'worse');
    assert.equal(compareStats(fast, slow, lowerIsBetter)?.reason, '');
    assert.ok(Math.abs((compareStats(slow, fast, lowerIsBetter)?.ratio ?? 0) - 1.5) < 1e-9);
  });

  it('is inconclusive when either side exceeds the noise threshold', () => {
    const comparison = compareStats(stat(1.0, 0.98, 1.02), stat(1.5, 1.45, 1.55, 0.08), lowerIsBetter);

    assert.equal(comparison?.verdict, 'inconclusive');
    assert.equal(comparison?.reason, 'a cell exceeds the noise threshold');
  });

  it('is inconclusive when min-max ranges overlap, including ranges that only touch', () => {
    // The E1 p50 medians of kafka-core and Platformatic on Node.js 24 in the 1 KiB session.
    const overlapping = compareStats(stat(1.379, 1.356, 1.566), stat(1.425, 1.38, 1.662), lowerIsBetter);
    const touching = compareStats(stat(1.0, 0.9, 1.1), stat(1.2, 1.1, 1.3), lowerIsBetter);

    assert.equal(overlapping?.verdict, 'inconclusive');
    assert.equal(overlapping?.reason, 'min-max ranges overlap');
    assert.equal(touching?.verdict, 'inconclusive');
  });

  it('is inconclusive when the difference is within the noise floor', () => {
    const rule = { ...lowerIsBetter, noiseFloor: 0.05 };

    assert.equal(
      compareStats(stat(1.0, 0.99, 1.01), stat(1.04, 1.03, 1.05), rule)?.reason,
      'difference is within the A/A noise floor',
    );
    assert.equal(compareStats(stat(1.0, 0.99, 1.01), stat(1.1, 1.09, 1.11), rule)?.verdict, 'better');
  });

  it('returns null when the reference median is 0', () => {
    assert.equal(compareStats(stat(1, 1, 1), stat(0, 0, 0), lowerIsBetter), null);
  });
});

describe('findClearBest', () => {
  const clients = (...stats: ComparedStat[]) => stats.map((entry, index) => ({ ...entry, client: `client-${index}` }));

  it('names the best candidate when it beats every other one', () => {
    const candidates = clients(stat(1.5, 1.45, 1.55), stat(1.0, 0.98, 1.02), stat(1.2, 1.15, 1.25));

    assert.equal(findClearBest(candidates, lowerIsBetter)?.client, 'client-1');
    assert.equal(findClearBest(candidates, higherIsBetter)?.client, 'client-0');
  });

  it('names no one when the best overlaps any other candidate, even if it beats the rest', () => {
    // E1 p50 on Node.js 24 in the 1 KiB session: kafka-core, Platformatic, KafkaJS, Confluent.
    const candidates = clients(
      stat(1.425, 1.38, 1.662, 0.0773),
      stat(1.379, 1.356, 1.566, 0.0612),
      stat(1.477, 1.343, 1.595, 0.0678),
      stat(3.415, 3.28, 3.486, 0.0267),
    );

    assert.equal(findClearBest(candidates, { ...lowerIsBetter, noiseThresholdCv: 1 }), null);
  });

  it('names no one when the best is noisy', () => {
    const candidates = clients(stat(1.0, 0.98, 1.02, 0.06), stat(1.5, 1.45, 1.55));

    assert.equal(findClearBest(candidates, lowerIsBetter), null);
  });

  it('names no one when medians tie or fewer than two candidates are measured', () => {
    assert.equal(findClearBest(clients(stat(1.0, 0.98, 1.02), stat(1.0, 0.99, 1.01)), lowerIsBetter), null);
    assert.equal(findClearBest(clients(stat(1.0, 0.98, 1.02)), lowerIsBetter), null);
    assert.equal(findClearBest([], lowerIsBetter), null);
  });
});
