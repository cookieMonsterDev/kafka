import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { headerValueBytes, payloadShapes } from '../client/payload.ts';
import { payloadSizes, scenarioIds, tiers } from '../client/spec.ts';
import { deriveBudget, minimumVmCpus, minimumVmMemoryBytes } from './budget.ts';
import { runCounts } from './schedule.ts';

// Generous per-record allowance for the record batch framing, header names and log indexes.
const recordOverheadBytes = 64;
// Room for the internal topics and for error in the per-record estimate.
const tmpfsUsableRatio = 0.75;

describe('runCounts', () => {
  const { brokerTmpfsBytes } = deriveBudget({ cpus: minimumVmCpus, memoryBytes: minimumVmMemoryBytes });

  for (const tier of tiers) {
    for (const payload of payloadSizes) {
      it(`keeps every ${tier} topic with ${payload} records inside the broker tmpfs`, () => {
        const shape = payloadShapes[payload];
        const recordBytes =
          shape.keyBytes + shape.valueBytes + shape.headerCount * headerValueBytes + recordOverheadBytes;
        for (const scenario of scenarioIds) {
          const counts = runCounts(tier, scenario, 1, payload);
          const topicBytes = (counts.messageCount + counts.warmupCount) * recordBytes;
          assert.ok(topicBytes < brokerTmpfsBytes * tmpfsUsableRatio, `${scenario}: about ${topicBytes} bytes`);
        }
      });
    }
  }

  it('scales record counts and timeouts but never the startup scenario', () => {
    const base = runCounts('standard', 'P2', 1, 'medium');
    const scaled = runCounts('standard', 'P2', 2, 'medium');
    assert.equal(scaled.messageCount, base.messageCount * 2);
    assert.equal(scaled.warmupCount, base.warmupCount * 2);
    assert.equal(scaled.timeoutMs, base.timeoutMs * 2);
    assert.deepEqual(runCounts('standard', 'L1', 8, 'medium'), runCounts('standard', 'L1', 1, 'medium'));
  });
});
