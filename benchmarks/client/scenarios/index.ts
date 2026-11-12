import type { ScenarioId } from '../spec.ts';
import { runConsumeScenario } from './consume.ts';
import { runE2eLatencyScenario } from './e2e-latency.ts';
import type { ScenarioRunner } from './harness.ts';
import { runProduceScenario } from './produce.ts';
import { runStartupScenario } from './startup.ts';

/** The runner for each scenario ID; wire settings and call shape come from `scenarios` in spec.ts. */
export const scenarioRunners: Readonly<Record<ScenarioId, ScenarioRunner>> = {
  P1: runProduceScenario,
  P2: runProduceScenario,
  P3: runProduceScenario,
  P4: runProduceScenario,
  P5: runProduceScenario,
  C1: runConsumeScenario,
  C2: runConsumeScenario,
  C3: runConsumeScenario,
  C4: runConsumeScenario,
  E1: runE2eLatencyScenario,
  L1: runStartupScenario,
};
