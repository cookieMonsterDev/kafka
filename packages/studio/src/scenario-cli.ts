import { readFile, stat } from 'node:fs/promises';
import { resolve } from 'node:path';
import { scenarioSchema } from './shared/contracts/debug';
import { createKafkaClient, resolveStudioConnectionConfig } from './server/kafka/connection';
import { readStudioSection } from './server/studio-config';
import { runScenario } from './server/kafka/scenario';
import type { Runtime } from './runtime';

/** Headless runner for the same versioned scenario files the debugger exports. */
export async function runScenarioFile(path: string, profile: string | null, runtime: Runtime): Promise<number> {
  const filename = resolve(runtime.cwd, path);
  if ((await stat(filename)).size > 1000000) throw new Error('Scenario file exceeds 1 MB');
  const scenario = scenarioSchema.parse(JSON.parse(await readFile(filename, 'utf8')) as unknown);
  const connection = await resolveStudioConnectionConfig({ cwd: runtime.cwd, env: runtime.env });
  if (readStudioSection(connection.fileConfig).readOnly === true)
    throw new Error('Scenarios cannot run while studio.readOnly is enabled');
  const client = createKafkaClient(connection, profile);
  const admin = client.admin();
  const producer = client.producer();
  const signal = AbortSignal.any([runtime.signal, AbortSignal.timeout(120000)]);
  try {
    await admin.connect();
    await producer.connect();
    const result = await runScenario(
      scenario,
      admin,
      producer,
      (selected) => ({ consumer: () => createKafkaClient(connection, selected).consumer({}) }),
      profile,
      signal,
      () => {},
    );
    runtime.stdout.write(`${JSON.stringify(result)}\n`);
    return result.results.length === scenario.steps.length && result.results.every((step) => step.status === 'passed')
      ? 0
      : 1;
  } finally {
    await Promise.all([admin.disconnect(), producer.disconnect()]);
  }
}
