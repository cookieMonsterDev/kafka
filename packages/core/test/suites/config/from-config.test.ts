import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { Kafka } from '../../../src/client';
import { plainTextBrokers, secureRandom } from '../../helpers/index';

let dir: string | undefined;

afterEach(() => {
  if (dir != null) {
    rmSync(dir, { recursive: true, force: true });
    dir = undefined;
  }
});

function tempConfigDir(): string {
  dir = mkdtempSync(join(tmpdir(), 'kafka-core-from-config-'));
  return dir;
}

function brokersLiteral(): string {
  return JSON.stringify(plainTextBrokers());
}

/**
 * End-to-end: a real `kafka.config.*` file resolved by `new Kafka()` / `Kafka.fromConfig()`
 * against a live broker — the fixture supplies only `brokers`, so a successful `admin.listTopics()`
 * proves the whole discover → load → merge → constructor path, not just the pure resolution logic
 * already covered by `src/config/resolve.test.ts`.
 */
describe('kafka.config.* resolution against a live broker', () => {
  it('resolves brokers from a .ts config file (new Kafka)', async () => {
    const cwd = tempConfigDir();
    writeFileSync(join(cwd, 'kafka.config.ts'), `export default { client: { brokers: ${brokersLiteral()} } };`);

    const kafka = new Kafka({ config: join(cwd, 'kafka.config.ts'), clientId: `test-${secureRandom()}` });
    expect(kafka.configSource().keys.brokers).toBe('file');

    const admin = kafka.admin();
    try {
      await admin.connect();
      await expect(admin.listTopics()).resolves.toEqual(expect.any(Array));
    } finally {
      await admin.disconnect();
    }
  });

  it('resolves brokers from a .mts config file (new Kafka)', async () => {
    const cwd = tempConfigDir();
    writeFileSync(join(cwd, 'kafka.config.mts'), `export default { client: { brokers: ${brokersLiteral()} } };`);

    const kafka = new Kafka({ config: join(cwd, 'kafka.config.mts'), clientId: `test-${secureRandom()}` });
    const admin = kafka.admin();
    try {
      await admin.connect();
      await expect(admin.listTopics()).resolves.toEqual(expect.any(Array));
    } finally {
      await admin.disconnect();
    }
  });

  it('resolves brokers from an async factory export through Kafka.fromConfig()', async () => {
    const cwd = tempConfigDir();
    writeFileSync(
      join(cwd, 'kafka.config.mjs'),
      `export default async () => ({ client: { brokers: ${brokersLiteral()} } });`,
    );

    const kafka = await Kafka.fromConfig({ clientId: `test-${secureRandom()}` }, { cwd });
    expect(kafka.configSource().keys.brokers).toBe('file');

    const admin = kafka.admin();
    try {
      await admin.connect();
      await expect(admin.listTopics()).resolves.toEqual(expect.any(Array));
    } finally {
      await admin.disconnect();
    }
  });
});
