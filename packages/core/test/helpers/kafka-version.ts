import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const DEFAULT_KAFKA_VERSION = '4.0';

/**
 * Integration matrix. `KAFKA_VERSION` selects a compose file; `COMPOSE_FILE` overrides.
 * @see ../assets/README.md
 */
export const KAFKA_VERSION_COMPOSE_FILES: Readonly<Record<string, string>> = Object.freeze({
  '0.10': 'docker-compose.zk-0-10.yml',
  '0.11': 'docker-compose.zk-0-11.yml',
  '1.1': 'docker-compose.zk-1-1.yml',
  '2.4': 'docker-compose.zk-2-4.yml',
  '3.6': 'docker-compose.kraft-3-6.yml',
  '4.0': 'docker-compose.kraft.yml',
  '4.1': 'docker-compose.kraft-4-1.yml',
  '4.2': 'docker-compose.kraft-4-2.yml',
  '4.3': 'docker-compose.kraft-4-3.yml',
});

export const OAUTHBEARER_COMPOSE_FILE = 'docker-compose.kraft-oauthbearer.yml';

export function getKafkaVersion(env: NodeJS.ProcessEnv = process.env): string {
  const raw = env.KAFKA_VERSION?.trim();
  return raw && raw.length > 0 ? raw : DEFAULT_KAFKA_VERSION;
}

export function coerceKafkaVersion(version: string): readonly [number, number, number] {
  const normalized = version.trim().replace(/^v/i, '');
  const parts = normalized.split('.').map((part) => {
    const value = Number.parseInt(part, 10);
    return Number.isFinite(value) ? value : 0;
  });
  return [parts[0] ?? 0, parts[1] ?? 0, parts[2] ?? 0];
}

export function normalizeKafkaVersion(version: string): string {
  const [major, minor] = coerceKafkaVersion(version);
  return `${major}.${minor}`;
}

export function compareKafkaVersions(left: string, right: string): number {
  const a = coerceKafkaVersion(left);
  const b = coerceKafkaVersion(right);
  for (let i = 0; i < 3; i++) {
    const delta = a[i]! - b[i]!;
    if (delta !== 0) {
      return delta;
    }
  }
  return 0;
}

export function kafkaVersionAtLeast(version: string, envVersion = getKafkaVersion()): boolean {
  return compareKafkaVersions(normalizeKafkaVersion(envVersion), normalizeKafkaVersion(version)) >= 0;
}

export function kafkaVersionAtMost(version: string, envVersion = getKafkaVersion()): boolean {
  return compareKafkaVersions(normalizeKafkaVersion(envVersion), normalizeKafkaVersion(version)) <= 0;
}

export function kafkaVersionEquals(version: string, envVersion = getKafkaVersion()): boolean {
  return compareKafkaVersions(normalizeKafkaVersion(envVersion), normalizeKafkaVersion(version)) === 0;
}

/** KRaft production images in this matrix start at Kafka 3.0. */
export function isKRaftKafkaVersion(version = getKafkaVersion()): boolean {
  return kafkaVersionAtLeast('3.0', version);
}

/** ZooKeeper compose files are named `docker-compose.zk-*.yml`. */
export function isZooKeeperComposeFile(composeFile: string): boolean {
  return path.basename(composeFile).includes('zk-');
}

function assetsDir(): string {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../assets');
}

export function resolveComposeFile(env: NodeJS.ProcessEnv = process.env): string {
  if (env.COMPOSE_FILE) {
    return path.isAbsolute(env.COMPOSE_FILE) ? env.COMPOSE_FILE : path.resolve(env.COMPOSE_FILE);
  }

  const assets = assetsDir();
  if (env.OAUTHBEARER_ENABLED === '1') {
    return path.join(assets, OAUTHBEARER_COMPOSE_FILE);
  }

  const normalized = normalizeKafkaVersion(getKafkaVersion(env));
  const fileName = KAFKA_VERSION_COMPOSE_FILES[normalized];
  if (fileName == null) {
    throw new Error(
      `Unsupported KAFKA_VERSION=${normalized}. Known versions: ${Object.keys(KAFKA_VERSION_COMPOSE_FILES).join(', ')}`,
    );
  }

  const filePath = path.join(assets, fileName);
  if (!existsSync(filePath)) {
    throw new Error(
      `Compose file ${fileName} for KAFKA_VERSION=${normalized} is not in test/assets yet. Default: KAFKA_VERSION=${DEFAULT_KAFKA_VERSION}.`,
    );
  }

  return filePath;
}
