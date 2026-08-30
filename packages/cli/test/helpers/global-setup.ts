import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, isAbsolute, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HELPERS_DIR = dirname(fileURLToPath(import.meta.url));
// Reuses core's own compose files rather than shipping a second copy of them.
const CORE_ASSETS_DIR = resolve(HELPERS_DIR, '../../../core/test/assets');
const CLI_ROOT = resolve(HELPERS_DIR, '../..');
const DEFAULT_KAFKA_VERSION = '4.0';

/**
 * Only the versions this suite's plain topic lifecycle actually needs to distinguish — core's
 * own `test/helpers/kafka-version.ts` owns the full matrix. `COMPOSE_FILE` overrides either way.
 */
const KAFKA_VERSION_COMPOSE_FILES: Readonly<Record<string, string>> = Object.freeze({
  '0.10': 'docker-compose.zk-0-10.yml',
  '4.0': 'docker-compose.kraft.yml',
  '4.3': 'docker-compose.kraft-4-3.yml',
});

function resolveComposeFile(): string {
  const explicit = process.env.COMPOSE_FILE;
  if (explicit !== undefined && explicit.length > 0) {
    return isAbsolute(explicit) ? explicit : resolve(explicit);
  }

  const version = (process.env.KAFKA_VERSION ?? DEFAULT_KAFKA_VERSION).trim();
  const fileName = KAFKA_VERSION_COMPOSE_FILES[version];
  if (fileName === undefined) {
    throw new Error(
      `the cli integration suite only knows KAFKA_VERSION values ${Object.keys(KAFKA_VERSION_COMPOSE_FILES).join(', ')} (or an explicit COMPOSE_FILE); got "${version}"`,
    );
  }

  const filePath = join(CORE_ASSETS_DIR, fileName);
  if (!existsSync(filePath)) {
    throw new Error(`compose file "${fileName}" was not found under core's test/assets (looked at ${filePath})`);
  }
  return filePath;
}

function compose(args: string[]): void {
  execFileSync('docker', ['compose', '-f', resolveComposeFile(), ...args], { cwd: CLI_ROOT, stdio: 'inherit' });
}

export async function setup(): Promise<void> {
  if (process.env.KAFKA_EXTERNAL === '1') return;
  compose(['up', '--wait', '--wait-timeout', '180']);
}

export async function teardown(): Promise<void> {
  if (process.env.KAFKA_EXTERNAL === '1' || process.env.DO_NOT_STOP === '1') return;
  compose(['down', '--remove-orphans']);
}
