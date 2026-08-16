import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const helpersDir = path.dirname(fileURLToPath(import.meta.url));
const coreRoot = path.resolve(helpersDir, '../..');
const composeFile = path.join(
  coreRoot,
  'test/assets',
  process.env.OAUTHBEARER_ENABLED === '1' ? 'docker-compose.kraft-oauthbearer.yml' : 'docker-compose.kraft.yml',
);

function compose(args: string[]): void {
  execFileSync('docker', ['compose', '-f', composeFile, ...args], {
    cwd: coreRoot,
    stdio: 'inherit',
  });
}

export async function setup(): Promise<void> {
  if (process.env.KAFKA_EXTERNAL === '1') {
    return;
  }

  compose(['up', '--wait', '--wait-timeout', '180']);
}

export async function teardown(): Promise<void> {
  if (process.env.KAFKA_EXTERNAL === '1' || process.env.DO_NOT_STOP === '1') {
    return;
  }

  compose(['down', '--remove-orphans']);
}
