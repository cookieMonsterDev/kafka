import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { isZooKeeperComposeFile, resolveComposeFile } from './kafka-version';

const helpersDir = path.dirname(fileURLToPath(import.meta.url));
const coreRoot = path.resolve(helpersDir, '../..');
const composeFile = resolveComposeFile();

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

  if (isZooKeeperComposeFile(composeFile)) {
    execFileSync('bash', [path.join(coreRoot, 'scripts/create-scram-credentials.sh')], {
      cwd: coreRoot,
      stdio: 'inherit',
    });
  }
}

export async function teardown(): Promise<void> {
  if (process.env.KAFKA_EXTERNAL === '1' || process.env.DO_NOT_STOP === '1') {
    return;
  }

  compose(['down', '--remove-orphans']);
}
