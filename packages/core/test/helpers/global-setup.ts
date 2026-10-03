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

// A broker occasionally never turns healthy on a shared CI runner even though the same stack
// starts fine on the next attempt. Dump what each container was doing (otherwise the failure is
// just "application not healthy"), then retry once from a clean stack before giving up.
function composeUpWithRetry(attempts = 2): void {
  for (let attempt = 1; ; attempt++) {
    try {
      compose(['up', '--wait', '--wait-timeout', '180']);
      return;
    } catch (error) {
      try {
        compose(['ps', '--all']);
        compose(['logs', '--no-color', '--tail', '200']);
      } catch {
        // Diagnostics are best-effort; the original failure is what matters.
      }
      if (attempt >= attempts) {
        throw error;
      }
      console.warn(`docker compose up failed (attempt ${attempt}/${attempts}); recreating the stack and retrying`);
      compose(['down', '--remove-orphans', '--volumes']);
    }
  }
}

export async function setup(): Promise<void> {
  if (process.env.KAFKA_EXTERNAL === '1') {
    return;
  }

  composeUpWithRetry();

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
