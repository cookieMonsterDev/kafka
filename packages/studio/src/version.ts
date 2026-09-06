import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const OWN_PACKAGE_NAME = '@cookiemonsterdev/kafka-studio';
const MAX_UPWARD_SEARCH = 6;

/**
 * Finds this package's own `package.json` by walking up from the running module's URL, checked
 * by `name` rather than a fixed relative depth — the depth from this file to the package root
 * differs between running against `src/` (tests) and a single bundled `dist/index.js` file.
 */
export function findOwnPackageJson(moduleUrl: string): string {
  let dir = dirname(fileURLToPath(moduleUrl));
  for (let i = 0; i < MAX_UPWARD_SEARCH; i++) {
    const candidate = join(dir, 'package.json');
    if (existsSync(candidate)) {
      const pkg = JSON.parse(readFileSync(candidate, 'utf8')) as { name?: string };
      if (pkg.name === OWN_PACKAGE_NAME) return candidate;
    }
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(`could not locate ${OWN_PACKAGE_NAME}'s package.json above ${moduleUrl}`);
}

export function readOwnVersion(moduleUrl: string): string {
  const packageJsonPath = findOwnPackageJson(moduleUrl);
  const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf8')) as { version: string };
  return packageJson.version;
}
