/**
 * Subprocess driver for the duplicate-instances test (D18a / Risk #15). Runs in a fresh process
 * for the same reason `run-load-sync.mjs` does: `installConfigTransformHooks()` calls
 * `module.registerHooks`, which has no `deregister` on this Node version, so calling it here must
 * never leak into the shared vitest worker.
 *
 * Usage: `node run-duplicate-instances.mjs <copyAIndexPath> <copyBIndexPath> <ladderFixturePath>
 * <invalidJsonFixturePath> <enumFixturePath>`. Prints one JSON line to stdout.
 */
import { pathToFileURL } from 'node:url';

const [, , copyAPath, copyBPath, ladderFixturePath, invalidJsonFixturePath, enumFixturePath] = process.argv;
if (enumFixturePath == null) {
  throw new Error(
    'Usage: run-duplicate-instances.mjs <copyAIndexPath> <copyBIndexPath> <ladderFixturePath> ' +
      '<invalidJsonFixturePath> <enumFixturePath>',
  );
}

const copyA = await import(pathToFileURL(copyAPath).href);
const copyB = await import(pathToFileURL(copyBPath).href);

const result = {};

// Two distinct module instances, loaded from two distinct resolved paths — the premise the rest
// of this driver tests against.
result.copiesAreDistinctModuleInstances = copyA.KafkaConfigError !== copyB.KafkaConfigError;

// The same config file resolves to deep-equal results from both copies.
const configFromA = copyA.loadConfigFileSync(ladderFixturePath);
const configFromB = copyB.loadConfigFileSync(ladderFixturePath);
result.deepEqualAcrossCopies = JSON.stringify(configFromA) === JSON.stringify(configFromB);

// A KafkaConfigError thrown by copy A is recognised by copy B via `.name` — never `instanceof`,
// which would fail here precisely because the two classes are distinct (previous assertion).
let errorFromA;
try {
  copyA.loadConfigFileSync(invalidJsonFixturePath);
} catch (error) {
  errorFromA = error;
}
result.errorFromAHasKafkaConfigErrorName = errorFromA?.name === 'KafkaConfigError';
result.errorFromAIsNotInstanceOfCopyBClass = !(errorFromA instanceof copyB.KafkaConfigError);
result.copyBOwnErrorHasSameName = errorFromA?.name === new copyB.KafkaConfigError('ConfigLoadError', 'x').name;

// Installing the transform hooks from copy A does not break copy B: `registerHooks` is a
// process-global Node API, so a rescuable fixture loads through either copy once installed.
copyA.installConfigTransformHooks();
try {
  result.rescuedConfig = copyB.loadConfigFileSync(enumFixturePath);
  result.copyBLoadsRescuableFixtureAfterCopyAInstalledHooks = true;
} catch (error) {
  result.copyBLoadsRescuableFixtureAfterCopyAInstalledHooks = false;
  result.rescueError = error instanceof Error ? error.message : String(error);
}

console.log(JSON.stringify(result));
