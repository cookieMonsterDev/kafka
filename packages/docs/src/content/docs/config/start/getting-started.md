---
title: Getting started
description: Discover, validate, load, and merge a standalone application config
order: 2
section: start
---

This example uses Node.js 24+ and the asynchronous loader. Config has no Kafka dependency.
Install `@cookiemonsterdev/kafka-config` and create `app.config.json`:

<!-- example: config/app.config.json -->

```json
{ "port": 3000, "debug": false }
```

Save the following as `load-config.mts` in the same directory:

<!-- example: config/load-config.mts -->

```ts
import { discoverConfigFile, loadConfigFileAsync, mergeConfigLayers } from '@cookiemonsterdev/kafka-config';

interface AppConfig extends Record<string, unknown> {
  port: number;
  debug: boolean;
}
function assertValid(value: unknown): asserts value is AppConfig {
  if (
    typeof value !== 'object' ||
    value === null ||
    !('port' in value) ||
    typeof value.port !== 'number' ||
    !Number.isInteger(value.port) ||
    value.port < 1 ||
    value.port > 65535 ||
    !('debug' in value) ||
    typeof value.debug !== 'boolean'
  ) {
    throw new TypeError('Expected port 1–65535 and a boolean debug setting');
  }
}
const path = discoverConfigFile({
  cwd: process.cwd(),
  name: 'app',
  onDiagnostic: (event) => console.error(event.message),
});
const file = path ? await loadConfigFileAsync<AppConfig>(path, { assertValid }) : undefined;
const merged = mergeConfigLayers<AppConfig>(file, { port: 8080, debug: false });
assertValid(merged);
console.log(merged);
```

Run `node load-config.mts`. Expected: `{ port: 3000, debug: false }`. Removing the config
uses the defaults (`8080`, `false`); an invalid file fails validation rather than silently falling back.

## Precedence and diagnostics

The first argument to `mergeConfigLayers` wins. Values such as `false` and `0` survive;
`undefined` means absent. Nested objects are replaced unless you explicitly choose shallow-merge keys.
Discovery chooses the first trusted directory with a candidate; it does not merge parent files.
Diagnostics report ambiguous candidates, while invalid file contents throw.

For JavaScript/TypeScript configs, use ESM-compatible extensions and explicit relative import
extensions. The async loader does not install transform hooks. See [API reference](../../reference/api/).
