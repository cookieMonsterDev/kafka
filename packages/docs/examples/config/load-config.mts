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
