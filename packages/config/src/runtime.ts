/** True under Bun, whose `require()` and `import()` load TypeScript natively (no `node:module` hooks needed). */
export function isBun(): boolean {
  return typeof process.versions.bun === 'string';
}
