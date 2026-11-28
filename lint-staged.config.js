export default {
  '*.{ts,tsx,js,jsx,mjs,cjs}': ['oxlint --fix --no-error-on-unmatched-pattern', 'prettier --write'],
  '*.{json,md,yml,yaml,css,html}': ['prettier --write'],
  'packages/*/package.json': () => 'node scripts/check-publishable-deps.mjs',
};
