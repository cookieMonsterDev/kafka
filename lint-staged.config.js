export default {
  '*.{ts,tsx,js,jsx,mjs,cjs}': ['eslint --fix --no-warn-ignored', 'prettier --write'],
  '*.{json,md,yml,yaml,css,html}': ['prettier --write'],
  'packages/*/package.json': () => 'node scripts/check-publishable-deps.mjs',
};
