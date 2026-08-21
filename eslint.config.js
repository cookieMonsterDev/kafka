import js from '@eslint/js';
import vitest from '@vitest/eslint-plugin';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  {
    ignores: [
      '**/dist/**',
      '**/build/**',
      '**/coverage/**',
      '**/.astro/**',
      '**/node_modules/**',
      'packages/docs/**',
      'scripts/**',
    ],
  },
  js.configs.recommended,
  {
    files: ['packages/core/{src,test}/**/*.ts'],
    extends: [...tseslint.configs.recommendedTypeChecked],
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/require-await': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
    },
  },
  {
    files: ['packages/core/src/protocol/**/*.ts'],
    ignores: ['**/*.test.ts'],
    rules: {
      '@typescript-eslint/no-non-null-assertion': 'error',
    },
  },
  {
    files: ['**/*.test.ts', 'packages/core/test/**/*.ts'],
    plugins: { vitest },
    rules: {
      ...vitest.configs.recommended.rules,
      'vitest/no-focused-tests': 'error',
      'vitest/no-disabled-tests': 'warn',
      'vitest/no-commented-out-tests': 'error',
      'vitest/expect-expect': ['error', { assertFunctionNames: ['expect', 'expectTypeOf'] }],
      'vitest/no-standalone-expect': [
        'error',
        {
          additionalTestBlockFunctions: [
            'testIfKafkaAtMost_0_10',
            'testIfKafkaAtMost_0_11',
            'testIfKafkaAtMost_1_1',
            'testIfKafkaAtMost_3_6',
            'testIfKafkaAtLeast_0_11',
            'testIfKafkaAtLeast_1_0',
            'testIfKafkaAtLeast_1_1',
            'testIfKafkaEquals_0_10',
            'testIfKafkaEquals_0_11',
            'testIfKafkaEquals_1_1',
            'testIfKafkaEquals_2_4',
            'testIfKafkaEquals_3_6',
            'testIfOauthbearerDisabled',
            'testIfKafkaAtLeast_2_1',
            'testIfKafkaAtLeast_2_2',
            'testIfKafkaAtLeast_2_4',
            'testIfKafkaAtLeast_2_8',
            'testIfKafkaAtLeast_3_0',
            'testIfKafkaAtLeast_3_6',
            'testIfKafkaAtLeast_4_0',
            'testIfKafkaAtLeast_4_1',
            'testIfKafkaAtLeast_4_2',
            'testIfKafkaAtLeast_4_3',
          ],
        },
      ],
      '@typescript-eslint/no-unsafe-return': 'off',
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/consistent-type-imports': [
        'error',
        { prefer: 'type-imports', fixStyle: 'inline-type-imports' },
      ],
    },
  },
);
