import js from '@eslint/js'
import vitest from '@vitest/eslint-plugin'
import tseslint from 'typescript-eslint'

export default tseslint.config(
  {
    ignores: ['**/dist/**', '**/build/**', '**/coverage/**', '**/.astro/**', '**/node_modules/**', 'packages/docs/**'],
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
      // noUncheckedIndexedAccess makes the protocol layer verbose; ban `!` there instead of
      // repo-wide so decode helpers stay honest about optional index access (see PLAN.md §10).
      '@typescript-eslint/no-non-null-assertion': 'off',
    },
  },
  {
    files: ['packages/core/src/protocol/**/*.ts'],
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
    },
  }
)
