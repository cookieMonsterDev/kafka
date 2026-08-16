export default {
  extends: ['@commitlint/config-conventional'],
  helpUrl: 'https://www.conventionalcommits.org/en/v1.0.0/',
  rules: {
    // Subject line <= 72 characters.
    'header-max-length': [2, 'always', 72],
    'subject-full-stop': [2, 'never', '.'],
    'type-enum': [
      2,
      'always',
      ['feat', 'fix', 'docs', 'style', 'refactor', 'perf', 'test', 'build', 'chore', 'ci', 'revert'],
    ],
  },
};
