export default {
  extends: ['@commitlint/config-conventional'],
  helpUrl: 'https://www.conventionalcommits.org/en/v1.0.0/',
  // Dependabot generates grouped-update subjects that exceed the header limit.
  ignores: [
    (message) =>
      /^build\(deps(?:-dev)?\): bump the npm-(?:production|development) group across \d+ director(?:y|ies) with \d+ updates$/.test(
        message.split('\n')[0],
      ),
  ],
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
