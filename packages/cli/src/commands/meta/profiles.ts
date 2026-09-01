import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const profilesCommand: CommandSpec = {
  path: ['profiles'],
  summary: 'List the named connection profiles configured under cli.profiles',
  examples: ['profiles'],
  exitCodes: [EXIT_CODES.ok],
  async run({ output, config }) {
    const profiles = config.cli.profiles ?? {};
    const names = Object.keys(profiles);

    // Secrets are unlikely in a profile (it's usually just an alternate broker list), but a
    // profile is a plain KafkaConfig-shaped object with no restriction on what it holds — every
    // other output boundary in this CLI redacts credential fields before printing, and this one
    // is no exception, so this is the one place `profiles` needs core at all.
    const { redactKafkaConfig } = await import('@cookiemonsterdev/kafka-core');
    const redacted = redactKafkaConfig(profiles) as Record<string, { brokers?: readonly string[] }>;

    output.write({
      human: () =>
        names.length === 0
          ? '(no profiles configured — add a "cli.profiles" section to your kafka.config file)'
          : renderTable(
              ['PROFILE', 'BROKERS'],
              names.map((name) => [
                name === config.profile ? `${name} (active)` : name,
                (redacted[name]?.brokers ?? []).join(',') || '(none)',
              ]),
            ),
      json: () => stringifyJsonSafe({ active: config.profile, profiles: redacted }),
    });
    return EXIT_CODES.ok;
  },
};
