import { ADMIN_METHOD_CLASSIFICATION, READ_ONLY_ADMIN_METHODS } from '../../admin/method-classification';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const adminMethodsCommand: CommandSpec = {
  path: ['admin', 'methods'],
  summary: 'List every Admin method admin call can reach, and how each is classified',
  examples: ['admin methods'],
  exitCodes: [EXIT_CODES.ok],
  async run({ output }) {
    const rows = Object.entries(ADMIN_METHOD_CLASSIFICATION)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([name, classification]) => ({
        name,
        classification,
        readOnly: READ_ONLY_ADMIN_METHODS.has(name as never),
      }));

    output.write({
      human: () =>
        renderTable(
          ['METHOD', 'STATUS', 'READ-ONLY'],
          rows.map((row) => [row.name, row.classification, row.readOnly ? 'yes' : 'no']),
        ),
      json: () => stringifyJsonSafe({ methods: rows }),
    });
    return EXIT_CODES.ok;
  },
};
