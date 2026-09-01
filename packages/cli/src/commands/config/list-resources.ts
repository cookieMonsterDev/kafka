import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { CONFIG_RESOURCE_TYPES, describeCode, formatCode } from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { resolveConfigResourceType } from './resource-type';

export const configListResourcesCommand: CommandSpec = {
  path: ['config', 'list-resources'],
  summary: 'List every config resource the broker knows about',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'type',
      type: 'string',
      multiple: true,
      brief: 'limit to this resource type (repeatable, default: every type)',
    },
  ],
  examples: ['config list-resources --brokers localhost:9092', 'config list-resources --type topic --type group'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const typeFlags = flags.type as string[] | undefined;
    const resourceTypes = typeFlags?.map(resolveConfigResourceType);

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { resources } = await admin.listConfigResources({ resourceTypes });

      output.write({
        human: () =>
          resources.length === 0
            ? '(no config resources)'
            : renderTable(
                ['RESOURCE_TYPE', 'RESOURCE_NAME'],
                resources.map((resource) => [
                  formatCode(describeCode(CONFIG_RESOURCE_TYPES, resource.resourceType)),
                  resource.resourceName,
                ]),
              ),
        json: () => stringifyJsonSafe({ resources }),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
