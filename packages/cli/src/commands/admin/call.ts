import { readFileSync } from 'node:fs';
import { decodeArgs } from '../../admin/decode-args';
import {
  ADMIN_METHOD_CLASSIFICATION,
  READ_ONLY_ADMIN_METHODS,
  type AdminMethodName,
} from '../../admin/method-classification';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { redactSecrets } from '../../admin/redact';
import { suggestMethodNames } from '../../admin/suggest-method-names';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { CliAbortedError } from '../../errors/aborted-error';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';

function bigintAwareReplacer(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? value.toString() : value;
}

function isAdminMethodName(name: string): name is AdminMethodName {
  return Object.hasOwn(ADMIN_METHOD_CLASSIFICATION, name);
}

function readArgsFile(path: string): unknown {
  let text: string;
  try {
    text = readFileSync(path, 'utf8');
  } catch (error) {
    throw new CliUsageError(
      `could not read --from-file "${path}": ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new CliUsageError(
      `--from-file "${path}" is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

export const adminCallCommand: CommandSpec = {
  path: ['admin', 'call'],
  summary: 'Call any Admin method by name — the escape hatch for everything without its own command',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'from-file', type: 'string', brief: 'path to a JSON file with the method arguments' },
    { name: 'yes', type: 'boolean', brief: 'confirm a non-read-only method' },
    { name: 'force', type: 'boolean', brief: 'confirm a non-read-only method (required alongside --yes)' },
    {
      name: 'show-secrets',
      type: 'boolean',
      brief: 'print credential fields (password, hmac, …) instead of redacting them',
    },
  ],
  positionals: [{ name: 'method', brief: 'Admin method name, e.g. listTopics' }],
  examples: [
    'admin call listTopics --brokers localhost:9092',
    'admin call createAcls --from-file ./acls.json --yes --force',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.abortedOrUnconfirmed],
  unstable: true,
  async run({ flags, positionals, runtime, output }) {
    const method = positionals[0];
    if (method === undefined) {
      throw new CliUsageError('admin call requires a method name');
    }
    if (!isAdminMethodName(method)) {
      const suggestions = suggestMethodNames(method, Object.keys(ADMIN_METHOD_CLASSIFICATION));
      const hint = suggestions.length > 0 ? ` — did you mean: ${suggestions.join(', ')}?` : '';
      throw new CliUsageError(`unknown Admin method "${method}"${hint}`);
    }

    const isReadOnly = READ_ONLY_ADMIN_METHODS.has(method);
    if (!isReadOnly && !(flags.yes === true && flags.force === true)) {
      throw new CliAbortedError(`admin call ${method} is not read-only; pass --yes --force to run it`);
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const args = typeof flags['from-file'] === 'string' ? decodeArgs(readArgsFile(flags['from-file'])) : undefined;

    const admin = await runtime.openAdmin({ brokers });
    try {
      // `admin call`'s whole point is invoking a method whose name and argument shape are only
      // known at runtime — a static Admin method signature can't describe that, which is exactly
      // why this command is marked `unstable` above.
      const call = (admin as unknown as Record<string, (input?: unknown) => Promise<unknown>>)[method];
      if (call === undefined) {
        throw new CliUsageError(`unknown Admin method "${method}"`);
      }
      const rawResult = args === undefined ? await call() : await call(args);
      const result = flags['show-secrets'] === true ? rawResult : redactSecrets(rawResult);
      output.write({
        human: () => JSON.stringify(result, bigintAwareReplacer, 2),
        json: () => stringifyJsonSafe(result),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
