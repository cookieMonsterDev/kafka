import { fetchClusterStatus } from './api';
import { CONSOLE_COMMANDS, parseConsoleCommand } from './console-commands';
import { getGroup, listGroups } from './groups-api';
import { listAcls, listQuotas, listTransactions } from './settings-api';
import { getTopic, listTopics } from './topics-api';

/** Dispatches only supported reads through Studio's authenticated API clients. */
export async function runConsoleCommand(line: string): Promise<unknown> {
  const { command, resource, help } = parseConsoleCommand(line);
  if (help) return `Usage: ${command.usage}\n${command.description}`;
  switch (command.path) {
    case 'help':
      return [
        'Available commands',
        '',
        ...CONSOLE_COMMANDS.map(({ usage, description }) => `${usage.padEnd(44)} ${description}`),
        '',
        'Tab completes · Enter runs · Escape dismisses suggestions · ↑ / ↓ recalls history',
        'Ctrl+L clears output · Shift+Tab leaves the prompt',
      ].join('\n');
    case 'cluster info': {
      const status = await fetchClusterStatus();
      if (!status.connected)
        throw new Error('Not connected to Kafka. Check the active connection profile and broker availability.');
      return status;
    }
    case 'topic list':
      return listTopics();
    case 'topic describe':
      return getTopic(resource ?? '');
    case 'topic offsets': {
      const topic = await getTopic(resource ?? '');
      return {
        topic: topic.name,
        partitions: topic.partitions.map(({ partitionIndex, earliestOffset, latestOffset }) => ({
          partition: partitionIndex,
          earliestOffset,
          latestOffset,
        })),
      };
    }
    case 'config describe': {
      const topic = await getTopic(resource ?? '');
      return { topic: topic.name, configs: topic.configs };
    }
    case 'group list':
      return listGroups();
    case 'group describe':
      return getGroup(resource ?? '');
    case 'group offsets': {
      const group = await getGroup(resource ?? '');
      return { groupId: group.groupId, partitions: group.partitionLag };
    }
    case 'acl list':
      return listAcls();
    case 'quota describe':
      return listQuotas();
    case 'txn list':
      return listTransactions();
    default:
      throw new Error('Unsupported console command.');
  }
}
