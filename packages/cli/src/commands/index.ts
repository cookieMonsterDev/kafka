import type { CommandSpec } from '../args/define';
import { pingCommand } from './meta/ping';
import { topicDescribeCommand } from './topic/describe';
import { topicListCommand } from './topic/list';

/** Every command the CLI mounts, in no particular order — the registry sorts them out. */
export const ALL_COMMANDS: CommandSpec[] = [pingCommand, topicListCommand, topicDescribeCommand];
