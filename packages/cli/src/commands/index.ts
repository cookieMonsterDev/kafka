import type { CommandSpec } from '../args/define';
import { adminCallCommand } from './admin/call';
import { adminMethodsCommand } from './admin/methods';
import { doctorCommand } from './meta/doctor';
import { initCommand } from './meta/init';
import { pingCommand } from './meta/ping';
import { profilesCommand } from './meta/profiles';
import { topicAddPartitionsCommand } from './topic/add-partitions';
import { topicCreateCommand } from './topic/create';
import { topicDeleteCommand } from './topic/delete';
import { topicDeleteRecordsCommand } from './topic/delete-records';
import { topicDescribeCommand } from './topic/describe';
import { topicListCommand } from './topic/list';
import { topicOffsetsCommand } from './topic/offsets';
import { topicProducersCommand } from './topic/producers';

/** Every command the CLI mounts, in no particular order — the registry sorts them out. */
export const ALL_COMMANDS: CommandSpec[] = [
  pingCommand,
  initCommand,
  doctorCommand,
  profilesCommand,
  topicListCommand,
  topicDescribeCommand,
  topicCreateCommand,
  topicDeleteCommand,
  topicAddPartitionsCommand,
  topicOffsetsCommand,
  topicDeleteRecordsCommand,
  topicProducersCommand,
  adminCallCommand,
  adminMethodsCommand,
];
