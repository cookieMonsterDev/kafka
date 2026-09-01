import type { CommandSpec } from '../args/define';
import { adminCallCommand } from './admin/call';
import { adminMethodsCommand } from './admin/methods';
import { doctorCommand } from './meta/doctor';
import { initCommand } from './meta/init';
import { pingCommand } from './meta/ping';
import { profilesCommand } from './meta/profiles';
import { topicCreateCommand } from './topic/create';
import { topicDescribeCommand } from './topic/describe';
import { topicListCommand } from './topic/list';

/** Every command the CLI mounts, in no particular order — the registry sorts them out. */
export const ALL_COMMANDS: CommandSpec[] = [
  pingCommand,
  initCommand,
  doctorCommand,
  profilesCommand,
  topicListCommand,
  topicDescribeCommand,
  topicCreateCommand,
  adminCallCommand,
  adminMethodsCommand,
];
