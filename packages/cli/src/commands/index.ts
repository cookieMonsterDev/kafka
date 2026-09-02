import type { CommandSpec } from '../args/define';
import { aclAddCommand } from './acl/add';
import { aclListCommand } from './acl/list';
import { aclRemoveCommand } from './acl/remove';
import { adminCallCommand } from './admin/call';
import { adminMethodsCommand } from './admin/methods';
import { clusterFeaturesCommand } from './cluster/features';
import { clusterInfoCommand } from './cluster/info';
import { clusterLogDirsCommand } from './cluster/log-dirs';
import { clusterQuorumCommand } from './cluster/quorum';
import { configDescribeCommand } from './config/describe';
import { configListResourcesCommand } from './config/list-resources';
import { configSetCommand } from './config/set';
import { configUnsetCommand } from './config/unset';
import { groupDeleteCommand } from './group/delete';
import { groupDeleteOffsetsCommand } from './group/delete-offsets';
import { groupDescribeCommand } from './group/describe';
import { groupListCommand } from './group/list';
import { groupOffsetsCommand } from './group/offsets';
import { groupRemoveMembersCommand } from './group/remove-members';
import { groupResetOffsetsCommand } from './group/reset-offsets';
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
  aclListCommand,
  aclAddCommand,
  aclRemoveCommand,
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
  configDescribeCommand,
  configListResourcesCommand,
  configSetCommand,
  configUnsetCommand,
  clusterInfoCommand,
  clusterQuorumCommand,
  clusterFeaturesCommand,
  clusterLogDirsCommand,
  groupListCommand,
  groupDescribeCommand,
  groupOffsetsCommand,
  groupResetOffsetsCommand,
  groupDeleteCommand,
  groupDeleteOffsetsCommand,
  groupRemoveMembersCommand,
  adminCallCommand,
  adminMethodsCommand,
];
