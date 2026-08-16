import { createDescribeGroupsRequestWithAuthorizedOps } from '../v3/request';

/**
 * DescribeGroups Request (Version: 4) — same wire as v3 (`includeAuthorizedOperations`).
 * The response adds `group.instance.id` on members.
 */
export const describeGroupsRequestV4 = createDescribeGroupsRequestWithAuthorizedOps(4);
