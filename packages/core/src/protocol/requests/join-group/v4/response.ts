import { KafkaJSMemberIdRequired } from '../../../../errors.js';
import { createErrorFromCode, ERROR_CODES, failure } from '../../../error-codes.js';
import type { ResponseDefinition } from '../../../schema.js';
import { joinGroupResponseV3, type JoinGroupResponseV3Body } from '../v3/response.js';

const MEMBER_ID_REQUIRED_ERROR_CODE = ERROR_CODES.find((e) => e.type === 'MEMBER_ID_REQUIRED')?.code;

/**
 * JoinGroup Response (Version: 4) — wire format identical to v3; only `parse` changes, giving
 * the caller `memberId` back on `KafkaJSMemberIdRequired` so it can retry the second-phase join
 * (see the v4 request note about the two-phase join with an assigned id).
 */
export const joinGroupResponseV4: ResponseDefinition<JoinGroupResponseV3Body> = {
  decode: (rawData) => joinGroupResponseV3.decode(rawData),
  parse: async (data) => {
    if (failure(data.errorCode)) {
      if (data.errorCode === MEMBER_ID_REQUIRED_ERROR_CODE) {
        throw new KafkaJSMemberIdRequired(createErrorFromCode(data.errorCode), { memberId: data.memberId });
      }
      throw createErrorFromCode(data.errorCode);
    }
    return data;
  },
};
