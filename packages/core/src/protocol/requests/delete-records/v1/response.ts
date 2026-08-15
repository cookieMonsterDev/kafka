import type { ResponseDefinition } from '../../../schema.js';
import type { DeleteRecordsTopic } from '../v0/request.js';
import { type DeleteRecordsResponseV0Body, deleteRecordsResponseV0 } from '../v0/response.js';

export interface DeleteRecordsResponseV1Body extends DeleteRecordsResponseV0Body {
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export function deleteRecordsResponseV1(requestOptions: {
  topics: readonly DeleteRecordsTopic[];
}): ResponseDefinition<DeleteRecordsResponseV1Body> {
  const v0 = deleteRecordsResponseV0(requestOptions);
  return {
    decode: async (rawData) => {
      const decoded = await v0.decode(rawData);
      return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
    },
    parse: async (data) => {
      await v0.parse(data);
      return data;
    },
  };
}
