import { createErrorFromCode, failure } from '../../error-codes';

export interface OffsetFetchTopicOptions {
  topic: string;
  partitions: { partition: number }[];
}

export interface OffsetFetchResponseShape {
  responses: readonly { partitions: readonly { errorCode: number }[] }[];
}

export function checkOffsetFetchPartitionErrors(data: OffsetFetchResponseShape): void {
  for (const response of data.responses) {
    const partitionWithError = response.partitions.find((partition) => failure(partition.errorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
  }
}
