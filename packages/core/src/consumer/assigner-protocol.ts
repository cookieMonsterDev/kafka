import { Decoder } from '../protocol/decoder';
import { Encoder } from '../protocol/encoder';
import type { MemberAssignment as MemberAssignmentMap } from './types';

export interface EncodedMemberMetadata {
  version: number;
  topics: readonly string[];
  userData?: Buffer;
}

export interface DecodedMemberMetadata {
  version: number;
  topics: string[];
  userData: Buffer;
}

/**
 * Encode/decode JoinGroup member metadata and SyncGroup assignments.
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const MemberMetadata = Object.freeze({
  encode({ version, topics, userData = Buffer.alloc(0) }: EncodedMemberMetadata): Buffer {
    return new Encoder().writeInt16(version).writeArray(topics).writeBytes(userData).buffer;
  },

  decode(buffer: Buffer): DecodedMemberMetadata {
    const decoder = new Decoder(buffer);
    return {
      version: decoder.readInt16(),
      topics: decoder.readArray((d) => {
        const topic = d.readString();
        if (topic === null) throw new RangeError('Expected a non-null topic name, got null');
        return topic;
      }),
      userData: decoder.readBytes() ?? Buffer.alloc(0),
    };
  },
});

export interface EncodedMemberAssignment {
  version: number;
  assignment: MemberAssignmentMap;
  userData?: Buffer;
}

export interface DecodedMemberAssignment {
  version: number;
  assignment: MemberAssignmentMap;
  userData: Buffer;
}

export const MemberAssignment = Object.freeze({
  encode({ version, assignment, userData = Buffer.alloc(0) }: EncodedMemberAssignment): Buffer {
    return new Encoder()
      .writeInt16(version)
      .writeArray(
        Object.keys(assignment).map((topic) => new Encoder().writeString(topic).writeArray(assignment[topic] ?? [])),
      )
      .writeBytes(userData).buffer;
  },

  decode(buffer: Buffer): DecodedMemberAssignment | null {
    const decoder = new Decoder(buffer);
    if (!decoder.canReadInt16()) {
      return null;
    }

    const decodePartitions = (d: Decoder): number => d.readInt32();
    const decodeAssignment = (d: Decoder): { topic: string; partitions: number[] } => {
      const topic = d.readString();
      if (topic === null) throw new RangeError('Expected a non-null topic name, got null');
      return { topic, partitions: d.readArray(decodePartitions) };
    };

    return {
      version: decoder.readInt16(),
      assignment: decoder.readArray(decodeAssignment).reduce<MemberAssignmentMap>((obj, { topic, partitions }) => {
        obj[topic] = partitions;
        return obj;
      }, {}),
      userData: decoder.readBytes() ?? Buffer.alloc(0),
    };
  },
});
