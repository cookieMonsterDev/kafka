import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { compactTopicNames, heartbeatTopicPartitionsSchema, nullableHeartbeatTopicPartitions } from './shared';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/consumer-group-heartbeat/shared', () => {
  it('round-trips assigned topic partitions', () => {
    const value = { topicId, partitions: [0, 3, 7] };
    const encoder = new Encoder();
    heartbeatTopicPartitionsSchema.write(encoder, value);
    expect(heartbeatTopicPartitionsSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });

  it('round-trips a null topic-partition list and an empty list', () => {
    const nullEncoder = new Encoder();
    nullableHeartbeatTopicPartitions.write(nullEncoder, null);
    expect(nullableHeartbeatTopicPartitions.read(new Decoder(nullEncoder.buffer))).toBeNull();

    const emptyEncoder = new Encoder();
    nullableHeartbeatTopicPartitions.write(emptyEncoder, []);
    expect(nullableHeartbeatTopicPartitions.read(new Decoder(emptyEncoder.buffer))).toEqual([]);
  });

  it('round-trips compact topic names including null', () => {
    const namesEncoder = new Encoder();
    compactTopicNames.write(namesEncoder, ['orders', 'payments']);
    expect(compactTopicNames.read(new Decoder(namesEncoder.buffer))).toEqual(['orders', 'payments']);

    const nullEncoder = new Encoder();
    compactTopicNames.write(nullEncoder, null);
    expect(compactTopicNames.read(new Decoder(nullEncoder.buffer))).toBeNull();
  });
});
