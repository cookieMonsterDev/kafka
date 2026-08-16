import { describe, expect, it } from 'vitest';
import {
  AclOperationTypes,
  AclPermissionTypes,
  AclResourceTypes,
  AssignerProtocol,
  CompressionCodecs,
  CompressionTypes,
  ConfigOperations,
  ConfigResourceTypes,
  ConfigSource,
  Kafka,
  KafkaError,
  KafkaNonRetriableError,
  PartitionAssigners,
  Partitioners,
  ResourcePatternTypes,
  ScramMechanisms,
  logLevel,
} from './index';

describe('public surface', () => {
  it('exports the Kafka client', () => {
    expect(Kafka).toBeTypeOf('function');
    const kafka = new Kafka({
      brokers: ['localhost:9092'],
      logLevel: logLevel.NOTHING,
      logCreator: () => () => {},
    });
    expect(kafka).toBeInstanceOf(Kafka);
  });

  it('exports partitioners, assigners, and the assigner protocol', () => {
    expect(Partitioners.DefaultPartitioner).toBeTypeOf('function');
    expect(Partitioners.LegacyPartitioner).toBeTypeOf('function');
    expect(Partitioners.JavaCompatiblePartitioner).toBe(Partitioners.DefaultPartitioner);
    expect(PartitionAssigners.roundRobin).toBeTypeOf('function');
    expect(PartitionAssigners.range).toBeTypeOf('function');
    expect(PartitionAssigners.sticky).toBeTypeOf('function');
    expect(PartitionAssigners.cooperativeSticky).toBeTypeOf('function');
    expect(AssignerProtocol.MemberMetadata.encode).toBeTypeOf('function');
    expect(AssignerProtocol.MemberAssignment.decode).toBeTypeOf('function');
  });

  it('exports log levels, compression, and protocol enums as frozen maps', () => {
    expect(logLevel.INFO).toBe(4);
    expect(CompressionTypes.GZIP).toBe(1);
    expect(CompressionTypes.ZSTD).toBe(4);
    expect(CompressionCodecs[CompressionTypes.GZIP]).toBeTypeOf('function');
    expect(ConfigResourceTypes.TOPIC).toBe(2);
    expect(ConfigOperations.SET).toBe(0);
    expect(ConfigOperations.DELETE).toBe(1);
    expect(ConfigSource.DYNAMIC_BROKER_CONFIG).toBe(2);
    expect(AclResourceTypes.TOPIC).toBe(2);
    expect(AclOperationTypes.READ).toBe(3);
    expect(AclPermissionTypes.ALLOW).toBe(3);
    expect(ResourcePatternTypes.LITERAL).toBe(3);
    expect(ScramMechanisms.SCRAM_SHA_256).toBe(1);
    expect(ScramMechanisms.SCRAM_SHA_512).toBe(2);
  });

  it('exports the error classes, not the internal helpers', () => {
    expect(new KafkaError('x')).toBeInstanceOf(Error);
    expect(new KafkaNonRetriableError('x').retriable).toBe(false);
  });
});
