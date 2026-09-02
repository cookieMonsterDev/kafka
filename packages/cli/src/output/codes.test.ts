import { describe, expect, it } from 'vitest';
import {
  ACL_OPERATION_TYPES,
  ACL_PERMISSION_TYPES,
  ACL_RESOURCE_TYPES,
  CONFIG_OPERATIONS,
  CONFIG_RESOURCE_TYPES,
  CONFIG_SOURCE,
  describeCode,
  FEATURE_UPDATE_UPGRADE_TYPES,
  formatCode,
  RESOURCE_PATTERN_TYPES,
  SCRAM_MECHANISMS,
} from './codes';

describe('CONFIG_RESOURCE_TYPES', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(CONFIG_RESOURCE_TYPES).toEqual(core.ConfigResourceTypes);
  });
});

describe('CONFIG_SOURCE', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(CONFIG_SOURCE).toEqual(core.ConfigSource);
  });
});

describe('CONFIG_OPERATIONS', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(CONFIG_OPERATIONS).toEqual(core.ConfigOperations);
  });
});

describe('ACL_RESOURCE_TYPES', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(ACL_RESOURCE_TYPES).toEqual(core.AclResourceTypes);
  });
});

describe('ACL_OPERATION_TYPES', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(ACL_OPERATION_TYPES).toEqual(core.AclOperationTypes);
  });
});

describe('ACL_PERMISSION_TYPES', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(ACL_PERMISSION_TYPES).toEqual(core.AclPermissionTypes);
  });
});

describe('RESOURCE_PATTERN_TYPES', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(RESOURCE_PATTERN_TYPES).toEqual(core.ResourcePatternTypes);
  });
});

describe('FEATURE_UPDATE_UPGRADE_TYPES', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(FEATURE_UPDATE_UPGRADE_TYPES).toEqual(core.FeatureUpdateUpgradeTypes);
  });
});

describe('SCRAM_MECHANISMS', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(SCRAM_MECHANISMS).toEqual(core.ScramMechanisms);
  });
});

describe('describeCode', () => {
  it('resolves a known code to its name', () => {
    expect(describeCode(CONFIG_RESOURCE_TYPES, 2)).toEqual({ name: 'TOPIC', code: 2 });
  });

  it('resolves an unknown code to a null name', () => {
    expect(describeCode(CONFIG_RESOURCE_TYPES, 999)).toEqual({ name: null, code: 999 });
  });
});

describe('formatCode', () => {
  it('formats a known code by name', () => {
    expect(formatCode({ name: 'TOPIC', code: 2 })).toBe('TOPIC');
  });

  it('formats an unknown code as UNKNOWN(n)', () => {
    expect(formatCode({ name: null, code: 999 })).toBe('UNKNOWN(999)');
  });
});
