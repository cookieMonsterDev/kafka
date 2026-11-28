import { describe, expect, it } from 'vitest';
import { apiVersionStatus, countApiVersionStatuses } from './api-version-status';

const usable = { brokerVersions: { minVersion: 0, maxVersion: 13 }, isImplemented: true, version: 13 };
const notImplemented = { brokerVersions: { minVersion: 0, maxVersion: 1 }, isImplemented: false, version: null };
const unsupported = { brokerVersions: null, isImplemented: true, version: null };
const noCommonVersion = { brokerVersions: { minVersion: 50, maxVersion: 60 }, isImplemented: true, version: null };

describe('apiVersionStatus', () => {
  it('tells which side lacks the API, or whether the two share a version', () => {
    expect(apiVersionStatus(usable)).toBe('usable');
    expect(apiVersionStatus(notImplemented)).toBe('not-implemented');
    expect(apiVersionStatus(unsupported)).toBe('unsupported');
    expect(apiVersionStatus(noCommonVersion)).toBe('no-common-version');
  });
});

describe('countApiVersionStatuses', () => {
  it('counts every status, including the ones no API has', () => {
    expect(countApiVersionStatuses([usable, usable, notImplemented, unsupported])).toEqual({
      usable: 2,
      'not-implemented': 1,
      unsupported: 1,
      'no-common-version': 0,
    });
  });
});
