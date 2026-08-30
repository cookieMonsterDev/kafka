import { describe, expect, it } from 'vitest';
import { suggestMethodNames } from './suggest-method-names';

const CANDIDATES = ['listTopics', 'listGroups', 'describeCluster', 'createTopics'];

describe('suggestMethodNames', () => {
  it('suggests candidates containing the input, case-insensitively', () => {
    expect(suggestMethodNames('topics', CANDIDATES)).toEqual(['listTopics', 'createTopics']);
  });

  it('suggests a candidate the input is a superset of', () => {
    expect(suggestMethodNames('listtopicsx', CANDIDATES)).toEqual(['listTopics']);
  });

  it('returns nothing for no plausible match', () => {
    expect(suggestMethodNames('zzz', CANDIDATES)).toEqual([]);
  });

  it('caps the number of suggestions at the given limit', () => {
    expect(suggestMethodNames('list', CANDIDATES, 1)).toEqual(['listTopics']);
  });
});
