import { describe, expect, it } from 'vitest';
import { matchesPredicates, parseMessageQuery, correlationPredicates } from './message-query';

const message = {
  key: 'order-1',
  value: '{"orderId":"a AND b","total":25}',
  headers: { 'x-source': 'checkout' },
  partition: 2,
  offset: '9007199254740993',
  timestamp: '123',
};
describe('message queries', () => {
  it('handles quoted AND, JSON fields, headers, and numeric comparisons', () => {
    const predicates = parseMessageQuery(
      'value.orderId == "a AND b" AND headers.x-source == "checkout" AND value.total >= 20',
    );
    expect(matchesPredicates(message, predicates)).toBe(true);
  });
  it('compares offsets without losing bigint precision', () => {
    expect(matchesPredicates(message, parseMessageQuery('offset > "9007199254740992"'))).toBe(true);
    expect(matchesPredicates(message, parseMessageQuery('offset == "9007199254740992"'))).toBe(false);
  });
  it('rejects executable expressions and incomplete conditions', () => {
    expect(() => parseMessageQuery('value.id == process.exit()')).toThrow();
    expect(() => parseMessageQuery('key == "a" AND ')).toThrow();
    expect(() => parseMessageQuery('value.total > 1e999')).toThrow();
  });
  it('does not traverse prototypes or treat missing fields as null', () => {
    expect(matchesPredicates(message, parseMessageQuery('value.constructor == null'))).toBe(false);
    expect(matchesPredicates(message, parseMessageQuery('value.missing == null'))).toBe(false);
    expect(matchesPredicates({ ...message, value: null }, parseMessageQuery('value == null'))).toBe(true);
  });
});

it('correlates numeric JSON IDs separately from string IDs', () => {
  expect(matchesPredicates({ ...message, value: '{"id":123}' }, correlationPredicates('value.id', '123', 'json'))).toBe(
    true,
  );
  expect(matchesPredicates({ ...message, value: '{"id":123}' }, correlationPredicates('value.id', '123', 'text'))).toBe(
    false,
  );
  expect(matchesPredicates({ ...message, key: '123' }, correlationPredicates('key', '123', 'text'))).toBe(true);
});

it('does not treat inherited header properties as present', () => {
  expect(matchesPredicates(message, parseMessageQuery('headers.toString != null'))).toBe(false);
});
