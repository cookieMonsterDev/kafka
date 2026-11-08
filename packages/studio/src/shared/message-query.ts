import type { MessagePredicate } from './contracts/debug';

function readPath(value: unknown, path: readonly string[]): unknown {
  let current = value;
  for (const part of path) {
    if (part === '__proto__' || part === 'prototype' || part === 'constructor') return undefined;
    if (typeof current !== 'object' || current === null || !Object.hasOwn(current, part)) return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

/** A deliberately small AND query language; values are JSON literals, never executable JavaScript. */
export function parseMessageQuery(query: string): MessagePredicate[] {
  if (query.length > 8192) throw new Error('Query is too long');
  const predicates: MessagePredicate[] = [];
  let rest = query.trim();
  while (rest.length > 0) {
    const match =
      /^(key|value(?:\.[\w-]+)*|headers\.[\w.-]+|partition|offset|timestamp)\s*(==|!=|>=|<=|>|<|contains\b)\s*("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?|true|false|null)(?:\s+AND\s+|\s*$)/.exec(
        rest,
      );
    if (!match) throw new Error('Use field == JSON-value joined with AND (also !=, contains, >, >=, <, <=)');
    const operators: Record<string, MessagePredicate['operator']> = {
      '==': 'eq',
      '!=': 'ne',
      '>': 'gt',
      '>=': 'gte',
      '<': 'lt',
      '<=': 'lte',
      contains: 'contains',
    };
    const field = match[1];
    const operator = operators[match[2] ?? ''];
    if (field === undefined || operator === undefined) throw new Error('Invalid predicate');
    const value: unknown = JSON.parse(match[3] ?? 'null');
    if (typeof value === 'number' && !Number.isFinite(value)) throw new Error('Query number must be finite');
    predicates.push({ field, operator, value: value as MessagePredicate['value'] });
    if (predicates.length > 16) throw new Error('Use at most 16 predicates');
    rest = rest.slice(match[0].length);
    if (rest.length === 0 && /AND\s+$/.test(match[0])) throw new Error('Expected predicate after AND');
  }
  return predicates;
}

export interface SearchableMessage {
  readonly key: string | null;
  readonly value: string | null;
  readonly headers: Readonly<Record<string, string | null>>;
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: string;
}

export function matchesPredicates(message: SearchableMessage, predicates: readonly MessagePredicate[]): boolean {
  return predicates.every((predicate) => {
    const [root, ...path] = predicate.field.split('.');
    let actual: unknown;
    if (root === 'value' && path.length > 0) {
      try {
        actual = readPath(JSON.parse(message.value ?? 'null') as unknown, path);
      } catch {
        return false;
      }
    } else if (root === 'headers') {
      const header = path.join('.');
      actual = Object.hasOwn(message.headers, header) ? message.headers[header] : undefined;
    } else if (root !== undefined && Object.hasOwn(message, root)) {
      actual = message[root as keyof SearchableMessage];
    }
    const expected = predicate.value;
    if ((root === 'offset' || root === 'timestamp') && path.length === 0) {
      if (typeof expected !== 'string' || !/^\d+$/.test(expected) || typeof actual !== 'string') return false;
      actual = BigInt(actual);
      return compare(actual, BigInt(expected), predicate.operator);
    }
    return compare(actual, expected, predicate.operator);
  });
}

function compare(actual: unknown, expected: unknown, operator: MessagePredicate['operator']): boolean {
  if (operator === 'eq') return actual === expected;
  if (operator === 'ne') return actual !== undefined && actual !== expected;
  if (operator === 'contains')
    return typeof actual === 'string' && typeof expected === 'string' && actual.includes(expected);
  if (
    (typeof actual === 'number' && typeof expected === 'number') ||
    (typeof actual === 'bigint' && typeof expected === 'bigint')
  ) {
    if (operator === 'gt') return actual > expected;
    if (operator === 'gte') return actual >= expected;
    if (operator === 'lt') return actual < expected;
    return actual <= expected;
  }
  return false;
}

export function correlationPredicates(field: string, value: string, type: 'text' | 'json'): MessagePredicate[] {
  const literal: unknown = type === 'json' ? JSON.parse(value) : value;
  return parseMessageQuery(`${field} == ${JSON.stringify(literal)}`);
}
