// Minimal validator for the JSON Schema subset used by `results/schema/summary.schema.json`, so the
// published-summary contract has one authoritative definition and the runner needs no dependencies.
// Supported keywords: type (string or array, including "null"), required, properties,
// additionalProperties (boolean), items, enum, const, minimum, $ref to "#/$defs/<name>".

import { readFile } from 'node:fs/promises';

interface SchemaNode {
  type?: string | string[];
  required?: string[];
  properties?: Record<string, SchemaNode>;
  additionalProperties?: boolean;
  items?: SchemaNode;
  enum?: unknown[];
  const?: unknown;
  minimum?: number;
  $ref?: string;
  $defs?: Record<string, SchemaNode>;
}

export const summarySchemaUrl = new URL('../results/schema/summary.schema.json', import.meta.url);

function typeOf(value: unknown): string {
  if (value === null) {
    return 'null';
  }
  if (Array.isArray(value)) {
    return 'array';
  }
  if (typeof value === 'number' && Number.isInteger(value)) {
    return 'integer';
  }
  return typeof value;
}

function matchesType(value: unknown, expected: string): boolean {
  const actual = typeOf(value);
  if (expected === 'number') {
    return actual === 'number' || actual === 'integer';
  }
  return actual === expected;
}

function validateNode(value: unknown, node: SchemaNode, root: SchemaNode, path: string, errors: string[]): void {
  if (node.$ref) {
    const name = node.$ref.replace('#/$defs/', '');
    const target = root.$defs?.[name];
    if (!target) {
      errors.push(`${path}: unknown schema reference ${node.$ref}`);
      return;
    }
    validateNode(value, target, root, path, errors);
    return;
  }
  if (node.type !== undefined) {
    const expected = Array.isArray(node.type) ? node.type : [node.type];
    if (!expected.some((type) => matchesType(value, type))) {
      errors.push(`${path}: expected ${expected.join(' or ')}, got ${typeOf(value)}`);
      return;
    }
  }
  if (node.const !== undefined && value !== node.const) {
    errors.push(`${path}: expected ${JSON.stringify(node.const)}`);
  }
  if (node.enum && !node.enum.includes(value)) {
    errors.push(`${path}: ${JSON.stringify(value)} is not one of ${JSON.stringify(node.enum)}`);
  }
  if (node.minimum !== undefined && typeof value === 'number' && value < node.minimum) {
    errors.push(`${path}: ${value} is below the minimum ${node.minimum}`);
  }
  if (Array.isArray(value)) {
    if (node.items) {
      value.forEach((item, index) => validateNode(item, node.items!, root, `${path}[${index}]`, errors));
    }
    return;
  }
  if (typeof value !== 'object' || value === null) {
    return;
  }
  const record = value as Record<string, unknown>;
  for (const key of node.required ?? []) {
    if (!(key in record)) {
      errors.push(`${path}: missing required property "${key}"`);
    }
  }
  for (const [key, child] of Object.entries(record)) {
    const childSchema = node.properties?.[key];
    if (childSchema) {
      validateNode(child, childSchema, root, `${path}.${key}`, errors);
    } else if (node.additionalProperties === false) {
      errors.push(`${path}: unexpected property "${key}"`);
    }
  }
}

/** Returns validation errors (empty when valid) for `value` against the given schema document. */
export function validateAgainstSchema(value: unknown, schema: unknown): string[] {
  const errors: string[] = [];
  const root = schema as SchemaNode;
  validateNode(value, root, root, '$', errors);
  return errors;
}

/** Validates a session summary against `results/schema/summary.schema.json`. */
export async function validateSummary(summary: unknown): Promise<string[]> {
  const schema: unknown = JSON.parse(await readFile(summarySchemaUrl, 'utf8'));
  return validateAgainstSchema(summary, schema);
}
