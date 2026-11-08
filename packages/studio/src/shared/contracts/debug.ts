import { z } from 'zod';
import { topicNameSchema } from './topic';
import type { MessageRecord } from './message';

export const predicateSchema = z.object({
  field: z
    .string()
    .min(1)
    .max(256)
    .regex(/^(?:key|value(?:\.[\w-]+)*|headers\.[\w.-]+|partition|offset|timestamp)$/),
  operator: z.enum(['eq', 'ne', 'contains', 'gt', 'gte', 'lt', 'lte']),
  value: z.union([z.string().max(4096), z.number().finite(), z.boolean(), z.null()]),
});
export type MessagePredicate = z.infer<typeof predicateSchema>;
export const scanRequestSchema = z
  .object({
    topics: z
      .array(topicNameSchema)
      .min(1)
      .max(8)
      .refine((topics) => new Set(topics).size === topics.length, 'duplicate topics'),
    predicates: z.array(predicateSchema).max(16).default([]),
    startTime: z.number().int().nonnegative().optional(),
    endTime: z.number().int().nonnegative().optional(),
    maxRecords: z.number().int().min(1).max(10000).default(1000),
    maxResults: z.number().int().min(1).max(1000).default(200),
  })
  .refine(
    (input) => input.startTime === undefined || input.endTime === undefined || input.startTime <= input.endTime,
    'start time must precede end time',
  );
export type ScanRequest = z.infer<typeof scanRequestSchema>;
export interface DebugRecord extends MessageRecord {
  readonly topic: string;
  readonly headerValues?: Readonly<Record<string, string | null | readonly (string | null)[]>>;
}
export interface ScanCoverage {
  readonly topic: string;
  readonly partition: number;
  readonly start: string;
  readonly end: string;
  readonly next: string;
  readonly complete: boolean;
}
export interface ScanResult {
  readonly records: readonly DebugRecord[];
  readonly scanned: number;
  readonly matched: number;
  readonly complete: boolean;
  readonly reason: 'complete' | 'record-limit' | 'result-limit' | 'byte-limit' | 'timeout' | 'cancelled';
  readonly coverage: readonly ScanCoverage[];
}
const base64 = z
  .string()
  .max(1000000)
  .regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/, 'invalid base64');
export const rawRecordSchema = z.object({
  key: base64.nullable(),
  value: base64.nullable(),
  headers: z.record(z.string().min(1), z.union([base64.nullable(), z.array(base64.nullable()).max(100)])).default({}),
});
export const replayRequestSchema = z.object({
  topic: topicNameSchema,
  records: z.array(rawRecordSchema).min(1).max(100),
  ratePerSecond: z.number().int().min(1).max(100).default(10),
});
export interface ReplayResult {
  readonly index: number;
  readonly status: 'sent' | 'failed';
  readonly error?: string;
}
export const scenarioSchema = z.object({
  version: z.literal(1),
  name: z.string().min(1).max(100),
  steps: z
    .array(
      z.discriminatedUnion('kind', [
        z.object({
          kind: z.literal('produce'),
          topic: topicNameSchema,
          key: z.string().optional(),
          value: z.string().nullable(),
          headers: z.record(z.string(), z.string()).optional(),
        }),
        z.object({
          kind: z.literal('expect'),
          topic: topicNameSchema,
          predicates: z.array(predicateSchema).min(1).max(16),
          timeoutMs: z.number().int().min(100).max(30000).default(10000),
        }),
      ]),
    )
    .min(1)
    .max(20),
});
export type DebugScenario = z.infer<typeof scenarioSchema>;
export interface ScenarioStepResult {
  readonly index: number;
  readonly status: 'passed' | 'failed';
  readonly message: string;
}
export interface ConfigDifference {
  readonly field: string;
  readonly left: string | null;
  readonly right: string | null;
}
