import { z } from 'zod';
import { topicNameSchema } from './topic';

export const produceHeadersSchema = z.record(z.string().min(1), z.string());

/** One record to produce. `value: null` is Kafka's tombstone — an explicit delete marker, not "no value". */
export const produceMessageSchema = z.object({
  key: z.string().optional(),
  value: z.string().nullable(),
  partition: z.number().int().nonnegative().optional(),
  headers: produceHeadersSchema.optional(),
  timestamp: z.number().int().nonnegative().optional(),
});
export type ProduceMessage = z.infer<typeof produceMessageSchema>;

export const produceRequestSchema = z.object({
  topic: topicNameSchema,
  messages: z.array(produceMessageSchema).min(1).max(1000),
  acks: z.union([z.literal(-1), z.literal(0), z.literal(1)]).optional(),
});
export type ProduceRequest = z.infer<typeof produceRequestSchema>;

export interface ProduceResultEntry {
  readonly partition: number;
  /** The partition's base offset for this write. `bigint` on the server; a decimal string over the wire. */
  readonly offset: string;
}

export interface ProduceResponse {
  readonly results: readonly ProduceResultEntry[];
}

const MAX_BURST_COUNT = 100_000;
const MAX_BURST_RATE_PER_SECOND = 1000;
/** Applied when the caller omits `ratePerSecond` — a burst is rate-limited by default, not only when asked for. */
const DEFAULT_BURST_RATE_PER_SECOND = 200;

/** `{{seq}}` in `template.key`/`template.value` is substituted with the 0-based message index as the burst runs. */
export const burstRequestSchema = z.object({
  topic: topicNameSchema,
  template: produceMessageSchema,
  count: z.number().int().positive().max(MAX_BURST_COUNT),
  ratePerSecond: z.number().int().positive().max(MAX_BURST_RATE_PER_SECOND).default(DEFAULT_BURST_RATE_PER_SECOND),
});
/** What the server sees after parsing — `ratePerSecond` always present (the default has been applied). */
export type BurstRequest = z.infer<typeof burstRequestSchema>;
/** What a caller may send — `ratePerSecond` optional; the server fills in {@link DEFAULT_BURST_RATE_PER_SECOND}. */
export type BurstRequestInput = z.input<typeof burstRequestSchema>;

export interface BurstStartResponse {
  readonly jobId: string;
}

export type BurstJobStatus = 'running' | 'completed' | 'cancelled' | 'failed';

export interface BurstProgress {
  readonly jobId: string;
  readonly sent: number;
  readonly total: number;
  readonly status: BurstJobStatus;
  readonly error?: string;
}
