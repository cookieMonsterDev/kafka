import { z } from 'zod';

/**
 * One decoded record read from a partition. Keys/values are arbitrary bytes, not necessarily
 * UTF-8, so they cross the wire as base64 — `web/lib/decode.ts` renders them per the viewer's
 * choice of decoder. `offset`/`timestamp` are `bigint` on the server; decimal strings over the wire.
 */
export interface MessageRecord {
  readonly partition: number;
  readonly offset: string;
  /** Unix epoch milliseconds, as a decimal string. */
  readonly timestamp: string;
  readonly key: string | null;
  readonly value: string | null;
  /** A duplicate header key keeps only its last value — Kafka allows repeats, the UI does not need them. */
  readonly headers: Readonly<Record<string, string | null>>;
  readonly size: number;
}

export interface PartitionRange {
  readonly partition: number;
  readonly low: string;
  readonly high: string;
}

export interface MessagesPageResponse {
  readonly messages: readonly MessageRecord[];
  readonly ranges: readonly PartitionRange[];
}

const decimalOffsetSchema = z.string().regex(/^\d+$/, 'must be a non-negative integer');

export const DEFAULT_MESSAGES_LIMIT = 100;
export const MAX_MESSAGES_LIMIT = 1000;

/**
 * `GET /api/topics/:name/messages` query params. `from` and `timestamp` both seek a starting
 * position and are mutually exclusive; omitting both reads the most recent `limit` messages
 * (a "tail" read) — the default a message browser opens on.
 */
export const messagesQuerySchema = z
  .object({
    partition: z.coerce.number().int().nonnegative().optional(),
    from: z.union([z.literal('earliest'), z.literal('latest'), decimalOffsetSchema]).optional(),
    timestamp: z.coerce.number().int().optional(),
    limit: z.coerce.number().int().positive().max(MAX_MESSAGES_LIMIT).default(DEFAULT_MESSAGES_LIMIT),
  })
  .refine((value) => value.from === undefined || value.timestamp === undefined, {
    message: '"from" and "timestamp" are mutually exclusive',
  });
export type MessagesQuery = z.infer<typeof messagesQuerySchema>;

/** `GET /api/topics/:name/tail` query params — omitting `partition` tails every partition. */
export const tailQuerySchema = z.object({
  partition: z.coerce.number().int().nonnegative().optional(),
});
export type TailQuery = z.infer<typeof tailQuerySchema>;

export const seekByTimeRequestSchema = z.object({
  timestamp: z.number().int(),
  partition: z.number().int().nonnegative().optional(),
});
export type SeekByTimeRequest = z.infer<typeof seekByTimeRequestSchema>;

export interface SeekByTimeEntry {
  readonly partition: number;
  /** `null` when no message at or after the timestamp exists on this partition (the broker returned `-1`). */
  readonly offset: string | null;
}

export interface SeekByTimeResponse {
  readonly offsets: readonly SeekByTimeEntry[];
}

export const deleteRecordsRequestSchema = z.object({
  partitions: z
    .array(
      z.object({
        partition: z.number().int().nonnegative(),
        /** Every record with an offset strictly below this one is deleted, per `Admin.deleteTopicRecords`. */
        beforeOffset: decimalOffsetSchema,
      }),
    )
    .min(1),
});
export type DeleteRecordsRequest = z.infer<typeof deleteRecordsRequestSchema>;
