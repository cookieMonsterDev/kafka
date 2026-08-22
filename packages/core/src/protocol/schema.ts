import { Decoder } from './decoder';
import { Encoder } from './encoder';

/**
 * A codec for one wire-format shape: writes a value of type `T` onto an `Encoder`, reads it back
 * off a `Decoder`. Every request/response body is composed from these — see `object()`/`array()`
 * below for how primitives combine into the field lists that describe a whole message.
 */
export interface FieldCodec<T> {
  write(encoder: Encoder, value: T): void;
  read(decoder: Decoder): T;
}

function codec<T>(write: FieldCodec<T>['write'], read: FieldCodec<T>['read']): FieldCodec<T> {
  return { write, read };
}

function nonNull<T>(value: T | null, what: string): T {
  if (value === null) throw new RangeError(`Expected a non-null ${what}, got null`);
  return value;
}

export const int8: FieldCodec<number> = codec(
  (e, v) => void e.writeInt8(v),
  (d) => d.readInt8(),
);
export const int16: FieldCodec<number> = codec(
  (e, v) => void e.writeInt16(v),
  (d) => d.readInt16(),
);
export const int32: FieldCodec<number> = codec(
  (e, v) => void e.writeInt32(v),
  (d) => d.readInt32(),
);
export const uint32: FieldCodec<number> = codec(
  (e, v) => void e.writeUInt32(v),
  (d) => d.readInt32() >>> 0,
);
export const int64: FieldCodec<bigint> = codec(
  (e, v) => void e.writeInt64(v),
  (d) => d.readInt64(),
);
export const boolean: FieldCodec<boolean> = codec(
  (e, v) => void e.writeBoolean(v),
  (d) => d.readBoolean(),
);

/**
 * IEEE 754 binary64 (`DOUBLE` on the wire). Client-quota APIs store quota values this way.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const float64: FieldCodec<number> = codec(
  (e, v) => void e.writeDouble(v),
  (d) => d.readDouble(),
);
export const varint: FieldCodec<number> = codec(
  (e, v) => void e.writeVarInt(v),
  (d) => d.readVarInt(),
);
export const varlong: FieldCodec<bigint> = codec(
  (e, v) => void e.writeVarLong(v),
  (d) => d.readVarLong(),
);
export const uvarint: FieldCodec<number> = codec(
  (e, v) => void e.writeUVarInt(v),
  (d) => d.readUVarInt(),
);

export const string: FieldCodec<string> = codec(
  (e, v) => void e.writeString(v),
  (d) => nonNull(d.readString(), 'string field'),
);
export const nullableString: FieldCodec<string | null> = codec(
  (e, v) => void e.writeString(v),
  (d) => d.readString(),
);

export const bytes: FieldCodec<Buffer> = codec(
  (e, v) => void e.writeBytes(v),
  (d) => nonNull(d.readBytes(), 'bytes field'),
);
export const nullableBytes: FieldCodec<Buffer | null> = codec(
  (e, v) => void e.writeBytes(v),
  (d) => d.readBytes(),
);

export const compactString: FieldCodec<string> = codec(
  (e, v) => void e.writeUVarIntString(v),
  (d) => nonNull(d.readUVarIntString(), 'compact string field'),
);
export const compactNullableString: FieldCodec<string | null> = codec(
  (e, v) => void e.writeUVarIntString(v),
  (d) => d.readUVarIntString(),
);

export const compactBytes: FieldCodec<Buffer> = codec(
  (e, v) => void e.writeUVarIntBytes(v),
  (d) => nonNull(d.readUVarIntBytes(), 'compact bytes field'),
);
export const compactNullableBytes: FieldCodec<Buffer | null> = codec(
  (e, v) => void e.writeUVarIntBytes(v),
  (d) => d.readUVarIntBytes(),
);

/**
 * 16-byte Kafka UUID (KIP-516 topic IDs and later Fetch/Metadata versions).
 */
export const uuid: FieldCodec<Buffer> = codec(
  (e, v) => {
    if (v.length !== 16) throw new RangeError(`Expected a 16-byte UUID, got ${v.length} bytes`);
    e.writeBuffer(v);
  },
  (d) => {
    const value = d.readBytes(16);
    if (value === null || value.length !== 16) throw new RangeError('Expected a 16-byte UUID');
    return value;
  },
);

/**
 * Empty tagged-fields buffer (`TAG_BUFFER`). Flexible structs always end with one; this codec
 * writes the empty form (`uvarint 0`) and skips whatever tagged fields the broker sent.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const taggedFields: FieldCodec<Record<string, unknown> | null> = codec(
  (e) => void e.writeUVarInt(0),
  (d) => d.readTaggedFields(),
);

/**
 * A buffer with no length prefix at all — e.g. `SaslAuthenticate`'s request body, which is just
 * the raw SASL mechanism bytes. Only meaningful as the last field in a schema: reading consumes
 * everything remaining in the buffer.
 */
export const rawBytes: FieldCodec<Buffer> = codec(
  (e, v) => void e.writeBuffer(v),
  (d) => d.readAll(),
);

function readArrayBody<T>(d: Decoder, length: number, element: FieldCodec<T>): T[] {
  if (length === -1) return [];
  const values = new Array<T>(length);
  for (let i = 0; i < length; i++) values[i] = element.read(d);
  return values;
}

/**
 * A protocol array is just a length prefix followed by that many elements, each read by the
 * element codec in sequence — no need for `Encoder`'s own `writeArray`/`readArray`, which exist
 * to serve the handwritten codecs the schema system replaces.
 */
export function array<T>(element: FieldCodec<T>): FieldCodec<T[]> {
  return codec(
    (e, values) => {
      e.writeInt32(values.length);
      for (const value of values) element.write(e, value);
    },
    (d) => readArrayBody(d, d.readInt32(), element),
  );
}

/**
 * Empty input is written as wire length `-1` (null), the Kafka convention for
 * "all topics" on Metadata. A true empty array cannot be requested through this
 * codec. A `-1` or `0` length always decodes as `[]`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function nullableArray<T>(element: FieldCodec<T>): FieldCodec<T[]> {
  return codec(
    (e, values) => {
      if (values.length === 0) {
        e.writeInt32(-1);
        return;
      }
      e.writeInt32(values.length);
      for (const value of values) element.write(e, value);
    },
    (d) => readArrayBody(d, d.readInt32(), element),
  );
}

function readCompactArrayBody<T>(d: Decoder, encodedLength: number, element: FieldCodec<T>): T[] {
  if (encodedLength === 0) return [];
  return readArrayBody(d, encodedLength - 1, element);
}

/**
 * Compact (flexible) array: length is an unsigned varint of `N + 1`. A null compact array
 * (encoded length `0`) decodes as `[]`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function compactArray<T>(element: FieldCodec<T>): FieldCodec<T[]> {
  return codec(
    (e, values) => {
      e.writeUVarInt(values.length + 1);
      for (const value of values) element.write(e, value);
    },
    (d) => readCompactArrayBody(d, d.readUVarInt(), element),
  );
}

/**
 * Compact array that preserves wire null (encoded length `0`) instead of mapping it to `[]`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function compactNullableArray<T>(element: FieldCodec<T>): FieldCodec<T[] | null> {
  return codec(
    (e, values) => {
      if (values === null) {
        e.writeUVarInt(0);
        return;
      }
      e.writeUVarInt(values.length + 1);
      for (const value of values) element.write(e, value);
    },
    (d) => {
      const encodedLength = d.readUVarInt();
      if (encodedLength === 0) return null;
      return readArrayBody(d, encodedLength - 1, element);
    },
  );
}

/**
 * Nullable struct (Kafka `NULLABLE_STRUCT`): a leading INT8 marker of `-1` (null) or `1`
 * (present), then the struct body. Used by ConsumerGroupHeartbeat's Assignment field.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function nullableStruct<T>(element: FieldCodec<T>): FieldCodec<T | null> {
  return codec(
    (e, value) => {
      if (value === null) {
        e.writeInt8(-1);
        return;
      }
      e.writeInt8(1);
      element.write(e, value);
    },
    (d) => {
      const marker = d.readInt8();
      if (marker === -1) return null;
      if (marker === 1) return element.read(d);
      throw new RangeError(`Expected nullable struct marker -1 or 1, got ${marker}`);
    },
  );
}

export interface FieldSpec<Name extends string, T> {
  name: Name;
  codec: FieldCodec<T>;
}

export function field<Name extends string, T>(name: Name, fieldCodec: FieldCodec<T>): FieldSpec<Name, T> {
  return { name, codec: fieldCodec };
}

type InferSchema<Fields extends readonly FieldSpec<string, unknown>[]> = {
  [K in Fields[number]['name']]: Extract<Fields[number], FieldSpec<K, unknown>> extends FieldSpec<K, infer T>
    ? T
    : never;
};

/**
 * Combines an ordered field list into a codec for the object they describe — the building block
 * both a whole request/response body and any nested repeated group (e.g. `[partition_metadata]`
 * inside a `Metadata` response) are built from. Field order is wire order; there is no reordering.
 */
export function object<const Fields extends readonly FieldSpec<string, unknown>[]>(
  fields: Fields,
): FieldCodec<InferSchema<Fields>> {
  return codec(
    (e, value) => {
      for (const spec of fields) spec.codec.write(e, (value as Record<string, unknown>)[spec.name]);
    },
    (d) => {
      const result: Record<string, unknown> = {};
      for (const spec of fields) result[spec.name] = spec.codec.read(d);
      return result as InferSchema<Fields>;
    },
  );
}

/**
 * Like `object()`, but appends an empty `TAG_BUFFER` so callers of flexible request/response
 * bodies cannot forget the trailing tagged fields required by KIP-482.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function flexibleObject<const Fields extends readonly FieldSpec<string, unknown>[]>(
  fields: Fields,
): FieldCodec<InferSchema<Fields>> {
  const body = object(fields);
  return codec(
    (e, value) => {
      body.write(e, value);
      taggedFields.write(e, null);
    },
    (d) => {
      const value = body.read(d);
      taggedFields.read(d);
      return value;
    },
  );
}

/**
 * Nullable nested record in flexible versions: INT8 `-1` for null, INT8 `1` plus the
 * compact struct (including its TAG_BUFFER) when present. Used by DescribeTopicPartitions
 * Cursor / NextCursor (API key 75).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function nullableFlexibleObject<const Fields extends readonly FieldSpec<string, unknown>[]>(
  fields: Fields,
): FieldCodec<InferSchema<Fields> | null> {
  const body = flexibleObject(fields);
  return codec(
    (e, value) => {
      if (value === null) {
        e.writeInt8(-1);
        return;
      }
      e.writeInt8(1);
      body.write(e, value);
    },
    (d) => {
      if (d.readInt8() < 0) return null;
      return body.read(d);
    },
  );
}

export interface RequestDefinition {
  apiKey: number;
  apiVersion: number;
  apiName: string;
  encode(): Promise<Encoder>;
  /**
   * Whether the broker actually writes a response to the wire for this request - false only for
   * a `Produce` sent with `acks: 0`, which the broker acknowledges by staying silent. Absent
   * (the default for every other request) means the caller should always wait for one.
   */
  expectResponse?(): boolean;
}

/**
 * Builds a per-version request factory from `{ apiKey, apiVersion, apiName }` and a body schema.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function defineRequest<T>(options: {
  apiKey: number;
  apiVersion: number;
  apiName: string;
  schema: FieldCodec<T>;
}): (values: T) => RequestDefinition {
  return (values) => ({
    apiKey: options.apiKey,
    apiVersion: options.apiVersion,
    apiName: options.apiName,
    encode: () => {
      const encoder = new Encoder();
      options.schema.write(encoder, values);
      return Promise.resolve(encoder);
    },
  });
}

export interface ResponseDefinition<T> {
  decode(rawData: Buffer): Promise<T>;
  parse(data: T): Promise<T>;
}

/**
 * Builds the per-version `response.ts`: a body schema plus an optional `parse` for
 * error-code inspection (defaults to identity — most responses without an `errorCode` field
 * have nothing to validate).
 */
export function defineResponse<T>(options: {
  schema: FieldCodec<T>;
  parse?: (data: T) => Promise<T>;
}): ResponseDefinition<T> {
  return {
    // `async` (not a plain arrow returning `Promise.resolve(...)`) so a synchronous throw from
    // `schema.read` — e.g. a non-nullable field seeing `null` on the wire — becomes a rejected
    // promise instead of throwing before the caller ever gets a promise to catch.
    decode: async (rawData) => options.schema.read(new Decoder(rawData)),
    parse: options.parse ?? (async (data) => data),
  };
}
