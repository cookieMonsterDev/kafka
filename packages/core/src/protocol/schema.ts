import { Decoder } from './decoder.js'
import { Encoder } from './encoder.js'

/**
 * A codec for one wire-format shape: writes a value of type `T` onto an `Encoder`, reads it back
 * off a `Decoder`. Every request/response body is composed from these — see `object()`/`array()`
 * below for how primitives combine into the field lists that describe a whole message.
 */
export interface FieldCodec<T> {
  write(encoder: Encoder, value: T): void
  read(decoder: Decoder): T
}

function codec<T>(write: FieldCodec<T>['write'], read: FieldCodec<T>['read']): FieldCodec<T> {
  return { write, read }
}

function nonNull<T>(value: T | null, what: string): T {
  if (value === null) throw new RangeError(`Expected a non-null ${what}, got null`)
  return value
}

export const int8: FieldCodec<number> = codec(
  (e, v) => void e.writeInt8(v),
  (d) => d.readInt8()
)
export const int16: FieldCodec<number> = codec(
  (e, v) => void e.writeInt16(v),
  (d) => d.readInt16()
)
export const int32: FieldCodec<number> = codec(
  (e, v) => void e.writeInt32(v),
  (d) => d.readInt32()
)
export const uint32: FieldCodec<number> = codec(
  (e, v) => void e.writeUInt32(v),
  (d) => d.readInt32() >>> 0
)
export const int64: FieldCodec<bigint> = codec(
  (e, v) => void e.writeInt64(v),
  (d) => d.readInt64()
)
export const boolean: FieldCodec<boolean> = codec(
  (e, v) => void e.writeBoolean(v),
  (d) => d.readBoolean()
)
export const varint: FieldCodec<number> = codec(
  (e, v) => void e.writeVarInt(v),
  (d) => d.readVarInt()
)
export const varlong: FieldCodec<bigint> = codec(
  (e, v) => void e.writeVarLong(v),
  (d) => d.readVarLong()
)
export const uvarint: FieldCodec<number> = codec(
  (e, v) => void e.writeUVarInt(v),
  (d) => d.readUVarInt()
)

export const string: FieldCodec<string> = codec(
  (e, v) => void e.writeString(v),
  (d) => nonNull(d.readString(), 'string field')
)
export const nullableString: FieldCodec<string | null> = codec(
  (e, v) => void e.writeString(v),
  (d) => d.readString()
)

export const bytes: FieldCodec<Buffer> = codec(
  (e, v) => void e.writeBytes(v),
  (d) => nonNull(d.readBytes(), 'bytes field')
)
export const nullableBytes: FieldCodec<Buffer | null> = codec(
  (e, v) => void e.writeBytes(v),
  (d) => d.readBytes()
)

/**
 * A buffer with no length prefix at all — e.g. `SaslAuthenticate`'s request body, which is just
 * the raw SASL mechanism bytes. Only meaningful as the last field in a schema: reading consumes
 * everything remaining in the buffer.
 */
export const rawBytes: FieldCodec<Buffer> = codec(
  (e, v) => void e.writeBuffer(v),
  (d) => d.readAll()
)

function readArrayBody<T>(d: Decoder, length: number, element: FieldCodec<T>): T[] {
  if (length === -1) return []
  const values = new Array<T>(length)
  for (let i = 0; i < length; i++) values[i] = element.read(d)
  return values
}

/**
 * A protocol array is just a length prefix followed by that many elements, each read by the
 * element codec in sequence — no need for `Encoder`'s own `writeArray`/`readArray`, which exist
 * to serve the handwritten codecs the schema system replaces.
 */
export function array<T>(element: FieldCodec<T>): FieldCodec<T[]> {
  return codec(
    (e, values) => {
      e.writeInt32(values.length)
      for (const value of values) element.write(e, value)
    },
    (d) => readArrayBody(d, d.readInt32(), element)
  )
}

/**
 * Mirrors `Encoder#writeNullableArray`: an empty input array is written as wire length `-1`
 * (kafkajs's collapsed stand-in for "null", meaning e.g. "all topics" to a `Metadata` request)
 * rather than an actual `0`-length array — there's no way to request a true `0`-length array
 * through this codec, matching kafkajs's own behavior byte-for-byte. Reading is identical to
 * `array()`: a `-1` length (or `0`) always comes back as `[]`.
 */
export function nullableArray<T>(element: FieldCodec<T>): FieldCodec<T[]> {
  return codec(
    (e, values) => {
      if (values.length === 0) {
        e.writeInt32(-1)
        return
      }
      e.writeInt32(values.length)
      for (const value of values) element.write(e, value)
    },
    (d) => readArrayBody(d, d.readInt32(), element)
  )
}

export interface FieldSpec<Name extends string, T> {
  name: Name
  codec: FieldCodec<T>
}

export function field<Name extends string, T>(name: Name, fieldCodec: FieldCodec<T>): FieldSpec<Name, T> {
  return { name, codec: fieldCodec }
}

type InferSchema<Fields extends readonly FieldSpec<string, unknown>[]> = {
  [K in Fields[number]['name']]: Extract<Fields[number], FieldSpec<K, unknown>> extends FieldSpec<K, infer T>
    ? T
    : never
}

/**
 * Combines an ordered field list into a codec for the object they describe — the building block
 * both a whole request/response body and any nested repeated group (e.g. `[partition_metadata]`
 * inside a `Metadata` response) are built from. Field order is wire order; there is no reordering.
 */
export function object<const Fields extends readonly FieldSpec<string, unknown>[]>(
  fields: Fields
): FieldCodec<InferSchema<Fields>> {
  return codec(
    (e, value) => {
      for (const spec of fields) spec.codec.write(e, (value as Record<string, unknown>)[spec.name])
    },
    (d) => {
      const result: Record<string, unknown> = {}
      for (const spec of fields) result[spec.name] = spec.codec.read(d)
      return result as InferSchema<Fields>
    }
  )
}

export interface RequestDefinition {
  apiKey: number
  apiVersion: number
  apiName: string
  encode(): Promise<Encoder>
}

/**
 * Builds the per-version `request.ts` factory: given the fixed `{apiKey, apiVersion, apiName}`
 * and a body schema, returns a function of the request's field values to a `RequestDefinition`,
 * mirroring kafkajs's `module.exports = (values) => ({ apiKey, apiVersion, apiName, encode })`.
 */
export function defineRequest<T>(options: {
  apiKey: number
  apiVersion: number
  apiName: string
  schema: FieldCodec<T>
}): (values: T) => RequestDefinition {
  return (values) => ({
    apiKey: options.apiKey,
    apiVersion: options.apiVersion,
    apiName: options.apiName,
    encode: () => {
      const encoder = new Encoder()
      options.schema.write(encoder, values)
      return Promise.resolve(encoder)
    },
  })
}

export interface ResponseDefinition<T> {
  decode(rawData: Buffer): Promise<T>
  parse(data: T): Promise<T>
}

/**
 * Builds the per-version `response.ts`: a body schema plus an optional `parse` for
 * error-code inspection (defaults to identity — most responses without an `errorCode` field
 * have nothing to validate).
 */
export function defineResponse<T>(options: { schema: FieldCodec<T>; parse?: (data: T) => Promise<T> }): ResponseDefinition<T> {
  return {
    // `async` (not a plain arrow returning `Promise.resolve(...)`) so a synchronous throw from
    // `schema.read` — e.g. a non-nullable field seeing `null` on the wire — becomes a rejected
    // promise instead of throwing before the caller ever gets a promise to catch.
    decode: async (rawData) => options.schema.read(new Decoder(rawData)),
    parse: options.parse ?? (async (data) => data),
  }
}
