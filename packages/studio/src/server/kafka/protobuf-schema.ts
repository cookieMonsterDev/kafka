import protobuf from 'protobufjs';

interface RegistryDefinition {
  readonly schema: string;
  readonly schemaType?: string;
  readonly references?: readonly { readonly name: string; readonly subject: string; readonly version: number }[];
}
export type FetchSchema = (path: string) => Promise<RegistryDefinition>;

async function parseSchema(definition: RegistryDefinition, fetchSchema: FetchSchema) {
  const root = new protobuf.Root();
  const visited = new Set<string>();
  async function addReferences(schema: RegistryDefinition): Promise<void> {
    for (const reference of schema.references ?? []) {
      const key = JSON.stringify([reference.subject, reference.version]);
      if (visited.has(key)) continue;
      if (visited.size >= 32) throw new Error('Protobuf schema exceeds 32 references');
      visited.add(key);
      const child = await fetchSchema(
        `subjects/${encodeURIComponent(reference.subject)}/versions/${String(reference.version)}`,
      );
      await addReferences(child);
      protobuf.parse(child.schema, root);
    }
  }
  const parsed = protobuf.parse(definition.schema, root);
  const namespace = parsed.package ? root.lookup(parsed.package) : root;
  if (!(namespace instanceof protobuf.Namespace)) throw new Error('Missing Protobuf package');
  const topTypes = messageTypes(namespace);
  await addReferences(definition);
  root.resolveAll();
  return { root, namespace, topTypes };
}

function messageTypes(namespace: protobuf.Namespace): protobuf.Type[] {
  return namespace.nestedArray.filter((entry): entry is protobuf.Type => entry instanceof protobuf.Type);
}

function readIndex(buffer: Buffer, cursor: { offset: number }): number {
  let value = 0;
  let shift = 0;
  while (shift < 35 && cursor.offset < buffer.length) {
    const byte = buffer[cursor.offset++];
    if (byte === undefined) throw new Error('Truncated Protobuf message index');
    value += (byte & 127) * 2 ** shift;
    if ((byte & 128) === 0) {
      if (value % 2 !== 0 || value > 0xffffffff) throw new Error('Invalid Protobuf message index');
      return value / 2;
    }
    shift += 7;
  }
  throw new Error('Invalid Protobuf message index');
}

function writeIndex(index: number): number[] {
  let value = index * 2;
  const bytes: number[] = [];
  while (value >= 128) {
    bytes.push((value % 128) | 128);
    value = Math.floor(value / 128);
  }
  bytes.push(value);
  return bytes;
}

export async function decodeProtobuf(
  definition: RegistryDefinition,
  buffer: Buffer,
  fetchSchema: FetchSchema,
): Promise<unknown> {
  const { namespace, topTypes } = await parseSchema(definition, fetchSchema);
  const cursor = { offset: 5 };
  const length = readIndex(buffer, cursor);
  if (length > 16) throw new Error('Protobuf message nesting exceeds 16');
  const indexes = length === 0 ? [0] : Array.from({ length }, () => readIndex(buffer, cursor));
  let parent: protobuf.Namespace = namespace;
  let type: protobuf.Type | undefined;
  for (const [position, index] of indexes.entries()) {
    type = (position === 0 ? topTypes : messageTypes(parent))[index];
    if (!type) throw new Error('Protobuf message index is outside the schema');
    parent = type;
  }
  if (!type) throw new Error('Missing Protobuf message type');
  return type.toObject(type.decode(buffer.subarray(cursor.offset)), { longs: String, bytes: String, enums: String });
}

const LONG_TYPES: ReadonlySet<string> = new Set(['int64', 'uint64', 'sint64', 'fixed64', 'sfixed64']);
const UNSIGNED_LONG_TYPES: ReadonlySet<string> = new Set(['uint64', 'fixed64']);

function normalizeLong(value: unknown, unsigned: boolean): unknown {
  if (typeof value !== 'string' && typeof value !== 'number')
    throw new Error('64-bit integers require a decimal string or safe JSON integer');
  if (typeof value === 'string' && !/^-?\d+$/.test(value)) throw new Error('64-bit integers require a decimal string');
  if (typeof value === 'number' && !Number.isSafeInteger(value))
    throw new Error('Use a decimal string for integers outside the safe JSON number range');
  const integer = BigInt(value);
  const minimum = unsigned ? 0n : -(1n << 63n);
  const maximum = unsigned ? (1n << 64n) - 1n : (1n << 63n) - 1n;
  if (integer < minimum || integer > maximum) throw new Error('64-bit integer is outside its schema range');
  return { low: Number(BigInt.asIntN(32, integer)), high: Number(BigInt.asIntN(32, integer >> 32n)), unsigned };
}

/** Normalize only JSON forms emitted by our decoder; do not coerce invalid strings, booleans, or numbers. */
function normalizePayload(type: protobuf.Type, payload: unknown, depth = 0): unknown {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return payload;
  if (depth > 64) throw new Error('Protobuf payload nesting exceeds 64');
  const result: Record<string, unknown> = {};
  for (const [name, value] of Object.entries(payload as Record<string, unknown>)) {
    const field = Object.hasOwn(type.fields, name) ? type.fields[name] : undefined;
    if (!field) throw new Error(`Unknown Protobuf field ${name}`);
    const scalar = (item: unknown): unknown => {
      if (item === null || item === undefined) return item;
      if (field.resolvedType instanceof protobuf.Type) return normalizePayload(field.resolvedType, item, depth + 1);
      if (field.resolvedType instanceof protobuf.Enum && typeof item === 'string') {
        const number = Object.hasOwn(field.resolvedType.values, item) ? field.resolvedType.values[item] : undefined;
        if (number === undefined) throw new Error(`Unknown enum value ${item} for ${name}`);
        return number;
      }
      if (LONG_TYPES.has(field.type)) return normalizeLong(item, UNSIGNED_LONG_TYPES.has(field.type));
      if (field.type === 'bytes' && typeof item === 'string') {
        if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(item))
          throw new Error(`Invalid base64 bytes for ${name}`);
        return Buffer.from(item, 'base64');
      }
      return item;
    };
    let normalized: unknown;
    if (field.repeated && Array.isArray(value)) normalized = value.map(scalar);
    else if (field.map && typeof value === 'object' && value !== null && !Array.isArray(value))
      normalized = Object.fromEntries(
        Object.entries(value as Record<string, unknown>).map(([key, entry]) => [key, scalar(entry)]),
      );
    else normalized = scalar(value);
    Object.defineProperty(result, name, { value: normalized, enumerable: true });
  }
  return result;
}

export async function encodeProtobuf(
  definition: RegistryDefinition,
  id: number,
  payload: unknown,
  fetchSchema: FetchSchema,
  messageType?: string,
): Promise<Buffer> {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload))
    throw new Error('Protobuf payload must be an object');
  const { root, namespace, topTypes } = await parseSchema(definition, fetchSchema);
  const type = messageType ? root.lookupType(messageType) : topTypes[0];
  if (!type) throw new Error('No Protobuf message type in schema');
  const indexes: number[] = [];
  let current: protobuf.ReflectionObject = type;
  while (current !== namespace) {
    const parent = current.parent;
    if (!(parent instanceof protobuf.Namespace) || !(current instanceof protobuf.Type))
      throw new Error('Message type is outside the writer schema package');
    const index = (parent === namespace ? topTypes : messageTypes(parent)).indexOf(current);
    if (index < 0) throw new Error('Message type is outside the writer schema');
    indexes.unshift(index);
    current = parent;
    if (indexes.length > 16) throw new Error('Protobuf message nesting exceeds 16');
  }
  const normalized = normalizePayload(type, payload);
  const invalid = type.verify(normalized as Record<string, unknown>);
  if (invalid) throw new Error(invalid);
  const message = type.fromObject(normalized as Record<string, unknown>);
  const header = Buffer.alloc(5);
  header.writeUInt32BE(id, 1);
  const encodedIndexes =
    indexes.length === 1 && indexes[0] === 0 ? [0] : [...writeIndex(indexes.length), ...indexes.flatMap(writeIndex)];
  return Buffer.concat([header, Buffer.from(encodedIndexes), type.encode(message).finish()]);
}
