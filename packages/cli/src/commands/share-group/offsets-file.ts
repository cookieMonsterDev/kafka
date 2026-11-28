import { CliUsageError } from '../../args/coerce';

export interface StartOffsetEntry {
  readonly topic: string;
  readonly partition: number;
  readonly offset: bigint;
}

/** The wire format's bounds: a partition index is an INT32 and an offset an INT64. */
const MAX_PARTITION = 2 ** 31 - 1;
const MIN_OFFSET = -(2n ** 63n);
const MAX_OFFSET = 2n ** 63n - 1n;

/**
 * Validates one start-offset entry's fields, shared by `--set` and `--from-file`, so a value the
 * request cannot encode fails before any topic is written.
 *
 * @param source - prefixes each error: `--set`, or the file and line.
 * @param entry - the raw text each error quotes.
 */
export function parseStartOffsetEntry(
  [topic, partitionRaw, offsetRaw]: readonly [string, string, string],
  { source, entry }: { readonly source: string; readonly entry: string },
): StartOffsetEntry {
  if (topic.length === 0) {
    throw new CliUsageError(`${source} topic must not be empty, got "${entry}"`);
  }
  const partition = Number(partitionRaw);
  if (!/^\s*\d+\s*$/.test(partitionRaw) || partition > MAX_PARTITION) {
    throw new CliUsageError(
      `${source} partition must be an integer from 0 to ${String(MAX_PARTITION)}, got "${entry}"`,
    );
  }
  const offset = /^\s*[+-]?\d+\s*$/.test(offsetRaw) ? BigInt(offsetRaw) : undefined;
  if (offset === undefined || offset < MIN_OFFSET || offset > MAX_OFFSET) {
    throw new CliUsageError(`${source} offset must be a 64-bit integer, got "${entry}"`);
  }
  return { topic, partition, offset };
}

const QUOTED_FIELD = /"((?:[^"]|"")*)"/y;

/**
 * Splits one CSV record on commas. A field may be double-quoted, with `""` for a literal quote, as
 * Jackson's CSV writer quotes a group id that contains a comma or space. Returns `undefined` for an
 * unterminated quote or text after a closing quote.
 */
function splitCsvRecord(line: string): string[] | undefined {
  const fields: string[] = [];
  let position = 0;
  while (true) {
    if (line[position] === '"') {
      QUOTED_FIELD.lastIndex = position;
      const match = QUOTED_FIELD.exec(line);
      if (match === null) return undefined;
      fields.push((match[1] ?? '').replaceAll('""', '"'));
      position = QUOTED_FIELD.lastIndex;
    } else {
      const commaIndex = line.indexOf(',', position);
      const end = commaIndex === -1 ? line.length : commaIndex;
      fields.push(line.slice(position, end));
      position = end;
    }
    if (position === line.length) return fields;
    if (line[position] !== ',') return undefined;
    position += 1;
  }
}

const NO_GROUP_FIELD_COUNT = 3;
const WITH_GROUP_FIELD_COUNT = 4;
const NO_GROUP_FORMAT = 'topic,partition,offset';
const WITH_GROUP_FORMAT = 'group,topic,partition,offset';

/**
 * Parses the CSV that `kafka-share-groups.sh --reset-offsets --from-file` reads (KIP-1323) and
 * `--export` writes: one `topic,partition,offset` record per line with no header, or
 * `group,topic,partition,offset` records (a multi-group `kafka-consumer-groups.sh` export) filtered
 * down to `groupId`. The first record picks the format and every record must match it. Blank lines
 * are skipped, and a repeated topic partition keeps its last offset, as Kafka's tool does.
 */
export function parseShareGroupOffsetsFile(
  text: string,
  { path, groupId }: { readonly path: string; readonly groupId: string },
): StartOffsetEntry[] {
  const fileSource = `--from-file "${path}"`;
  let fieldCount: number | undefined;
  const offsets = new Map<string, StartOffsetEntry>();

  text.split(/\r?\n/).forEach((line, index) => {
    if (line.trim().length === 0) return;
    const source = `${fileSource} line ${String(index + 1)}:`;
    const fields = splitCsvRecord(line);
    fieldCount ??= fields?.length;
    if (fieldCount !== NO_GROUP_FIELD_COUNT && fieldCount !== WITH_GROUP_FIELD_COUNT) {
      throw new CliUsageError(`${source} expected "${NO_GROUP_FORMAT}" or "${WITH_GROUP_FORMAT}", got "${line}"`);
    }
    if (fields?.length !== fieldCount) {
      const format = fieldCount === NO_GROUP_FIELD_COUNT ? NO_GROUP_FORMAT : WITH_GROUP_FORMAT;
      throw new CliUsageError(`${source} expected "${format}" like the first line, got "${line}"`);
    }

    const hasGroup = fieldCount === WITH_GROUP_FIELD_COUNT;
    const recordFields = hasGroup ? fields.slice(1) : fields;
    const entry = parseStartOffsetEntry(recordFields as [string, string, string], { source, entry: line });
    if (hasGroup && fields[0] !== groupId) return;
    offsets.set(`${entry.topic}:${String(entry.partition)}`, entry);
  });

  if (fieldCount === undefined) {
    throw new CliUsageError(`${fileSource} has no entries`);
  }
  if (offsets.size === 0) {
    throw new CliUsageError(`${fileSource} has no entries for share group "${groupId}"`);
  }
  return [...offsets.values()];
}
