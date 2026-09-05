import { MESSAGE_DECODERS, type MessageDecoder } from '../../lib/decode';
import { Input } from '../ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';

const ALL_PARTITIONS = 'all';

export type MessagesFrom = 'latest' | 'earliest';

export interface MessageFiltersValue {
  /** `null` reads/tails every partition. */
  readonly partition: number | null;
  /** History mode only — where the page read starts. A live tail always starts from "now". */
  readonly from: MessagesFrom;
  /** Client-side substring match over the decoded key, value, and header values of already-loaded messages. */
  readonly search: string;
  readonly decoder: MessageDecoder;
}

export interface MessageFiltersProps {
  readonly value: MessageFiltersValue;
  readonly onChange: (value: MessageFiltersValue) => void;
  readonly partitions: readonly number[];
  /** Hides the "from" seek control — a live tail has no seek position, it only ever starts now. */
  readonly showFrom?: boolean;
  readonly disabled?: boolean;
}

export function MessageFilters({
  value,
  onChange,
  partitions,
  showFrom = true,
  disabled = false,
}: MessageFiltersProps) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <Select
        value={value.partition === null ? ALL_PARTITIONS : String(value.partition)}
        onValueChange={(next) => onChange({ ...value, partition: next === ALL_PARTITIONS ? null : Number(next) })}
        disabled={disabled}
      >
        <SelectTrigger size="sm" className="w-36" aria-label="Partition">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL_PARTITIONS}>All partitions</SelectItem>
          {partitions.map((partition) => (
            <SelectItem key={partition} value={String(partition)}>
              Partition {partition}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {showFrom && (
        <Select
          value={value.from}
          onValueChange={(next) => onChange({ ...value, from: next as MessagesFrom })}
          disabled={disabled}
        >
          <SelectTrigger size="sm" className="w-32" aria-label="Read from">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="latest">Latest</SelectItem>
            <SelectItem value="earliest">Earliest</SelectItem>
          </SelectContent>
        </Select>
      )}

      <Select value={value.decoder} onValueChange={(next) => onChange({ ...value, decoder: next as MessageDecoder })}>
        <SelectTrigger size="sm" className="w-28" aria-label="Decode as">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {MESSAGE_DECODERS.map((decoder) => (
            <SelectItem key={decoder.value} value={decoder.value}>
              {decoder.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <label className="flex min-w-0 flex-1 items-center sm:max-w-xs">
        <span className="sr-only">Filter messages</span>
        <Input
          type="search"
          placeholder="Filter key, value, headers…"
          value={value.search}
          onChange={(event) => onChange({ ...value, search: event.target.value })}
        />
      </label>
    </div>
  );
}
