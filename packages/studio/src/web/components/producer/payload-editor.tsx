import { useId, useState } from 'react';
import { X } from 'lucide-react';
import {
  payloadEditorValueError,
  type HeaderRow,
  type PayloadEditorValue,
  type ValueFormat,
} from '../../lib/payload-editor-schema';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { Textarea } from '../ui/textarea';

let nextHeaderRowId = 0;

export interface PayloadEditorProps {
  readonly value: PayloadEditorValue;
  readonly onChange: (value: PayloadEditorValue) => void;
  readonly disabled?: boolean;
}

/** Key, JSON/text value, headers, and an optional fixed partition — the fields a produce request needs beyond the topic. */
export function PayloadEditor({ value, onChange, disabled = false }: PayloadEditorProps) {
  const [touchedError, setTouchedError] = useState<string | null>(null);
  const keyId = useId();
  const valueId = useId();
  const partitionId = useId();

  function addHeaderRow(): void {
    nextHeaderRowId += 1;
    onChange({
      ...value,
      headers: [...value.headers, { id: `header-${String(nextHeaderRowId)}`, key: '', value: '' }],
    });
  }

  function updateHeaderRow(id: string, patch: Partial<HeaderRow>): void {
    onChange({ ...value, headers: value.headers.map((row) => (row.id === id ? { ...row, ...patch } : row)) });
  }

  function removeHeaderRow(id: string): void {
    onChange({ ...value, headers: value.headers.filter((row) => row.id !== id) });
  }

  const displayedError = touchedError ?? payloadEditorValueError(value);

  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <div className="flex flex-col gap-1.5">
          <label htmlFor={keyId} className="text-sm font-medium">
            Key <span className="text-muted-foreground">(optional)</span>
          </label>
          <Input
            id={keyId}
            value={value.key}
            disabled={disabled}
            onChange={(event) => onChange({ ...value, key: event.target.value })}
            placeholder="no key"
          />
        </div>
        <div className="flex flex-col gap-1.5">
          <label htmlFor={partitionId} className="text-sm font-medium">
            Partition <span className="text-muted-foreground">(optional)</span>
          </label>
          <Input
            id={partitionId}
            type="number"
            min={0}
            step={1}
            value={value.partition}
            disabled={disabled}
            onChange={(event) => onChange({ ...value, partition: event.target.value })}
            placeholder="auto"
          />
        </div>
      </div>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <span className="text-sm font-medium">
            Headers <span className="text-muted-foreground">(optional)</span>
          </span>
          <Button type="button" variant="outline" size="sm" onClick={addHeaderRow} disabled={disabled}>
            Add header
          </Button>
        </div>
        {value.headers.map((row) => (
          <div key={row.id} className="flex items-center gap-2">
            <Input
              aria-label="header key"
              placeholder="content-type"
              value={row.key}
              disabled={disabled}
              onChange={(event) => updateHeaderRow(row.id, { key: event.target.value })}
            />
            <Input
              aria-label="header value"
              placeholder="application/json"
              value={row.value}
              disabled={disabled}
              onChange={(event) => updateHeaderRow(row.id, { value: event.target.value })}
            />
            <Button
              type="button"
              variant="ghost"
              size="icon"
              aria-label="remove header"
              disabled={disabled}
              onClick={() => removeHeaderRow(row.id)}
            >
              <X className="size-4" aria-hidden="true" />
            </Button>
          </div>
        ))}
      </div>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <label htmlFor={valueId} className="text-sm font-medium">
            Value
          </label>
          <Select
            value={value.valueFormat}
            onValueChange={(next: ValueFormat) => onChange({ ...value, valueFormat: next })}
            disabled={disabled}
          >
            <SelectTrigger size="sm" aria-label="Value format">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="json">JSON</SelectItem>
              <SelectItem value="text">Text</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <Textarea
          id={valueId}
          value={value.value}
          disabled={disabled}
          onChange={(event) => {
            setTouchedError(null);
            onChange({ ...value, value: event.target.value });
          }}
          onBlur={() => setTouchedError(payloadEditorValueError(value))}
          className="min-h-40 font-mono text-xs"
          aria-invalid={displayedError !== null}
          spellCheck={false}
        />
        {displayedError !== null && (
          <p className="text-sm text-destructive" role="alert">
            {displayedError}
          </p>
        )}
      </div>
    </div>
  );
}
