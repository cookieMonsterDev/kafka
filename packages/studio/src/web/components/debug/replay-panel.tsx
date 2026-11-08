import { useState } from 'react';
import { Field, type OperationProps } from './fields';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { Textarea } from '../ui/textarea';
import { replayRequestSchema, rawRecordSchema } from '../../../shared/contracts/debug';
import { streamDebug, encodeText } from '../../lib/debug-api';

export function ReplayPanel({
  running,
  canWrite,
  operate,
  setOutput,
  replay,
  setReplay,
}: OperationProps & { readonly replay: string; readonly setReplay: (value: string) => void }) {
  const [target, setTarget] = useState('');
  const [rate, setRate] = useState('10');
  const [editedValue, setEditedValue] = useState('');
  const [tombstone, setTombstone] = useState(false);
  return (
    <section aria-label="Replay preview" className="flex flex-col gap-4 rounded-xl border border-border p-4">
      <p className="text-sm">
        Preview and edit up to 100 records below. Keys, values, and headers are base64 bytes; null preserves tombstones.
        Original offsets, timestamps, and partitions are not copied. Replayed events can cause duplicate application
        effects. Cancellation cannot undo sent records; do not blindly retry uncertain sends.
      </p>
      <Field label="Destination topic">
        <Input value={target} onChange={(event) => setTarget(event.target.value)} />
      </Field>
      <Field label="Records per second (1–100)">
        <Input type="number" min={1} max={100} value={rate} onChange={(event) => setRate(event.target.value)} />
      </Field>
      <details>
        <summary className="min-h-6 cursor-pointer py-1 text-sm">Edit the first record value as UTF-8</summary>
        <div className="mt-3 flex flex-col gap-3">
          <Button
            variant="outline"
            disabled={running}
            onClick={() =>
              void operate(() => {
                const records: unknown = JSON.parse(replay);
                const first = rawRecordSchema.parse(Array.isArray(records) ? (records[0] as unknown) : undefined);
                setTombstone(first.value === null);
                const bytes = Uint8Array.from(atob(first.value ?? ''), (character) => character.charCodeAt(0));
                setEditedValue(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
              })
            }
          >
            Load first record value
          </Button>
          <Field label="Edited UTF-8 value">
            <Textarea value={editedValue} onChange={(event) => setEditedValue(event.target.value)} />
          </Field>
          <label className="flex items-center gap-2 text-sm">
            <input
              className="size-6 focus-visible:ring-2 focus-visible:ring-ring"
              type="checkbox"
              checked={tombstone}
              onChange={(event) => setTombstone(event.target.checked)}
            />
            Use a null tombstone
          </label>
          <Button
            variant="outline"
            disabled={running}
            onClick={() =>
              void operate(() => {
                const records: unknown = JSON.parse(replay);
                if (!Array.isArray(records) || records.length === 0) throw new Error('Stage a record first');
                const first = rawRecordSchema.parse(records[0] as unknown);
                setReplay(
                  JSON.stringify(
                    [
                      { ...first, value: tombstone ? null : encodeText(editedValue) },
                      ...(records.slice(1) as unknown[]),
                    ],
                    null,
                    2,
                  ),
                );
              })
            }
          >
            Apply edited value to preview
          </Button>
        </div>
      </details>
      <Field label="Editable replay records (base64 JSON)">
        <Textarea rows={12} value={replay} onChange={(event) => setReplay(event.target.value)} />
      </Field>
      <Button
        disabled={running || !canWrite}
        onClick={() =>
          void operate(async (signal) => {
            const input = replayRequestSchema.parse({
              topic: target,
              records: JSON.parse(replay) as unknown,
              ratePerSecond: Number(rate),
            });
            const results: unknown[] = [];
            await streamDebug('/api/debug/replay', input, signal, (event, value) => {
              if (event === 'record') {
                results.push(value);
                setOutput([...results]);
              }
            });
          })
        }
      >
        Replay previewed records to {target || 'destination'}
      </Button>
      {!canWrite && (
        <p className="text-sm">Writes are unavailable in read-only mode or while permissions are loading.</p>
      )}
    </section>
  );
}
