import { useMemo, useState } from 'react';
import type { AlterTopicConfigsRequest, TopicConfigEntry } from '../../../shared/contracts/topic';
import { validateTopicConfigValue } from '../../lib/topic-config-schema';
import { Button } from '../ui/button';
import { Input } from '../ui/input';

export interface ConfigEditorProps {
  readonly configs: readonly TopicConfigEntry[];
  readonly onSave: (input: AlterTopicConfigsRequest) => void;
  readonly pending?: boolean;
}

/** A row's pending edit: a string to set it to, or `null` to unset it back to the broker default. */
type PendingEdits = Record<string, string | null>;

/**
 * Every entry starts at its current broker value; typing in one queues a `set`, "Reset" on a
 * non-default entry queues an `unset` — nothing reaches the server until Save, so a config page
 * left open with unsaved edits can't drift out from under the operator.
 */
export function ConfigEditor({ configs, onSave, pending = false }: ConfigEditorProps) {
  const [edits, setEdits] = useState<PendingEdits>({});

  const dirtyCount = Object.keys(edits).length;
  const sorted = useMemo(() => [...configs].sort((a, b) => a.name.localeCompare(b.name)), [configs]);

  /** Only a queued `set` is validated — a queued `unset` reverts to the broker default, which needs no check. */
  function rowError(name: string): string | null {
    const edit = edits[name];
    if (edit === null || edit === undefined) return null;
    return validateTopicConfigValue(name, edit);
  }

  const hasInvalidRow = Object.keys(edits).some((name) => rowError(name) !== null);

  function currentValue(entry: TopicConfigEntry): string {
    const edit = edits[entry.name];
    if (edit === null) return '';
    if (edit !== undefined) return edit;
    return entry.value ?? '';
  }

  function setValue(name: string, value: string): void {
    setEdits((prev) => ({ ...prev, [name]: value }));
  }

  function queueUnset(name: string): void {
    setEdits((prev) => ({ ...prev, [name]: null }));
  }

  function discard(name: string): void {
    setEdits((prev) => {
      const next = { ...prev };
      delete next[name];
      return next;
    });
  }

  function save(): void {
    const set: Record<string, string> = {};
    const unset: string[] = [];
    for (const [name, value] of Object.entries(edits)) {
      if (value === null) unset.push(name);
      else set[name] = value;
    }
    onSave({ ...(Object.keys(set).length > 0 ? { set } : {}), ...(unset.length > 0 ? { unset } : {}) });
    setEdits({});
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="overflow-x-auto rounded-xl border border-border">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs text-muted-foreground">
              <th scope="col" className="px-3 py-2 font-medium">
                Key
              </th>
              <th scope="col" className="px-3 py-2 font-medium">
                Value
              </th>
              <th scope="col" className="px-3 py-2 font-medium">
                Source
              </th>
              <th scope="col" className="px-3 py-2 font-medium">
                <span className="sr-only">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((entry) => {
              const isUnsetQueued = edits[entry.name] === null;
              const error = rowError(entry.name);
              return (
                <tr key={entry.name} className="border-b border-border last:border-0">
                  <td className="px-3 py-2 font-mono text-xs">{entry.name}</td>
                  <td className="px-3 py-2">
                    <Input
                      aria-label={`value for ${entry.name}`}
                      value={currentValue(entry)}
                      disabled={entry.readOnly}
                      placeholder={isUnsetQueued ? 'reverting to default…' : undefined}
                      onChange={(event) => setValue(entry.name, event.target.value)}
                      aria-invalid={error !== null}
                    />
                    {error !== null && (
                      <p className="mt-1 text-xs text-destructive" role="alert">
                        {error}
                      </p>
                    )}
                  </td>
                  <td className="px-3 py-2 text-xs text-muted-foreground">
                    {entry.isDefault ? 'default' : 'override'}
                  </td>
                  <td className="px-3 py-2 text-right whitespace-nowrap">
                    {edits[entry.name] !== undefined ? (
                      <Button type="button" variant="ghost" size="sm" onClick={() => discard(entry.name)}>
                        Undo
                      </Button>
                    ) : (
                      !entry.isDefault &&
                      !entry.readOnly && (
                        <Button type="button" variant="ghost" size="sm" onClick={() => queueUnset(entry.name)}>
                          Reset
                        </Button>
                      )
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="flex items-center justify-end gap-2">
        {dirtyCount > 0 && (
          <span className="text-sm text-muted-foreground">
            {dirtyCount} pending change{dirtyCount === 1 ? '' : 's'}
          </span>
        )}
        <Button type="button" disabled={dirtyCount === 0 || hasInvalidRow || pending} onClick={save}>
          {pending ? 'Saving…' : 'Save changes'}
        </Button>
      </div>
    </div>
  );
}
