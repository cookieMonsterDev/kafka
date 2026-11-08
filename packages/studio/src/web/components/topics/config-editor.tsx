import { useId, useMemo, useState } from 'react';
import type { AlterTopicConfigsRequest, TopicConfigEntry } from '../../../shared/contracts/topic';
import { formatConfigDuration } from '../../lib/topic-config-display';
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
  const durationId = useId();
  const [search, setSearch] = useState('');
  const [overridesOnly, setOverridesOnly] = useState(false);
  const [edits, setEdits] = useState<PendingEdits>({});

  const dirtyCount = Object.keys(edits).length;
  const sorted = useMemo(() => configs.toSorted((a, b) => a.name.localeCompare(b.name)), [configs]);

  const visible = sorted.filter(
    (entry) =>
      entry.name.toLowerCase().includes(search.trim().toLowerCase()) &&
      (!overridesOnly || !entry.isDefault || edits[entry.name] !== undefined),
  );

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
      <div className="flex flex-wrap items-center gap-3">
        <label className="w-full sm:max-w-xs">
          <span className="sr-only">Filter configuration keys</span>
          <Input
            type="search"
            placeholder="Filter configuration keys…"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </label>
        <label className="flex min-h-8 items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={overridesOnly}
            onChange={(event) => setOverridesOnly(event.target.checked)}
            className="size-4 accent-primary focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
          />
          Overrides only
        </label>
        <p role="status" className="text-xs text-muted-foreground">
          {visible.length} of {configs.length} settings
        </p>
      </div>
      <div
        tabIndex={0}
        role="region"
        aria-label="Topic configuration, scroll horizontally for more columns"
        className="overflow-x-auto rounded-xl border border-border"
      >
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
            {visible.length === 0 && (
              <tr>
                <td colSpan={4} className="px-3 py-6 text-center text-sm text-muted-foreground">
                  No settings match these filters. Clear the search or turn off Overrides only.
                </td>
              </tr>
            )}
            {visible.map((entry) => {
              const isUnsetQueued = edits[entry.name] === null;
              const error = rowError(entry.name);
              const duration = formatConfigDuration(entry.name, currentValue(entry));
              const hintId = `${durationId}-${entry.name}`;
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
                      aria-describedby={duration === null ? undefined : hintId}
                    />
                    {duration !== null && (
                      <p id={hintId} className="mt-1 text-xs text-muted-foreground">
                        {duration}
                      </p>
                    )}
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
