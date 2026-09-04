import { useState } from 'react';
import { useForm } from '@tanstack/react-form';
import { z } from 'zod';
import { topicNameSchema } from '../../../shared/contracts/topic';
import { Button } from '../ui/button';
import { Input } from '../ui/input';

interface ConfigEntryRow {
  readonly id: string;
  readonly key: string;
  readonly value: string;
}

/** Empty (defer to the broker default) or a positive whole number — the string form of `z.number().int().positive()`. */
const optionalPositiveIntegerSchema = z
  .string()
  .refine((value) => value.trim() === '' || (/^\d+$/.test(value.trim()) && Number(value.trim()) > 0), {
    message: 'must be a positive whole number',
  });

function fieldErrorMessage(errors: readonly unknown[]): string {
  return errors
    .map((error) =>
      typeof error === 'string'
        ? error
        : typeof error === 'object' && error !== null && 'message' in error
          ? String(error.message)
          : String(error),
    )
    .join(', ');
}

export interface CreateTopicFormValues {
  readonly topic: string;
  readonly numPartitions?: number;
  readonly replicationFactor?: number;
  readonly configEntries?: Record<string, string>;
}

export interface CreateTopicFormProps {
  readonly onSubmit: (values: CreateTopicFormValues) => void;
  readonly onCancel: () => void;
  readonly pending?: boolean;
}

let nextRowId = 0;

/**
 * The core fields (name, partitions, replication factor) are validated with TanStack Form against
 * {@link topicNameSchema} — the same schema `POST /api/topics` validates the request body with, so
 * a topic name the form accepts is one the server will too. Config entries are plain local state:
 * a dynamic key/value list doesn't need form-library array plumbing to stay correct.
 */
export function CreateTopicForm({ onSubmit, onCancel, pending = false }: CreateTopicFormProps) {
  const [configRows, setConfigRows] = useState<ConfigEntryRow[]>([]);

  const form = useForm({
    defaultValues: { topic: '', numPartitions: '', replicationFactor: '' },
    onSubmit: ({ value }) => {
      const configEntries = Object.fromEntries(
        configRows.filter((row) => row.key.trim().length > 0).map((row) => [row.key.trim(), row.value]),
      );
      onSubmit({
        topic: value.topic,
        ...(value.numPartitions.trim() !== '' ? { numPartitions: Number(value.numPartitions) } : {}),
        ...(value.replicationFactor.trim() !== '' ? { replicationFactor: Number(value.replicationFactor) } : {}),
        ...(Object.keys(configEntries).length > 0 ? { configEntries } : {}),
      });
    },
  });

  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        event.stopPropagation();
        void form.handleSubmit();
      }}
      className="flex flex-col gap-4"
    >
      <form.Field name="topic" validators={{ onChange: topicNameSchema }}>
        {(field) => (
          <div className="flex flex-col gap-1.5">
            <label htmlFor={field.name} className="text-sm font-medium">
              Topic name
            </label>
            <Input
              id={field.name}
              name={field.name}
              value={field.state.value}
              onBlur={field.handleBlur}
              onChange={(event) => field.handleChange(event.target.value)}
              aria-invalid={field.state.meta.errors.length > 0}
              autoFocus
            />
            {field.state.meta.errors.length > 0 && (
              <p className="text-sm text-destructive" role="alert">
                {fieldErrorMessage(field.state.meta.errors)}
              </p>
            )}
          </div>
        )}
      </form.Field>

      <div className="grid grid-cols-2 gap-4">
        <form.Field name="numPartitions" validators={{ onChange: optionalPositiveIntegerSchema }}>
          {(field) => (
            <div className="flex flex-col gap-1.5">
              <label htmlFor={field.name} className="text-sm font-medium">
                Partitions
              </label>
              <Input
                id={field.name}
                name={field.name}
                type="number"
                min={1}
                step={1}
                value={field.state.value}
                onBlur={field.handleBlur}
                onChange={(event) => field.handleChange(event.target.value)}
                placeholder="broker default"
                aria-invalid={field.state.meta.errors.length > 0}
              />
              {field.state.meta.errors.length > 0 && (
                <p className="text-sm text-destructive" role="alert">
                  {fieldErrorMessage(field.state.meta.errors)}
                </p>
              )}
            </div>
          )}
        </form.Field>

        <form.Field name="replicationFactor" validators={{ onChange: optionalPositiveIntegerSchema }}>
          {(field) => (
            <div className="flex flex-col gap-1.5">
              <label htmlFor={field.name} className="text-sm font-medium">
                Replication factor
              </label>
              <Input
                id={field.name}
                name={field.name}
                type="number"
                min={1}
                step={1}
                value={field.state.value}
                onBlur={field.handleBlur}
                onChange={(event) => field.handleChange(event.target.value)}
                placeholder="broker default"
                aria-invalid={field.state.meta.errors.length > 0}
              />
              {field.state.meta.errors.length > 0 && (
                <p className="text-sm text-destructive" role="alert">
                  {fieldErrorMessage(field.state.meta.errors)}
                </p>
              )}
            </div>
          )}
        </form.Field>
      </div>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <span className="text-sm font-medium">Config overrides</span>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => {
              nextRowId += 1;
              setConfigRows((rows) => [...rows, { id: `row-${String(nextRowId)}`, key: '', value: '' }]);
            }}
          >
            Add entry
          </Button>
        </div>
        {configRows.map((row) => (
          <div key={row.id} className="flex items-center gap-2">
            <Input
              aria-label="config key"
              placeholder="retention.ms"
              value={row.key}
              onChange={(event) => {
                const key = event.target.value;
                setConfigRows((rows) => rows.map((r) => (r.id === row.id ? { ...r, key } : r)));
              }}
            />
            <Input
              aria-label="config value"
              placeholder="604800000"
              value={row.value}
              onChange={(event) => {
                const value = event.target.value;
                setConfigRows((rows) => rows.map((r) => (r.id === row.id ? { ...r, value } : r)));
              }}
            />
            <Button
              type="button"
              variant="ghost"
              size="icon"
              aria-label="remove config entry"
              onClick={() => setConfigRows((rows) => rows.filter((r) => r.id !== row.id))}
            >
              <svg
                className="size-4"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                aria-hidden="true"
              >
                <path d="M18 6 6 18" />
                <path d="m6 6 12 12" />
              </svg>
            </Button>
          </div>
        ))}
      </div>

      <div className="flex justify-end gap-2">
        <Button type="button" variant="outline" onClick={onCancel}>
          Cancel
        </Button>
        <form.Subscribe selector={(state) => [state.canSubmit] as const}>
          {([canSubmit]) => (
            <Button type="submit" disabled={!canSubmit || pending}>
              {pending ? 'Creating…' : 'Create topic'}
            </Button>
          )}
        </form.Subscribe>
      </div>
    </form>
  );
}
