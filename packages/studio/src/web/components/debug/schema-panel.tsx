import { useState } from 'react';
import { Field, type OperationProps } from './fields';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { Textarea } from '../ui/textarea';
import { apiGet, apiSend } from '../../lib/api';

export function SchemaPanel({
  running,
  canWrite,
  operate,
  setOutput,
  schemaBytes,
  setSchemaBytes,
  onStage,
}: OperationProps & {
  readonly schemaBytes: string;
  readonly setSchemaBytes: (value: string) => void;
  readonly onStage: (value: string) => void;
}) {
  const [subject, setSubject] = useState('');
  const [version, setVersion] = useState('latest');
  const [messageType, setMessageType] = useState('');
  const [schemaId, setSchemaId] = useState('');
  const [schemaPayload, setSchemaPayload] = useState('{}');
  return (
    <section aria-label="Schema Registry" className="flex flex-col gap-4 rounded-xl border border-border p-4">
      <p className="text-sm">
        Requires a registry configured for this profile. Supports Confluent-framed Avro, Protobuf, and JSON Schema.
        Encode validates before producing; no schemas are registered or changed.
      </p>
      <Button
        variant="outline"
        disabled={running}
        onClick={() =>
          void operate(async (signal) => {
            setOutput(await apiGet('/api/debug/schemas', signal));
          })
        }
      >
        List subjects
      </Button>
      <div className="grid gap-4 sm:grid-cols-2">
        <Field label="Subject">
          <Input value={subject} onChange={(event) => setSubject(event.target.value)} />
        </Field>
        <Field label="Version (number or latest)">
          <Input value={version} onChange={(event) => setVersion(event.target.value)} />
        </Field>
      </div>
      <div className="flex gap-2">
        <Button
          variant="outline"
          disabled={running || !subject}
          onClick={() =>
            void operate(async (signal) => {
              setOutput(await apiGet(`/api/debug/schemas?subject=${encodeURIComponent(subject)}`, signal));
            })
          }
        >
          List versions
        </Button>
        <Button
          variant="outline"
          disabled={running || !subject}
          onClick={() =>
            void operate(async (signal) => {
              const schema = await apiGet<{ id: number }>(
                `/api/debug/schemas?subject=${encodeURIComponent(subject)}&version=${encodeURIComponent(version)}`,
                signal,
              );
              setSchemaId(String(schema.id));
              setOutput(schema);
            })
          }
        >
          Load schema
        </Button>
      </div>
      <Field label="Protobuf message type (optional fully qualified name)">
        <Input value={messageType} onChange={(event) => setMessageType(event.target.value)} />
      </Field>
      <Field label="Schema ID">
        <Input type="number" min={1} value={schemaId} onChange={(event) => setSchemaId(event.target.value)} />
      </Field>
      <Field label="JSON payload to validate and encode">
        <Textarea value={schemaPayload} onChange={(event) => setSchemaPayload(event.target.value)} />
      </Field>
      <Button
        disabled={running}
        onClick={() =>
          void operate(async (signal) => {
            setSchemaBytes('');
            const encoded = await apiSend<{ value: string }>(
              'POST',
              '/api/debug/schemas/encode',
              {
                id: Number(schemaId),
                value: JSON.parse(schemaPayload) as unknown,
                messageType: messageType || undefined,
              },
              signal,
            );
            setSchemaBytes(encoded.value);
            setOutput(encoded);
          })
        }
      >
        Validate and encode
      </Button>
      <Field label="Confluent-framed payload (base64)">
        <Textarea value={schemaBytes} onChange={(event) => setSchemaBytes(event.target.value)} />
      </Field>
      <div className="flex flex-wrap gap-2">
        <Button
          variant="outline"
          disabled={running || !schemaBytes}
          onClick={() =>
            void operate(async (signal) => {
              setOutput(await apiSend('POST', '/api/debug/schemas/decode', { value: schemaBytes }, signal));
            })
          }
        >
          Decode payload
        </Button>
        <Button
          variant="outline"
          disabled={!schemaBytes || !canWrite}
          onClick={() => {
            onStage(JSON.stringify([{ key: null, value: schemaBytes, headers: {} }], null, 2));
          }}
        >
          Stage encoded payload for replay
        </Button>
      </div>
    </section>
  );
}
