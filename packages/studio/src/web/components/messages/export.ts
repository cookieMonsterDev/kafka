import type { MessageRecord } from '../../../shared/contracts/message';
import { decodeMessageField, type MessageDecoder } from '../../lib/decode';

function toExportLine(message: MessageRecord, decoder: MessageDecoder): string {
  const headers = Object.fromEntries(
    Object.entries(message.headers).map(([key, value]) => [key, decodeMessageField(value, decoder).text]),
  );
  return JSON.stringify({
    partition: message.partition,
    offset: message.offset,
    timestamp: message.timestamp,
    key: message.key === null ? null : decodeMessageField(message.key, decoder).text,
    value: message.value === null ? null : decodeMessageField(message.value, decoder).text,
    headers,
  });
}

/** Newline-delimited JSON — one decoded record per line. */
export function toJsonl(messages: readonly MessageRecord[], decoder: MessageDecoder): string {
  return messages.map((message) => toExportLine(message, decoder)).join('\n');
}

/** Triggers a browser download of `messages` as `.jsonl`, decoded per the viewer's current choice of decoder. */
export function downloadMessagesAsJsonl(
  messages: readonly MessageRecord[],
  decoder: MessageDecoder,
  filename: string,
): void {
  const blob = new Blob([toJsonl(messages, decoder)], { type: 'application/x-ndjson' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.click();
  // Deferred: revoking synchronously can invalidate the URL before the download starts.
  setTimeout(() => URL.revokeObjectURL(url), 0);
}
