import { assertOk } from './api';
import { withAuthHeaders } from './auth';

/** Fetch-based SSE supports POST bodies, auth headers, and explicit cancellation. */
export async function streamDebug(
  path: string,
  body: unknown,
  signal: AbortSignal,
  onEvent: (event: string, value: unknown) => void,
): Promise<void> {
  const response = await fetch(path, {
    method: 'POST',
    headers: withAuthHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify(body),
    signal,
  });
  await assertOk(response, path);
  if (!response.body) throw new Error('Missing operation stream');
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = '';
  let finished = false;
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      pending += decoder.decode(chunk.value, { stream: true });
      let boundary = pending.indexOf('\n\n');
      while (boundary >= 0) {
        const frame = pending.slice(0, boundary);
        pending = pending.slice(boundary + 2);
        const event = frame
          .split('\n')
          .find((line) => line.startsWith('event: '))
          ?.slice(7);
        const data = frame
          .split('\n')
          .find((line) => line.startsWith('data: '))
          ?.slice(6);
        if (event && data) {
          const value: unknown = JSON.parse(data);
          if (event === 'operation-error') throw new Error((value as { message: string }).message);
          if (event === 'result' || event === 'done') finished = true;
          onEvent(event, value);
        }
        boundary = pending.indexOf('\n\n');
      }
    }
    if (!finished)
      throw new Error('Operation disconnected before completion; acknowledged writes may already have reached Kafka');
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export function downloadJson(name: string, value: unknown): void {
  const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], { type: 'application/json' }));
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = name;
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

export function encodeText(value: string): string {
  const bytes = new TextEncoder().encode(value);
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}
