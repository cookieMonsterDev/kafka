import { parentPort } from 'node:worker_threads';
import { createInterface } from 'node:readline';
import { PassThrough, Writable } from 'node:stream';

const port = parentPort;
if (port === null) throw new Error('The line editor must run in a terminal worker.');
class Output extends Writable {
  columns = 80;
  readonly isTTY = true;
  private pending = '';
  private scheduled = false;
  override _write(chunk: Buffer, _encoding: BufferEncoding, callback: () => void) {
    this.pending += chunk.toString('utf8');
    if (!this.scheduled) {
      this.scheduled = true;
      setImmediate(() => {
        port?.postMessage({ type: 'output', data: this.pending });
        this.pending = '';
        this.scheduled = false;
      });
    }
    callback();
  }
}
const input = new PassThrough();
const output = new Output();
let completionId = 0;
let recordHistory = true;
const completions = new Map<number, (error: null, result: [string[], string]) => void>();
const editor = createInterface({
  input,
  output,
  terminal: true,
  historySize: 50,
  completer: (line, callback) => {
    const id = completionId++;
    completions.set(id, callback);
    port.postMessage({ type: 'complete', id, line });
  },
});
editor.on('history', (history) => {
  if (!recordHistory) history.shift();
});
editor.on('line', (line) => port.postMessage({ type: 'line', line }));
editor.on('SIGINT', () => {
  editor.write(null, { ctrl: true, name: 'u' });
  output.write('^C\r\n');
  port.postMessage({ type: 'interrupt' });
});
editor.on('close', () => port.postMessage({ type: 'close' }));
port.on(
  'message',
  (message: {
    type: string;
    data?: string;
    columns?: number;
    id?: number;
    result?: [string[], string];
    history?: boolean;
  }) => {
    if (message.type === 'input') {
      if (editor.line.length + (message.data?.length ?? 0) > 32768) {
        editor.write(null, { ctrl: true, name: 'u' });
        output.write('\r\nCommand input exceeds 32768 characters.\r\n');
        editor.prompt();
      } else input.write(message.data ?? '');
    } else if (message.type === 'output') output.write(message.data ?? '');
    else if (message.type === 'prompt') {
      recordHistory = message.history !== false;
      editor.setPrompt(message.data ?? '');
      editor.prompt();
    } else if (message.type === 'set-prompt') {
      recordHistory = false;
      editor.setPrompt(message.data ?? '');
    } else if (message.type === 'resize') {
      output.columns = message.columns ?? 80;
      output.emit('resize');
    } else if (message.type === 'complete' && message.id !== undefined) {
      completions.get(message.id)?.(null, message.result ?? [[], '']);
      completions.delete(message.id);
    }
  },
);
