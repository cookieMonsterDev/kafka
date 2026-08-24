import { benchEncodeRecordBatch } from './encode-record-batch';
import { benchDecodeFetchPayload } from './decode-fetch-payload';
import { benchConnectionFraming } from './connection-framing';
import { benchEncoderBufferPool } from './encoder-buffer-pool';
import { benchProduceLinger } from './produce-linger';
import { benchConsumeHandlers } from './consume-handlers';

export async function main(): Promise<void> {
  console.log('kafka-core microbenches (local, not CI-gated)');
  console.log('No-network benches always run. Live produce/consume need KAFKA_EXTERNAL=1 or KAFKA_BROKERS.\n');

  await benchEncodeRecordBatch();
  console.log('');
  await benchDecodeFetchPayload();
  console.log('');
  await benchConnectionFraming();
  console.log('');
  await benchEncoderBufferPool();
  console.log('');
  await benchProduceLinger();
  console.log('');
  await benchConsumeHandlers();
}

const isDirect = process.argv[1] !== undefined && process.argv[1].includes('bench/run');
if (isDirect) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
