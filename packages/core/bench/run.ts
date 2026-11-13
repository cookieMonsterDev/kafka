import { benchEncodeRecordBatch } from './encode-record-batch';
import { benchDecodeFetchPayload } from './decode-fetch-payload';
import { benchConnectionFraming } from './connection-framing';
import { benchEncoderBufferPool } from './encoder-buffer-pool';
import { benchRecordDecode } from './record-decode';
import { benchProduceLinger } from './produce-linger';
import { benchConsumeHandlers } from './consume-handlers';
import { benchCrc32c } from './crc32c';
import { benchCompressionZlib } from './compression-zlib';

export async function main(): Promise<void> {
  console.log('kafka-core microbenches (local, not CI-gated)');
  console.log('No-network benches always run. Live produce/consume need KAFKA_EXTERNAL=1 or KAFKA_BROKERS.\n');

  await benchEncodeRecordBatch();
  console.log('');
  await benchDecodeFetchPayload();
  console.log('');
  await benchRecordDecode();
  console.log('');
  await benchConnectionFraming();
  console.log('');
  await benchEncoderBufferPool();
  console.log('');
  await benchProduceLinger();
  console.log('');
  await benchConsumeHandlers();
  console.log('');
  await benchCrc32c();
  console.log('');
  await benchCompressionZlib();
}

const isDirect = process.argv[1] !== undefined && process.argv[1].includes('bench/run');
if (isDirect) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
