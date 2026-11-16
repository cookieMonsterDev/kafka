import { createRequire } from 'node:module';
import { describe, expect, it, type Mock, vi } from 'vitest';
import {
  createDeferredCrc32c,
  crc32c,
  crc32cJs,
  getCrc32cImplementation,
  NATIVE_CRC32C_MIN_BYTES,
  selectCrc32c,
} from './crc32c';

const requireFromHere = createRequire(import.meta.url);

/** False after an install without optional dependencies, where only the JS fallback can run. */
const isNodeRsInstalled = ((): boolean => {
  try {
    requireFromHere.resolve('@node-rs/crc32');
    return true;
  } catch {
    return false;
  }
})();

/** Bit-at-a-time CRC32C straight from the polynomial: slow, but shares nothing with the tables. */
function crc32cBitwise(buffer: Buffer): number {
  let crc = -1;
  for (const byte of buffer) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) {
      crc = (crc & 1) === 1 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1;
    }
  }
  return (crc ^ -1) >>> 0;
}

describe('protocol/crc32c', () => {
  it('returns 0 for an empty buffer', () => {
    expect(crc32c(Buffer.alloc(0))).toBe(0);
  });

  it('matches the standard CRC-32C/Castagnoli check value for "123456789"', () => {
    // https://reveng.sourceforge.io/crc-catalogue/17plus.htm#crc.cat.crc-32c — check=0xe3069283
    expect(crc32c(Buffer.from('123456789'))).toBe(0xe3069283);
  });

  it('matches the RFC 3720 appendix B.4 test vectors', () => {
    const ascending = Buffer.from(Array.from({ length: 32 }, (_, i) => i));
    const descending = Buffer.from(Array.from({ length: 32 }, (_, i) => 31 - i));
    expect(crc32cJs(Buffer.alloc(32, 0x00))).toBe(0x8a9136aa);
    expect(crc32cJs(Buffer.alloc(32, 0xff))).toBe(0x62a8ab43);
    expect(crc32cJs(ascending)).toBe(0x46dd794e);
    expect(crc32cJs(descending)).toBe(0x113fdb5c);
  });

  it('JS implementation matches a bit-at-a-time reference for every length and offset near a block', () => {
    const source = Buffer.from(Array.from({ length: 200 }, (_, i) => (i * 151 + 7) & 0xff));
    for (let offset = 0; offset < 4; offset++) {
      for (let length = 0; length <= 70; length++) {
        const sample = source.subarray(offset, offset + length);
        expect(crc32cJs(sample)).toBe(crc32cBitwise(sample));
      }
    }
  });

  it('matches a known-answer CRC32C vector', () => {
    const input =
      '2AGBW7dAQSw0TeE3ZekW4HZgUDVs0NgLvJzExMJRXzZuMX6mAUb2eiT9f0BhLS9ekDNcmG90JQTuQ00Uf1hZg4eis4Tl9LVRgWfH9tupHIHMGx6gFTxLz3cqdIgz1r1bHorXn9pUm1iQTU9qR3Udhm1v25P6ZRInegmEROiIMU1CKk9UpOD7UgoumQMvgLH0UAPo2XuzSf5l70lkSSn3osm64T7At0lPO25OOqiiaC09vTERsolr5VhlqkKZTF0OqrubahE6srN0sQuGxHa6PaLahL74k16F5VpR39u4mgtpVqZ1ceE7ckB6q2IGqvnfw7P7Ja0uvOsNOWS6VxnpATDS9knnTgyfHCCxQzsl25lrqpT2R8ZKfzREBHlMuA0zpeTr3UQgiOPpD5xoNkh7kTaCMkgtqvuvlTsw2VhbVNNrlP3PNSNau28vIc5i3BVxqEpiNS7fkC1Z44oGxTRliQorUQJWd8MmBnCMh5kHTamBgKEsTrTyLhBzUzoQbZST3dHZrF5BguClmM32poQ7iSBhtXKqyzK9XOfZZpoRT9A3drsZUFVkvPSCw5SJqTAwU1eJ28SsJkWDwAK8WD72tG1p5sT7ohbUy1Szla7gdi7cIwF6QV0CwGJzkURZwIVi8uDT7eaBNwkCl0uA8LugqAACxoexiiKJ6HP5zei7iTUhMfdRk1uyFD9DzZ9Mzsr4GnCVyqQbRHllwOu8Z7tXWVmNbZsLtNF57rQfm780kh13aSo49gwUT979TsG2W6fEz2JrKWHcE3iS5yS0r2AOOltvDMMfZR3HCbQyCVPKN5GtDE6uFkLz6FsTC0tauH9inW2wycQzTVhggLzyqlgG0pNJlnd30nvtt1dZUcolCfUmUNMsmbx5zJrPHbU0fXPzonMvpDWFHUe1Ib9Kze82L5nWBilcSnLqSPzf0IqeLRTD0PUGAEWJM3kGEfpoeJNvIADrlo2bDa0u8Im7otPAp7K5mmJadqAVdAUpbDz0aIR1WqSBq6ESvial6RC72Uq0';
    expect(crc32c(Buffer.from(input))).toBe(3605965599);
  });

  it('handles payloads larger than 64 KiB', () => {
    const large = Buffer.alloc(70 * 1024, 'a');
    expect(Number.isInteger(crc32c(large))).toBe(true);
    expect(crc32c(large)).toBe(crc32c(Buffer.from(large)));
  });

  it('is sensitive to every byte (changing one byte changes the checksum)', () => {
    const a = Buffer.from('kafka-core');
    const b = Buffer.from('kafka-core');
    b[0] = (b[0] as number) + 1;
    expect(crc32c(a)).not.toBe(crc32c(b));
  });

  it('JS table and public crc32c agree', () => {
    const samples = [
      Buffer.alloc(0),
      Buffer.from('123456789'),
      Buffer.from('kafka-core'),
      Buffer.alloc(70 * 1024, 'a'),
      Buffer.from([0xff, 0x00, 0x01, 0x80, 0x7f]),
    ];
    for (const sample of samples) {
      expect(crc32c(sample)).toBe(crc32cJs(sample));
    }
  });

  it.skipIf(!isNodeRsInstalled)('reports the native implementation when the optional package is installed', () => {
    expect(getCrc32cImplementation()).toBe('native');
  });
});

describe('protocol/crc32c implementation selection', () => {
  const ascending = Buffer.from(Array.from({ length: 32 }, (_, i) => i));
  const offsetSource = Buffer.from(Array.from({ length: 200 }, (_, i) => (i * 151 + 7) & 0xff));
  const knownVectors: [Buffer, number][] = [
    [Buffer.alloc(0), 0],
    [Buffer.from('123456789'), 0xe3069283],
    [Buffer.alloc(32, 0x00), 0x8a9136aa],
    [Buffer.alloc(32, 0xff), 0x62a8ab43],
    [ascending, 0x46dd794e],
    [Buffer.from(ascending).reverse(), 0x113fdb5c],
  ];

  function expectKnownVectors(checksum: (buffer: Buffer) => number): void {
    for (const [input, expected] of knownVectors) {
      expect(checksum(input)).toBe(expected);
    }
    // A subarray that doesn't start at its backing buffer's first byte, as batch.ts passes.
    for (let offset = 1; offset < 4; offset++) {
      const sample = offsetSource.subarray(offset, offset + 150);
      expect(checksum(sample)).toBe(crc32cJs(sample));
    }
  }

  it.skipIf(!isNodeRsInstalled)('uses @node-rs/crc32 when it loads, with the same checksums as the JS table', () => {
    const selected = selectCrc32c(requireFromHere);
    expect(selected.implementation).toBe('native');
    expect(selected.checksum).not.toBe(crc32cJs);
    expectKnownVectors(selected.checksum);
  });

  it('falls back to the JS table when loading @node-rs/crc32 fails', () => {
    const selected = selectCrc32c((id) => {
      throw new Error(`Cannot find module '${id}'`);
    });
    expect(selected).toEqual({ implementation: 'javascript', checksum: crc32cJs });
    expectKnownVectors(selected.checksum);
  });

  it.each([
    ['is not an object', null],
    ['has no crc32c function', {}],
    ['computes a different CRC (IEEE CRC-32 check value)', { crc32c: () => 0xcbf43926 }],
    [
      'throws from crc32c',
      {
        crc32c: () => {
          throw new Error('binding failed');
        },
      },
    ],
  ])('falls back to the JS table when the loaded module %s', (_case, loaded) => {
    expect(selectCrc32c(() => loaded).implementation).toBe('javascript');
  });
});

describe('protocol/crc32c deferred native loading', () => {
  const checkInput = Buffer.from('123456789');
  const smallBatches = [Buffer.alloc(0), checkInput, Buffer.alloc(NATIVE_CRC32C_MIN_BYTES - 1, 0x5a)];
  const largeBatch = Buffer.from(Array.from({ length: NATIVE_CRC32C_MIN_BYTES }, (_, i) => (i * 151 + 7) & 0xff));

  /** A binding that passes the check-value test and counts its calls, standing in for `@node-rs/crc32`. */
  function fakeBinding(): { requireModule: Mock<(id: string) => unknown>; nativeCrc32c: Mock<typeof crc32cJs> } {
    const nativeCrc32c = vi.fn(crc32cJs);
    const requireModule = vi.fn((_id: string): unknown => ({ crc32c: nativeCrc32c }));
    return { requireModule, nativeCrc32c };
  }

  it('checksums batches below the threshold without loading @node-rs/crc32', () => {
    const { requireModule } = fakeBinding();
    const deferred = createDeferredCrc32c(requireModule);
    for (const batch of smallBatches) {
      expect(deferred.checksum(batch)).toBe(crc32cJs(batch));
    }
    expect(requireModule).not.toHaveBeenCalled();
  });

  it('loads @node-rs/crc32 on the first batch at the threshold and uses it for every batch after', () => {
    const { requireModule, nativeCrc32c } = fakeBinding();
    const deferred = createDeferredCrc32c(requireModule);
    deferred.checksum(checkInput);

    expect(deferred.checksum(largeBatch)).toBe(crc32cJs(largeBatch));
    expect(requireModule).toHaveBeenCalledTimes(1);
    expect(nativeCrc32c).toHaveBeenLastCalledWith(largeBatch);

    for (const batch of smallBatches) {
      expect(deferred.checksum(batch)).toBe(crc32cJs(batch));
      expect(nativeCrc32c).toHaveBeenLastCalledWith(batch);
    }
    expect(deferred.resolveImplementation()).toBe('native');
    expect(requireModule).toHaveBeenCalledTimes(1);
  });

  it('loads @node-rs/crc32 when resolveImplementation() is called before any large batch', () => {
    const { requireModule, nativeCrc32c } = fakeBinding();
    const deferred = createDeferredCrc32c(requireModule);

    expect(deferred.resolveImplementation()).toBe('native');
    expect(requireModule).toHaveBeenCalledTimes(1);
    deferred.checksum(checkInput);
    expect(nativeCrc32c).toHaveBeenLastCalledWith(checkInput);
  });

  it('keeps the JS table and tries to load only once when @node-rs/crc32 is missing', () => {
    const requireModule = vi.fn((id: string): unknown => {
      throw new Error(`Cannot find module '${id}'`);
    });
    const deferred = createDeferredCrc32c(requireModule);

    expect(deferred.checksum(largeBatch)).toBe(crc32cJs(largeBatch));
    expect(deferred.checksum(largeBatch)).toBe(crc32cJs(largeBatch));
    for (const batch of smallBatches) {
      expect(deferred.checksum(batch)).toBe(crc32cJs(batch));
    }
    expect(deferred.resolveImplementation()).toBe('javascript');
    expect(requireModule).toHaveBeenCalledTimes(1);
  });

  it.skipIf(!isNodeRsInstalled)('gives the same checksums before and after switching to @node-rs/crc32', () => {
    const deferred = createDeferredCrc32c(requireFromHere);
    const batches = [...smallBatches, largeBatch];
    const before = smallBatches.map((batch) => deferred.checksum(batch));
    const after = batches.map((batch) => deferred.checksum(batch));

    expect(deferred.resolveImplementation()).toBe('native');
    expect(before).toEqual(smallBatches.map(crc32cJs));
    expect(after).toEqual(batches.map(crc32cJs));
  });
});
