import { describe, expect, it } from 'vitest';
import { crc32c, crc32cJs, usesNativeCrc32c } from './crc32c';

describe('protocol/crc32c', () => {
  it('returns 0 for an empty buffer', () => {
    expect(crc32c(Buffer.alloc(0))).toBe(0);
  });

  it('matches the standard CRC-32C/Castagnoli check value for "123456789"', () => {
    // https://reveng.sourceforge.io/crc-catalogue/17plus.htm#crc.cat.crc-32c — check=0xe3069283
    expect(crc32c(Buffer.from('123456789'))).toBe(0xe3069283);
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

  it('JS table and public crc32c agree (native path when available)', () => {
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

  it('JS table matches the Castagnoli check value independently of native', () => {
    expect(crc32cJs(Buffer.from('123456789'))).toBe(0xe3069283);
    expect(typeof usesNativeCrc32c).toBe('boolean');
  });
});
