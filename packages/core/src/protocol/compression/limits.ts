/**
 * Cap on decompressed record-batch size. Crafted or oversized compressed payloads that expand
 * past this limit are rejected instead of exhausting memory.
 */
export const MAX_DECOMPRESSED_SIZE = 100 * 1024 * 1024;
