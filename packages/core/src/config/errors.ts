/** Reason a config file could not be discovered, loaded, or parsed. */
export type ConfigErrorTag = 'ConfigFileNotFound' | 'ConfigLoadError' | 'ConfigFileInvalid' | 'UnsupportedExtension';

export interface KafkaConfigErrorOptions {
  path?: string;
  cause?: unknown;
}

/**
 * Raised while discovering, loading, or parsing a `kafka.config.*` file. `tag` names the specific
 * failure so callers can branch without parsing `.message`.
 */
export class KafkaConfigError extends Error {
  override readonly name = 'KafkaConfigError';
  readonly tag: ConfigErrorTag;
  readonly path: string | undefined;

  constructor(tag: ConfigErrorTag, message: string, options: KafkaConfigErrorOptions = {}) {
    super(message, { cause: options.cause });
    this.tag = tag;
    this.path = options.path;
  }
}

/**
 * A config file could not be loaded synchronously because it (or a module it imports) uses
 * top-level `await`, which fails fast with Node's `ERR_REQUIRE_ASYNC_MODULE`. Use
 * `Kafka.fromConfig()`, or the async config loader directly, instead of the synchronous path.
 */
export class KafkaConfigRequiresAsyncError extends Error {
  override readonly name = 'KafkaConfigRequiresAsyncError';
  readonly path: string;

  constructor(path: string, options: { cause?: unknown } = {}) {
    super(
      `kafka config file "${path}" requires an async loader (it uses top-level await, directly or through an ` +
        'import). Use Kafka.fromConfig() instead of `new Kafka()`, or load the file with the async config loader.',
      { cause: options.cause },
    );
    this.path = path;
  }
}
