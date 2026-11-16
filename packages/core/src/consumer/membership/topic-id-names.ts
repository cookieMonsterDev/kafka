import { KafkaNonRetriableError } from '../../errors';

/** A topic id with its name, as a ConsumerGroupDescribe or ShareGroupDescribe response reports it. */
export interface DescribedTopic {
  topicId: Buffer;
  topicName: string | null;
}

export interface TopicIdNamesOptions {
  /** The topic names the member subscribed to. */
  subscribedTopics: () => readonly string[];
  /** Whether the member also subscribed with a pattern, so the broker can assign topics it did not name. */
  hasPattern: boolean;
  /** Describes the group and returns the topics of this member's assignments. */
  describeAssignedTopics: () => Promise<Iterable<DescribedTopic>>;
}

function topicIdKey(topicId: Buffer): string {
  return topicId.toString('hex');
}

/**
 * Maps the topic ids of a heartbeat assignment (KIP-848, KIP-932) to topic names. A member that
 * subscribed to exactly one topic name and no pattern can only be assigned that topic; any other
 * member describes the group to learn the names.
 */
export class TopicIdNames {
  readonly #namesById = new Map<string, string>();
  readonly #options: TopicIdNamesOptions;

  constructor(options: TopicIdNamesOptions) {
    this.#options = options;
  }

  async resolve(topicId: Buffer): Promise<string> {
    const key = topicIdKey(topicId);
    const cached = this.#namesById.get(key);
    if (cached) return cached;

    const subscribedTopics = this.#options.subscribedTopics();
    const [onlyTopic] = subscribedTopics;
    if (subscribedTopics.length === 1 && onlyTopic != null && !this.#options.hasPattern) {
      this.#namesById.set(key, onlyTopic);
      return onlyTopic;
    }

    for (const { topicId: describedId, topicName } of await this.#options.describeAssignedTopics()) {
      if (topicName) this.#namesById.set(topicIdKey(describedId), topicName);
    }
    const resolved = this.#namesById.get(key);
    if (resolved) return resolved;

    throw new KafkaNonRetriableError(`Unable to resolve topic id ${key} to a subscribed topic name`);
  }
}
