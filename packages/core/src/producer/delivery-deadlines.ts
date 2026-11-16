import { INT_32_MAX_VALUE } from '../constants';
import { Deadline } from '../utils/deadline';

/** One pending send's delivery deadline, tracked by {@link DeliveryDeadlines}. */
export class DeliveryDeadline extends Deadline {
  /** `performance.now()` ms at which the deadline expires. */
  readonly at: number;
  readonly #onExpire: ((error: Error) => void) | undefined;

  constructor(at: number, onExpire: ((error: Error) => void) | undefined) {
    super();
    this.at = at;
    this.#onExpire = onExpire;
  }

  /** Expires the deadline, then passes `reason` to its `onExpire`. Calls after the first are ignored. */
  override expire(reason: Error): void {
    if (this.reason) return;
    super.expire(reason);
    this.#onExpire?.(reason);
  }
}

/**
 * The delivery deadlines of one producer's pending sends, expired by a single timer. A producer's
 * `deliveryTimeoutMs` is fixed, so its sends mostly reach their deadlines in the order they start:
 * the timer is armed for the earliest deadline and re-armed only when it fires or a send starts
 * with an earlier deadline, not per send. A settled send leaves the timer armed, so it can fire
 * before any deadline is due. While nothing is tracked the timer is unref'd, so an idle producer
 * doesn't keep the process alive.
 *
 * Deadlines are `performance.now()` ms, so a wall-clock jump moves none of them.
 */
export class DeliveryDeadlines {
  readonly #createError: () => Error;
  readonly #tracked = new Set<DeliveryDeadline>();
  /**
   * Whether `#tracked` iterates in deadline order, so a sweep can stop at the first deadline not
   * yet due instead of scanning every pending send each time the timer fires.
   */
  #isInDeadlineOrder = true;
  #lastTrackedAt = Number.NEGATIVE_INFINITY;
  #timer: ReturnType<typeof setTimeout> | undefined;
  /** When `#timer` fires; `Infinity` while it is not armed. */
  #timerAt = Number.POSITIVE_INFINITY;

  /** `createError` builds the error each expired deadline gets. */
  constructor(createError: () => Error) {
    this.#createError = createError;
  }

  /**
   * Tracks a deadline at `performance.now()` ms `at`. Once it passes, the deadline is expired with
   * a new error, which `onExpire` also receives.
   */
  track(at: number, onExpire?: (error: Error) => void): DeliveryDeadline {
    const deadline = new DeliveryDeadline(at, onExpire);
    this.#isInDeadlineOrder = this.#tracked.size === 0 || (this.#isInDeadlineOrder && at >= this.#lastTrackedAt);
    this.#lastTrackedAt = at;
    this.#tracked.add(deadline);

    if (at < this.#timerAt) {
      this.#arm(at);
    } else if (this.#tracked.size === 1) {
      this.#timer?.ref();
    }
    return deadline;
  }

  /** Stops tracking the deadline of a settled send. Does nothing once it expired. */
  untrack(deadline: DeliveryDeadline): void {
    if (this.#tracked.delete(deadline) && this.#tracked.size === 0) this.#timer?.unref();
  }

  #arm(at: number): void {
    clearTimeout(this.#timer);
    this.#timerAt = at;
    // Node fires a longer delay after 1 ms, so a far deadline is re-armed until it is due.
    const delay = Math.min(Math.max(0, at - performance.now()), INT_32_MAX_VALUE);
    this.#timer = setTimeout(this.#expireDue, delay);
  }

  #expireDue = (): void => {
    this.#timer = undefined;
    this.#timerAt = Number.POSITIVE_INFINITY;

    const now = performance.now();
    let nextAt = Number.POSITIVE_INFINITY;
    let previousAt = Number.NEGATIVE_INFINITY;
    let isRestInDeadlineOrder = true;
    for (const deadline of this.#tracked) {
      if (deadline.at <= now) {
        this.#tracked.delete(deadline);
        deadline.expire(this.#createError());
        continue;
      }
      if (this.#isInDeadlineOrder) {
        nextAt = deadline.at;
        break;
      }
      if (deadline.at < previousAt) isRestInDeadlineOrder = false;
      previousAt = deadline.at;
      nextAt = Math.min(nextAt, deadline.at);
    }
    if (isRestInDeadlineOrder) this.#isInDeadlineOrder = true;

    if (nextAt !== Number.POSITIVE_INFINITY) this.#arm(nextAt);
  };
}
