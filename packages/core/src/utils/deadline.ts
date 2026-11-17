/**
 * A deadline that its owner expires, usually from one timer shared by many deadlines; it holds no
 * time of its own. Work reads `reason` before it starts a step. A wait that must end at the
 * deadline takes `signal`, whose `AbortController` is created on first use, so work that never
 * waits allocates none.
 */
export class Deadline {
  #reason: Error | undefined;
  #controller: AbortController | undefined;

  /** The error passed to `expire`, or `undefined` while the deadline has not passed. */
  get reason(): Error | undefined {
    return this.#reason;
  }

  /** Aborted with `reason` once the deadline expires, or already aborted if it has. */
  get signal(): AbortSignal {
    if (!this.#controller) {
      this.#controller = new AbortController();
      if (this.#reason) this.#controller.abort(this.#reason);
    }
    return this.#controller.signal;
  }

  /** Marks the deadline as passed and aborts `signal`. Calls after the first are ignored. */
  expire(reason: Error): void {
    if (this.#reason) return;
    this.#reason = reason;
    this.#controller?.abort(reason);
  }
}
