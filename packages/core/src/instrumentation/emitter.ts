import { EventEmitter } from 'node:events';
import { KafkaError } from '../errors';
import { InstrumentationEvent } from './event';

export type RemoveInstrumentationEventListener = () => void;

export type InstrumentationPayload<T> = T | (() => T);

function resolvePayload<T>(payload: InstrumentationPayload<T>): T {
  return typeof payload === 'function' ? (payload as () => T)() : payload;
}

/**
 * Typed wrapper over `node:events`. `EventMap` maps each event name to its payload type, so
 * `emitter.on(SOME_EVENT, event => ...)` infers `event.payload` from the event name.
 *
 * Payload builders (`() => payload`) run only when `listenerCount > 0`, so unused instrumentation
 * does not allocate event fields. Object payloads remain supported for existing callers.
 */
export class InstrumentationEventEmitter<EventMap extends object = Record<string, unknown>> {
  #emitter = new EventEmitter();

  listenerCount<EventName extends keyof EventMap & string>(eventName: EventName): number {
    return this.#emitter.listenerCount(eventName);
  }

  hasListeners<EventName extends keyof EventMap & string>(eventName: EventName): boolean {
    return this.#emitter.listenerCount(eventName) > 0;
  }

  emit<EventName extends keyof EventMap & string>(
    eventName: EventName,
    payload: InstrumentationPayload<EventMap[EventName]>,
  ): void {
    if (!eventName) {
      throw new KafkaError('Invalid event name', { retriable: false });
    }

    if (this.#emitter.listenerCount(eventName) > 0) {
      const event = new InstrumentationEvent(eventName, resolvePayload(payload));
      this.#emitter.emit(eventName, event);
    }
  }

  addListener<EventName extends keyof EventMap & string>(
    eventName: EventName,
    listener: (event: InstrumentationEvent<EventMap[EventName]>) => void,
  ): RemoveInstrumentationEventListener {
    this.#emitter.addListener(eventName, listener);
    return () => this.#emitter.removeListener(eventName, listener);
  }
}
