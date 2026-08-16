import { EventEmitter } from 'node:events';
import { KafkaError } from '../errors';
import { InstrumentationEvent } from './event';

export type RemoveInstrumentationEventListener = () => void;

/**
 * Typed wrapper over `node:events`. `EventMap` maps each event name to its payload type, so
 * `emitter.on(SOME_EVENT, event => ...)` infers `event.payload` from the event name.
 */
export class InstrumentationEventEmitter<EventMap extends object = Record<string, unknown>> {
  #emitter = new EventEmitter();

  emit<EventName extends keyof EventMap & string>(eventName: EventName, payload: EventMap[EventName]): void {
    if (!eventName) {
      throw new KafkaError('Invalid event name', { retriable: false });
    }

    if (this.#emitter.listenerCount(eventName) > 0) {
      const event = new InstrumentationEvent(eventName, payload);
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
