let nextEventId = 0

function nextId(): number {
  if (nextEventId === Number.MAX_VALUE) {
    nextEventId = 0
  }
  return nextEventId++
}

export class InstrumentationEvent<Payload> {
  readonly id: number = nextId()
  readonly timestamp: number = Date.now()
  readonly type: string
  readonly payload: Payload

  constructor(type: string, payload: Payload) {
    this.type = type
    this.payload = payload
  }
}
