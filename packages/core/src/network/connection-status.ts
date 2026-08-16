export const CONNECTION_STATUS = Object.freeze({
  CONNECTED: 'connected',
  DISCONNECTING: 'disconnecting',
  DISCONNECTED: 'disconnected',
});

export type ConnectionStatus = (typeof CONNECTION_STATUS)[keyof typeof CONNECTION_STATUS];

export const CONNECTED_STATUS: readonly ConnectionStatus[] = Object.freeze([
  CONNECTION_STATUS.CONNECTED,
  CONNECTION_STATUS.DISCONNECTING,
]);
