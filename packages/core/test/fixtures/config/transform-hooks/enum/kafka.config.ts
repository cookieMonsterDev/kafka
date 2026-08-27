enum Level {
  Info = 'info',
  Warn = 'warn',
}

export default { client: { brokers: [`enum:${Level.Info}`] } };
