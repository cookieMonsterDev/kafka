export default async function config() {
  await Promise.resolve();
  return { client: { brokers: ['async-factory:9092'] } };
}
