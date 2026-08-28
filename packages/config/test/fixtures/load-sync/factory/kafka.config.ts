let calls = 0;

export default function config() {
  calls += 1;
  return { client: { brokers: [`call-${calls}:9092`] } };
}
