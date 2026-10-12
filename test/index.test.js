import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RingBuffer, ProtocolParser, AppendOnlyLog, StreamPipeline, TokenBucket } from '../src/index.js';

test('RingBuffer push/pop works as lock‑free queue', () => {
  const rb = new RingBuffer(8);
  for (let i = 0; i < 7; i++) assert.equal(rb.push(i), true);
  assert.equal(rb.push(99), false); // full
  for (let i = 0; i < 7; i++) assert.equal(rb.pop(), i);
  assert.equal(rb.pop(), null);
});

test('ProtocolParser emits length‑prefixed frames', () => {
  const frames = [];
  const parser = new ProtocolParser((f) => frames.push(f));
  const makeChunk = (arr) => new Uint8Array(arr);
  // two frames: [len=3, 'abc'], [len=4, 'defg']
  const data = [];
  const view = new DataView(new ArrayBuffer(4));
  view.setUint32(0, 3, false);
  data.push(...new Uint8Array(view.buffer), 97, 98, 99);
  view.setUint32(0, 4, false);
  data.push(...new Uint8Array(view.buffer), 100, 101, 102, 103);
  parser.feed(makeChunk(data.slice(0, 5)));
  parser.feed(makeChunk(data.slice(5)));
  assert.equal(frames.length, 2);
  assert.deepEqual(Array.from(frames[0]), [97, 98, 99]);
  assert.deepEqual(Array.from(frames[1]), [100, 101, 102, 103]);
});

test('AppendOnlyLog stores entries and Bloom filter works', () => {
  const log = new AppendOnlyLog(64);
  const a = new Uint8Array([1, 2, 3]);
  const b = new Uint8Array([4, 5, 6]);
  log.append(a);
  log.append(b);
  assert.equal(log.get(0), a);
  assert.equal(log.get(1), b);
  assert.equal(log.maybeHas(a), true);
  const c = new Uint8Array([7, 8, 9]);
  // false is guaranteed only if bits are not set; may be true false‑positive
  assert.equal(log.maybeHas(c), false);
});

test('StreamPipeline transforms async iterable', async () => {
  async function* source() {
    yield 1; yield 2; yield 3;
  }
  const pipeline = new StreamPipeline(source(), async (n) => n * 2);
  const results = [];
  for await (const v of pipeline) results.push(v);
  assert.deepEqual(results, [2, 4, 6]);
});

test('TokenBucket rate limits correctly', () => {
  const bucket = new TokenBucket(5, 0.01); // 10 tokens per second
  // consume initial burst
  for (let i = 0; i < 5; i++) assert.equal(bucket.tryConsume(), true);
  assert.equal(bucket.tryConsume(), false);
  // wait 200ms -> ~2 tokens
  const start = Date.now();
  while (Date.now() - start < 200) {}
  assert.equal(bucket.tryConsume(2), true);
});
