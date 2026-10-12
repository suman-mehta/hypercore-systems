/**
 * hypercore-systems – core high‑performance primitives.
 *
 * @module hypercore-systems
 */

/**
 * Lock‑free ring buffer based on SharedArrayBuffer and Atomics.
 * Supports single‑producer / single‑consumer pattern.
 */
export class RingBuffer {
  /**
   * @param {number} capacity Must be a power of two.
   */
  constructor(capacity) {
    if (capacity <= 0 || (capacity & (capacity - 1)) !== 0) {
      throw new RangeError('capacity must be a power of two');
    }
    this._mask = capacity - 1;
    this._sab = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 2 + Uint8Array.BYTES_PER_ELEMENT * capacity);
    this._head = new Int32Array(this._sab, 0, 1);
    this._tail = new Int32Array(this._sab, Int32Array.BYTES_PER_ELEMENT, 1);
    this._data = new Uint8Array(this._sab, Int32Array.BYTES_PER_ELEMENT * 2);
  }

  /**
   * Push a byte (0‑255) into the buffer.
   * @param {number} byte
   * @returns {boolean} true if written, false if full.
   */
  push(byte) {
    const head = Atomics.load(this._head, 0);
    const tail = Atomics.load(this._tail, 0);
    if (((head + 1) & this._mask) === tail) return false; // full
    this._data[head] = byte;
    Atomics.store(this._head, 0, (head + 1) & this._mask);
    return true;
  }

  /**
   * Pop a byte from the buffer.
   * @returns {number|null} byte or null if empty.
   */
  pop() {
    const tail = Atomics.load(this._tail, 0);
    const head = Atomics.load(this._head, 0);
    if (tail === head) return null; // empty
    const byte = this._data[tail];
    Atomics.store(this._tail, 0, (tail + 1) & this._mask);
    return byte;
  }
}

/**
 * Zero‑copy binary protocol parser. Example: simple length‑prefixed frames.
 */
export class ProtocolParser {
  /**
   * @param {function(Uint8Array): void} onFrame callback invoked for each complete frame.
   */
  constructor(onFrame) {
    if (typeof onFrame !== 'function') throw new TypeError('onFrame must be a function');
    this._onFrame = onFrame;
    this._buffer = new Uint8Array(0);
  }

  /**
   * Feed raw bytes; frames are emitted via the callback.
   * @param {Uint8Array} chunk
   */
  feed(chunk) {
    // concatenate without copy when possible
    const combined = new Uint8Array(this._buffer.length + chunk.length);
    combined.set(this._buffer);
    combined.set(chunk, this._buffer.length);
    let offset = 0;
    while (combined.length - offset >= 4) { // 4‑byte length prefix
      const view = new DataView(combined.buffer, combined.byteOffset + offset, 4);
      const len = view.getUint32(0, false); // network order
      if (combined.length - offset - 4 < len) break; // incomplete
      const frame = combined.subarray(offset + 4, offset + 4 + len);
      this._onFrame(frame);
      offset += 4 + len;
    }
    this._buffer = combined.subarray(offset);
  }
}

/**
 * In‑memory append‑only log with a minimal Bloom filter for existence checks.
 */
export class AppendOnlyLog {
  /**
   * @param {number} [bloomSize=1024] size of the Bloom filter in bits (must be power of two).
   */
  constructor(bloomSize = 1024) {
    if (bloomSize <= 0 || (bloomSize & (bloomSize - 1)) !== 0) {
      throw new RangeError('bloomSize must be a power of two');
    }
    this._entries = [];
    this._bloom = new Uint8Array(bloomSize >>> 3);
    this._mask = bloomSize - 1;
  }

  _hash(value) {
    // simple 32‑bit FNV‑1a variant – deterministic, fast, no external deps.
    let hash = 0x811c9dc5;
    for (let i = 0; i < value.length; i++) {
      hash ^= value[i];
      hash = (hash * 0x01000193) >>> 0;
    }
    return hash & this._mask;
  }

  _addToBloom(value) {
    const h = this._hash(value);
    const byteIdx = h >>> 3;
    const bitIdx = h & 7;
    this._bloom[byteIdx] |= 1 << bitIdx;
  }

  /**
   * Append a Uint8Array entry.
   * @param {Uint8Array} entry
   */
  append(entry) {
    if (!(entry instanceof Uint8Array)) throw new TypeError('entry must be Uint8Array');
    this._entries.push(entry);
    this._addToBloom(entry);
  }

  /**
   * Retrieve entry by index.
   * @param {number} idx
   * @returns {Uint8Array|null}
   */
  get(idx) {
    return this._entries[idx] ?? null;
  }

  /**
   * Probabilistic existence test using the Bloom filter.
   * @param {Uint8Array} entry
   * @returns {boolean} true if possibly present, false if definitely absent.
   */
  maybeHas(entry) {
    const h = this._hash(entry);
    const byteIdx = h >>> 3;
    const bitIdx = h & 7;
    return (this._bloom[byteIdx] & (1 << bitIdx)) !== 0;
  }
}

/**
 * Backpressure‑aware async stream pipeline.
 * Accepts an async iterable source, applies a transform, and respects consumer demand.
 */
export class StreamPipeline {
  /**
   * @param {AsyncIterable<any>} source
   * @param {function(any): Promise<any>} asyncTransform
   */
  constructor(source, asyncTransform) {
    this._source = source;
    this._transform = asyncTransform;
  }

  /**
   * Returns an async iterator that yields transformed items.
   */
  async *[Symbol.asyncIterator]() {
    for await (const chunk of this._source) {
      const transformed = await this._transform(chunk);
      yield transformed;
    }
  }
}

/**
 * High‑frequency token bucket rate limiter using bitwise arithmetic.
 */
export class TokenBucket {
  /**
   * @param {number} capacity Maximum tokens.
   * @param {number} refillRate Tokens added per millisecond.
   */
  constructor(capacity, refillRate) {
    if (capacity <= 0 || refillRate <= 0) throw new RangeError('capacity and refillRate must be > 0');
    this._capacity = capacity >>> 0;
    this._refillRate = refillRate; // float tokens/ms
    this._tokens = this._capacity;
    this._last = Date.now();
  }

  /**
   * Attempt to consume `n` tokens.
   * @param {number} n
   * @returns {boolean} true if allowed, false otherwise.
   */
  tryConsume(n = 1) {
    const now = Date.now();
    const elapsed = now - this._last;
    this._last = now;
    // refill using integer arithmetic for speed
    const refill = Math.floor(elapsed * this._refillRate);
    this._tokens = Math.min(this._capacity, this._tokens + refill);
    if (this._tokens >= n) {
      this._tokens -= n;
      return true;
    }
    return false;
  }
}
