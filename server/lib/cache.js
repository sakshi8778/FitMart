const { createClient } = (() => {
  try { return require('redis'); } catch (e) { return null; }
})();
const EventEmitter = require('events');

class Cache extends EventEmitter {
  constructor() {
    super();
    this.enabled = false;
    this.ttl = Number(process.env.PRODUCTS_CACHE_TTL || 60);
    this.client = null;
    this.mem = new Map();
    this.init();
  }

  async init() {
    const redisUrl = process.env.REDIS_URL || process.env.REDIS_HOST;
    if (!createClient || !redisUrl) {
      console.warn('[cache] Redis not configured — using in-memory fallback');
      this.enabled = false;
      return;
    }

    try {
      const client = createClient({ url: redisUrl });
      client.on('error', (err) => {
        // Be defensive: some error events may not include a message property.
        if (process.env.DEBUG_REDIS === 'true') {
          console.warn('[redis] error (full):', err);
        } else {
          const msg = err && (err.message || err.code || String(err))
            ? (err.message || err.code || String(err))
            : '<no-message>';
          console.warn('[redis] error', msg);
        }
      });
      await client.connect();
      this.client = client;
      this.enabled = true;
      console.log('[cache] Connected to Redis');
    } catch (err) {
      if (process.env.DEBUG_REDIS === 'true') {
        console.warn('[cache] Redis connection failed — falling back to memory cache (full):', err);
      } else {
        console.warn('[cache] Redis connection failed — falling back to memory cache', err && (err.message || err.code) ? (err.message || err.code) : '<no-message>');
      }
      this.enabled = false;
    }
  }

  _serializeKey(key) {
    if (typeof key === 'string') return key;
    return JSON.stringify(key);
  }

  async get(key) {
    const k = this._serializeKey(key);
    if (this.enabled && this.client) {
      try {
        const val = await this.client.get(k);
        if (!val) return null;
        return JSON.parse(val);
      } catch (err) {
        console.warn('[cache] redis get failed', err.message);
        return null;
      }
    }
    const v = this.mem.get(k);
    return v === undefined ? null : v;
  }

  async set(key, value, ttlSec) {
    const k = this._serializeKey(key);
    const ttl = typeof ttlSec === 'number' ? ttlSec : this.ttl;
    if (this.enabled && this.client) {
      try {
        await this.client.set(k, JSON.stringify(value), { EX: ttl });
        return true;
      } catch (err) {
        console.warn('[cache] redis set failed', err.message);
        return false;
      }
    }
    this.mem.set(k, value);
    setTimeout(() => this.mem.delete(k), ttl * 1000);
    return true;
  }

  async delPattern(prefix) {
    if (this.enabled && this.client) {
      try {
        // use SCAN to find matching keys and delete
        const iter = this.client.scanIterator({ MATCH: `${prefix}*` });
        const keys = [];
        for await (const k of iter) keys.push(k);
        if (keys.length) await this.client.del(keys);
      } catch (err) {
        console.warn('[cache] redis delPattern failed', err.message);
      }
      return;
    }
    for (const k of Array.from(this.mem.keys())) {
      if (k.startsWith(prefix)) this.mem.delete(k);
    }
  }

  async clearAll() {
    if (this.enabled && this.client) {
      try { await this.client.flushDb(); } catch (err) { console.warn('[cache] flushDb failed', err.message); }
      return;
    }
    this.mem.clear();
  }
}

module.exports = new Cache();
