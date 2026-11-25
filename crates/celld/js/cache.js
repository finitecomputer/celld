// The Workers Cache API over the node's cache (crates/celld/cache_api.rs).
//
// Cloudflare's Cache API is a view of the edge cache of the data center a
// Worker runs in. A node is celld's data center: every isolate of the node
// shares one cache, kept in memory, per script. The shim validates each call
// with Workerd's rules and messages; the host keeps what Cloudflare's rules
// say to keep (`cache_api::keep_for`) and answers matches until each entry
// expires. A Dynamic Worker's cache keeps nothing and misses every match.
(function () {
  const kCreate = Symbol("celld.Cache.create");
  const kName = Symbol("celld.Cache.name");
  // The statuses whose Response must have a null body.
  const nullBodyStatus = new Set([101, 103, 204, 205, 304]);

  // A key is a Request or URL string, and it must parse as an absolute
  // http(s) URL -- the same rule and message as Workerd's validateUrl.
  // The entry's URL has no fragment: a fragment never reaches a server.
  const asKey = (request) => {
    const url = request instanceof Request ? request.url : String(request);
    let parsed = null;
    try {
      parsed = new URL(url);
    } catch { /* rejected below */ }
    if (!parsed ||
      (parsed.protocol !== "http:" && parsed.protocol !== "https:")) {
      throw new TypeError(
        "Invalid URL. Cache API keys must be fully-qualified, valid URLs.");
    }
    // cut at the serialized fragment, which keeps the rest as it is: a bare
    // `?` stays, and is another URL
    const href = parsed.href;
    const hash = href.indexOf("#");
    const method = request instanceof Request ? request.method : "GET";
    const headers = request instanceof Request ? request.headers : null;
    return { url: hash < 0 ? href : href.slice(0, hash), method, headers };
  };

  // A match or delete considers only a GET, unless `ignoreMethod`.
  const considered = (key, options) =>
    key.method === "GET" || Boolean(options && options.ignoreMethod);

  // Cloudflare answers these from the cache with a 206 or a 304; the node
  // answers neither, so it misses, and the Worker's own path answers them
  // as it did when every match missed.
  const partOrCondition = ["range", "if-none-match", "if-modified-since"];
  const whole = (key) =>
    key.headers === null || !partOrCondition.some((h) => key.headers.has(h));

  class Cache {
    constructor(token = undefined, name = null) {
      if (token !== kCreate) throw new TypeError("Illegal constructor");
      Object.defineProperty(this, kName, { value: name });
    }

    get [Symbol.toStringTag]() { return "Cache"; }

    async put(request, response) {
      const key = asKey(request);
      if (!(response instanceof Response)) {
        throw new TypeError(
          "Cache.put() requires a Response as its second argument.");
      }
      if (key.method !== "GET") {
        throw new TypeError("Cannot cache response to non-GET request.");
      }
      if (response.status === 206) {
        throw new TypeError(
          "Cannot cache response to a range request (206 Partial Content).");
      }
      const vary = response.headers.get("vary");
      if (vary !== null && vary.includes("*")) {
        throw new TypeError("Cannot cache response with 'Vary: *' header.");
      }
      if (response.bodyUsed) {
        throw new TypeError(
          "Cannot cache a response whose body is already used.");
      }
      // put() disturbs the response, kept or not: a streaming producer sees
      // its stream pulled to completion instead of hanging on a reader that
      // never comes.
      const body = response.body
        ? new Uint8Array(await response.arrayBuffer())
        : new Uint8Array();
      __cache_put(
        this[kName], key.url, response.status, response.statusText,
        JSON.stringify([...response.headers]), body);
    }

    async match(request, options = undefined) {
      const key = asKey(request);
      if (!considered(key, options) || !whole(key)) return undefined;
      const found = __cache_match(this[kName], key.url);
      if (found === undefined) return undefined;
      const [status, statusText, headers, body] = found;
      return new Response(nullBodyStatus.has(status) ? null : body, {
        status,
        statusText,
        headers: JSON.parse(headers),
      });
    }

    async delete(request, options = undefined) {
      const key = asKey(request);
      if (!considered(key, options)) return false;
      return __cache_delete(this[kName], key.url);
    }
  }

  class CacheStorage {
    constructor(token = undefined) {
      if (token !== kCreate) throw new TypeError("Illegal constructor");
    }

    get [Symbol.toStringTag]() { return "CacheStorage"; }

    async open(cacheName) {
      const name = String(cacheName);
      if (name.length >= 1024) {
        throw new TypeError("Cache name is too long.");
      }
      return new Cache(kCreate, name);
    }
  }

  const storage = new CacheStorage(kCreate);
  Object.defineProperty(storage, "default", {
    value: new Cache(kCreate, null),
    enumerable: true,
  });

  globalThis.Cache = Cache;
  globalThis.CacheStorage = CacheStorage;
  globalThis.caches = storage;
})();
