'use strict';

(function initTraceContext(root) {
  function createTraceId() {
    return root.crypto.randomUUID();
  }

  function fetchWithTrace(input, init = {}) {
    const headers = new Headers(init.headers || {});
    headers.set('trace_id', createTraceId());
    return root.fetch(input, { ...init, headers });
  }

  const api = { createTraceId, fetchWithTrace };
  root.createTraceId = createTraceId;
  root.fetchWithTrace = fetchWithTrace;
  if (typeof module === 'object' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
