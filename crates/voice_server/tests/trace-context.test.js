'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { createTraceId, fetchWithTrace } = require('../static/js/core/trace-context.js');

test('createTraceId returns a UUID', () => {
  assert.match(createTraceId(), /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
});

test('fetchWithTrace adds a fresh trace_id header', async () => {
  const calls = [];
  const originalFetch = global.fetch;
  global.fetch = async (...args) => { calls.push(args); return {}; };
  try {
    await fetchWithTrace('/admin/voices', { headers: { 'Content-Type': 'application/json' } });
  } finally {
    global.fetch = originalFetch;
  }
  const headers = calls[0][1].headers;
  assert.match(headers.get('trace_id'), /^[0-9a-f-]{36}$/i);
});

