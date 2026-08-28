'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { executeRetry } = require('./app.js');

test('retry clears stale playback and invalidates the old request before sending', () => {
  const calls = [];

  const sent = executeRetry({
    canRetry: true,
    socketOpen: true,
    stopPlayback: () => calls.push('stop-playback'),
    invalidateRequest: () => calls.push('invalidate-request'),
    sendRetry: () => calls.push('send-retry'),
  });

  assert.equal(sent, true);
  assert.deepEqual(calls, ['stop-playback', 'invalidate-request', 'send-retry']);
});

test('retry does nothing when there is no safe request to repeat', () => {
  let called = false;

  const sent = executeRetry({
    canRetry: false,
    socketOpen: true,
    stopPlayback: () => { called = true; },
    invalidateRequest: () => { called = true; },
    sendRetry: () => { called = true; },
  });

  assert.equal(sent, false);
  assert.equal(called, false);
});
