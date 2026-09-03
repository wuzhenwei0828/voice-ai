'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createClientMetricReport, executeRetry, shouldStopPlaybackForAsr, trySendWebSocket } = require('./app.js');

test('returns false when browser websocket send throws synchronously', () => {
  assert.equal(trySendWebSocket({ send: () => { throw new Error('closed'); } }, new Uint8Array([1])), false);
  assert.equal(trySendWebSocket({ send: () => {} }, new Uint8Array([1])), true);
});

test('ASR stops only when a non-empty event changes the current message id', () => {
  assert.equal(shouldStopPlaybackForAsr('   ', 'message-1', 'message-0'), false);
  assert.equal(shouldStopPlaybackForAsr('你好', 'message-1', 'message-0'), true);
  assert.equal(shouldStopPlaybackForAsr('你好', 'message-1', 'message-1'), false);
});

test('retry clears stale playback before sending', () => {
  const calls = [];

  const sent = executeRetry({
    canRetry: true,
    socketOpen: true,
    stopPlayback: () => calls.push('stop-playback'),
    sendRetry: () => calls.push('send-retry'),
  });

  assert.equal(sent, true);
  assert.deepEqual(calls, ['stop-playback', 'send-retry']);
});

test('retry does nothing when there is no safe request to repeat', () => {
  let called = false;

  const sent = executeRetry({
    canRetry: false,
    socketOpen: true,
    stopPlayback: () => { called = true; },
    sendRetry: () => { called = true; },
  });

  assert.equal(sent, false);
  assert.equal(called, false);
});

test('client metric reports contain only a fixed relative duration', () => {
  assert.deepEqual(createClientMetricReport({
    sessionId: 'session-1',
    messageId: 'message-1',
    metric: 'input_end_to_final_audio_sent',
    startedAt: 10,
    endedAt: 12.4,
  }), {
    type: 'client_metric_report',
    session_id: 'session-1',
    message_id: 'message-1',
    metric: 'input_end_to_final_audio_sent',
    duration_ms: 2,
  });
  assert.equal(createClientMetricReport({
    sessionId: 'session-1',
    messageId: 'message-1',
    metric: 'unknown',
    startedAt: 10,
    endedAt: 12,
  }), null);
});
