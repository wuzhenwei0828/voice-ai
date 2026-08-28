'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

global.window = {};
require('./sse.js');

function mockResponse(chunks) {
  let index = 0;
  return {
    ok: true,
    body: {
      getReader() {
        return {
          async read() {
            if (index >= chunks.length) return { done: true };
            return { done: false, value: Buffer.from(chunks[index++]) };
          },
        };
      },
    },
  };
}

test('parseSse emits JSON data events across chunk boundaries', async () => {
  const events = [];
  for await (const event of window.parseSse(mockResponse([
    'data: {"text":"你"',
    ',"is_final":false}\n\ndata: {"text":"好","is_final":true}\n\n',
  ]))) {
    events.push(event);
  }

  assert.deepEqual(events, [
    { text: '你', is_final: false },
    { text: '好', is_final: true },
  ]);
});
