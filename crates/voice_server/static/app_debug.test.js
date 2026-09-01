'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const appSource = fs.readFileSync(path.join(__dirname, 'app.js'), 'utf8');

test('browser app does not expose the removed websocket debug test in the transcript', () => {
  assert.doesNotMatch(appSource, /testWsSendBinary/);
  assert.doesNotMatch(appSource, /window\.__testWs/);
  assert.doesNotMatch(appSource, /调试：控制台运行 __testWs\.testWsSendBinary\(\)/);
});
