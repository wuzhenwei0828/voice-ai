'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

global.window = {};
require('./pcm-stream-player.js');

function fakeAudioContext() {
  const starts = [];
  const buffers = [];
  const context = {
    currentTime: 0,
    destination: {},
    state: 'running',
    createBuffer(channels, frames, sampleRate) {
      const buffer = { channels, frames, sampleRate, duration: frames / sampleRate, data: [] };
      buffer.getChannelData = (channel) => {
        buffer.data[channel] ??= new Float32Array(frames);
        return buffer.data[channel];
      };
      buffers.push(buffer);
      return buffer;
    },
    createBufferSource() {
      return {
        connect() {},
        start(when) { starts.push(when); },
        stop() {},
        disconnect() {},
      };
    },
    resume() { return Promise.resolve(); },
  };
  return { context, starts, buffers };
}

test('streams the first PCM chunk immediately and schedules the next chunk contiguously', () => {
  const fake = fakeAudioContext();
  const player = new window.PcmStreamPlayer({ audioContext: fake.context });

  player.enqueue(new Uint8Array([0, 0, 255, 127]), 1000, 1);
  player.enqueue(new Uint8Array([0, 0, 255, 127]), 1000, 1);

  assert.equal(fake.starts.length, 2);
  assert.equal(fake.starts[0], 0);
  assert.equal(fake.starts[1], 2 / 1000);
  assert.equal(fake.buffers.length, 2);
});

test('stop cancels all scheduled sources and allows a new stream to start now', () => {
  const fake = fakeAudioContext();
  const player = new window.PcmStreamPlayer({ audioContext: fake.context });

  player.enqueue(new Uint8Array([0, 0]), 1000, 1);
  player.stop();
  player.enqueue(new Uint8Array([0, 0]), 1000, 1);

  assert.equal(fake.starts.length, 2);
  assert.equal(fake.starts[1], 0);
});

test('preserves PCM bytes split across chunk boundaries', () => {
  const fake = fakeAudioContext();
  const player = new window.PcmStreamPlayer({ audioContext: fake.context });

  player.enqueue(new Uint8Array([0]), 1000, 1);
  player.enqueue(new Uint8Array([0, 0]), 1000, 1);

  assert.equal(fake.starts.length, 1);
  assert.equal(fake.buffers[0].frames, 1);
});

test('paused playback stays paused when new chunks arrive', async () => {
  const fake = fakeAudioContext();
  let resumeCalls = 0;
  let suspendCalls = 0;
  fake.context.suspend = () => { suspendCalls++; fake.context.state = 'suspended'; return Promise.resolve(); };
  fake.context.resume = () => { resumeCalls++; fake.context.state = 'running'; return Promise.resolve(); };
  const player = new window.PcmStreamPlayer({ audioContext: fake.context });

  await player.pause();
  player.enqueue(new Uint8Array([0, 0]), 1000, 1);

  assert.equal(suspendCalls, 1);
  assert.equal(resumeCalls, 0);
  assert.equal(player.isPaused(), true);
});

test('the custom control replays completed audio and exposes its progress', async () => {
  const fake = fakeAudioContext();
  const listeners = {};
  const trackListeners = {};
  const track = {
    disabled: true,
    max: 0,
    value: 0,
    addEventListener(name, listener) { trackListeners[name] = listener; },
  };
  const controlElement = {
    dataset: {},
    querySelector() { return track; },
  };
  const button = {
    disabled: true,
    textContent: '',
    title: '',
    setAttribute() {},
    addEventListener(name, listener) { listeners[name] = listener; },
    closest() { return controlElement; },
  };
  const audioListeners = {};
  const replayAudio = {
    paused: true,
    ended: false,
    duration: 12,
    currentTime: 0,
    addEventListener(name, listener) { audioListeners[name] = listener; },
    play() { this.paused = false; audioListeners.play(); return Promise.resolve(); },
    pause() { this.paused = true; audioListeners.pause(); },
    removeAttribute() {},
    load() {},
  };
  window.Audio = function Audio() { return replayAudio; };
  const player = new window.PcmStreamPlayer({ audioContext: fake.context });
  const control = window.bindPcmStreamPlaybackToggle(player, button);

  control.setReplay('blob:test-audio');
  audioListeners.loadedmetadata();
  assert.equal(button.disabled, false);
  assert.equal(button.textContent, '>');
  assert.equal(track.disabled, false);
  assert.equal(track.max, 12);
  listeners.click();
  await Promise.resolve();

  assert.equal(button.textContent, '||');
  track.value = 7;
  trackListeners.input();
  assert.equal(replayAudio.currentTime, 7);
});
