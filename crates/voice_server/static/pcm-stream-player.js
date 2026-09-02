// Stream s16le PCM chunks through Web Audio without waiting for a terminal event.
(function initPcmStreamPlayer(root) {
  'use strict';

  class PcmStreamPlayer {
    constructor(options = {}) {
      this.context = options.audioContext;
      this.audioContextFactory = options.audioContextFactory || (() => {
        const AudioContextCtor = root.AudioContext || root.webkitAudioContext;
        if (!AudioContextCtor) throw new Error('浏览器不支持 Web Audio');
        return new AudioContextCtor();
      });
      this.onState = options.onState || (() => {});
      this.nextStartTime = 0;
      this.sources = new Set();
      this.pending = new Uint8Array(0);
      this.paused = false;
      this.stateListeners = new Set();
    }

    enqueue(bytes, sampleRate = 16000, channels = 1) {
      const safeChannels = channels > 0 ? Math.min(channels, 8) : 1;
      const incoming = bytes || new Uint8Array(0);
      const combined = new Uint8Array(this.pending.byteLength + incoming.byteLength);
      combined.set(this.pending); combined.set(incoming, this.pending.byteLength);
      this.pending = combined;
      const frameBytes = 2 * safeChannels;
      const usableBytes = this.pending.byteLength - (this.pending.byteLength % frameBytes);
      if (!usableBytes) return undefined;
      const pcm = this.pending.subarray(0, usableBytes);
      this.pending = this.pending.slice(usableBytes);
      const context = this.getContext();
      const frames = usableBytes / frameBytes;
      if (!frames) return undefined;
      const buffer = context.createBuffer(safeChannels, frames, sampleRate);
      const view = new DataView(pcm.buffer, pcm.byteOffset, pcm.byteLength);
      for (let channel = 0; channel < safeChannels; channel++) {
        const output = buffer.getChannelData(channel);
        for (let frame = 0; frame < frames; frame++) {
          const offset = (frame * safeChannels + channel) * 2;
          output[frame] = view.getInt16(offset, true) / 32768;
        }
      }
      const source = context.createBufferSource();
      source.buffer = buffer;
      source.connect(context.destination);
      const startAt = Math.max(context.currentTime, this.nextStartTime);
      this.nextStartTime = startAt + buffer.duration;
      this.sources.add(source);
      source.onended = () => {
        this.sources.delete(source);
        this.emitState();
      };
      source.start(startAt);
      this.emitState();
      if (this.paused) {
        if (context.state !== 'suspended') void context.suspend();
      } else if (context.state === 'suspended') {
        void context.resume();
      }
      const now = typeof performance !== 'undefined' ? performance.now() : Date.now();
      return now + Math.max(0, startAt - context.currentTime) * 1000;
    }

    stop() {
      for (const source of this.sources) {
        try { source.stop(); } catch (_) { /* already ended */ }
        try { source.disconnect(); } catch (_) { /* already disconnected */ }
      }
      this.sources.clear();
      this.nextStartTime = 0;
      this.pending = new Uint8Array(0);
      this.paused = false;
      this.emitState();
    }

    pause() {
      this.paused = true;
      this.emitState();
      if (!this.context) return Promise.resolve();
      return Promise.resolve(this.context.suspend());
    }

    resume() {
      this.paused = false;
      this.emitState();
      try {
        return Promise.resolve(this.getContext().resume());
      } catch (error) {
        return Promise.reject(error);
      }
    }

    isPaused() {
      return this.paused;
    }

    hasScheduledAudio() {
      return this.sources.size > 0;
    }

    addStateListener(listener) {
      this.stateListeners.add(listener);
      listener(this.hasScheduledAudio() && !this.paused);
      return () => this.stateListeners.delete(listener);
    }

    emitState() {
      const playing = this.hasScheduledAudio() && !this.paused;
      this.onState(playing);
      for (const listener of this.stateListeners) listener(playing);
    }

    getContext() {
      if (!this.context) this.context = this.audioContextFactory();
      return this.context;
    }
  }

  root.PcmStreamPlayer = PcmStreamPlayer;
  root.bindPcmStreamPlaybackToggle = (player, button) => {
    let replayAudio = null;
    const control = button.closest ? button.closest('.stream-playback-control') : null;
    const track = control && control.querySelector
      ? control.querySelector('.stream-playback-track')
      : null;
    const updateTrack = (hasLiveAudio) => {
      if (!track) return;
      track.disabled = hasLiveAudio || !replayAudio || !Number.isFinite(replayAudio.duration);
      if (hasLiveAudio || !replayAudio) {
        track.max = 0;
        track.value = 0;
        return;
      }
      track.max = replayAudio.duration;
      track.value = Math.min(replayAudio.currentTime, replayAudio.duration);
    };
    const update = () => {
      const paused = player.isPaused();
      const hasLiveAudio = player.hasScheduledAudio();
      const replayPlaying = replayAudio && !replayAudio.paused && !replayAudio.ended;
      const replayPaused = replayAudio && replayAudio.paused && replayAudio.currentTime > 0 && !replayAudio.ended;
      const state = hasLiveAudio
        ? (paused ? 'paused' : 'playing')
        : (replayPlaying ? 'playing' : (replayPaused ? 'paused' : (replayAudio ? 'ready' : 'idle')));
      if (control) control.dataset.state = state;
      button.disabled = !hasLiveAudio && !replayAudio;
      button.textContent = state === 'playing' ? '||' : '>';
      const label = state === 'playing' ? '暂停播放' : (state === 'paused' ? '继续播放' : '从头播放');
      button.title = label;
      button.setAttribute('aria-label', label);
      updateTrack(hasLiveAudio);
    };
    button.addEventListener('click', () => {
      let action;
      if (player.hasScheduledAudio()) {
        action = player.isPaused() ? player.resume() : player.pause();
      } else if (replayAudio) {
        if (replayAudio.ended) replayAudio.currentTime = 0;
        action = replayAudio.paused ? replayAudio.play() : replayAudio.pause();
      } else {
        return;
      }
      Promise.resolve(action).catch((error) => console.warn('[pcm-player] 播放状态切换失败:', error));
      update();
    });
    player.addStateListener(update);
    if (track) {
      track.addEventListener('input', () => {
        if (!replayAudio) return;
        replayAudio.currentTime = Number(track.value);
        update();
      });
    }
    return {
      setReplay(url) {
        if (replayAudio) replayAudio.pause();
        replayAudio = new root.Audio(url);
        for (const event of ['loadedmetadata', 'timeupdate', 'play', 'pause', 'ended']) {
          replayAudio.addEventListener(event, update);
        }
        update();
      },
      clearReplay() {
        if (replayAudio) {
          replayAudio.pause();
          replayAudio.removeAttribute('src');
          replayAudio.load();
          replayAudio = null;
        }
        update();
      },
    };
  };
  if (typeof module === 'object' && module.exports) module.exports = PcmStreamPlayer;
})(typeof window !== 'undefined' ? window : globalThis);
