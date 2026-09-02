export class AudioPlayer {
  private readonly context?: AudioContext;
  private contextInstance?: AudioContext;
  private nextStartTime = 0;
  private sources = new Set<AudioBufferSourceNode>();
  private pending = new Uint8Array(0);

  constructor(context?: AudioContext) {
    this.context = context;
  }

  enqueue(audio: Uint8Array | string, sampleRate = 24000, channels = 1, _isLast = true): number | undefined {
    const bytes = typeof audio === 'string'
      ? Uint8Array.from(atob(audio), (char) => char.charCodeAt(0))
      : audio;
    const safeChannels = channels > 0 ? Math.min(channels, 8) : 1;
    const combined = new Uint8Array(this.pending.byteLength + bytes.byteLength);
    combined.set(this.pending); combined.set(bytes, this.pending.byteLength);
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
    source.onended = () => this.sources.delete(source);
    source.start(startAt);
    if (context.state === 'suspended') void context.resume();
    const now = typeof performance !== 'undefined' ? performance.now() : Date.now();
    return now + Math.max(0, startAt - context.currentTime) * 1000;
  }

  stop() {
    for (const source of this.sources) {
      try { source.stop(); } catch { /* already ended */ }
      try { source.disconnect(); } catch { /* already disconnected */ }
    }
    this.sources.clear();
    this.nextStartTime = 0;
    this.pending = new Uint8Array(0);
  }

  resume() {
    try {
      return Promise.resolve(this.getContext().resume());
    } catch (error) {
      return Promise.reject(error);
    }
  }

  private getContext() {
    if (this.context) return this.context;
    if (!this.contextInstance) this.contextInstance = new AudioContext();
    return this.contextInstance;
  }
}
