export type VADFrame = { bytes: ArrayBuffer; isLast: boolean; rms: number };
type VADOptions = { threshold?: number; startFrames?: number; endFrames?: number; maxFrames?: number; prerollFrames?: number };

export class EnergyVad {
  private speaking = false; private voicedRun = 0; private silentRun = 0; private speechFrames = 0; private preroll: VADFrame[] = [];
  private readonly threshold: number; private readonly startFrames: number; private readonly endFrames: number; private readonly maxFrames: number; private readonly prerollFrames: number;
  constructor(options: VADOptions = {}) { this.threshold = options.threshold ?? 0.01; this.startFrames = options.startFrames ?? 5; this.endFrames = options.endFrames ?? 40; this.maxFrames = options.maxFrames ?? 1500; this.prerollFrames = options.prerollFrames ?? 5; }
  push(samples: Int16Array): VADFrame[] {
    let sum = 0; for (const sample of samples) { const value = sample / 32768; sum += value * value; }
    const rms = Math.sqrt(sum / Math.max(1, samples.length)); const bytes = new Uint8Array(samples.byteLength); bytes.set(new Uint8Array(samples.buffer, samples.byteOffset, samples.byteLength)); const frame = { bytes: bytes.buffer, isLast: false, rms }; const voiced = rms >= this.threshold;
    if (!this.speaking) { this.preroll.push(frame); while (this.preroll.length > this.prerollFrames + this.startFrames) this.preroll.shift(); if (voiced) this.voicedRun += 1; else this.voicedRun = 0; if (this.voicedRun < this.startFrames) return []; this.speaking = true; this.speechFrames = this.voicedRun; this.voicedRun = 0; this.silentRun = 0; const output = this.preroll; this.preroll = []; return output; }
    this.speechFrames += 1; this.silentRun = voiced ? 0 : this.silentRun + 1; const isLast = this.silentRun >= this.endFrames || this.speechFrames >= this.maxFrames;
    if (isLast) { this.speaking = false; this.voicedRun = 0; this.silentRun = 0; this.speechFrames = 0; this.preroll = []; }
    return [{ ...frame, isLast }];
  }
  reset() { this.speaking = false; this.voicedRun = 0; this.silentRun = 0; this.speechFrames = 0; this.preroll = []; }
}

export function floatToPcm16k(input: Float32Array, sourceRate: number): ArrayBuffer { if (sourceRate === 16_000) return toPcm(input); const ratio = sourceRate / 16_000; const output = new Int16Array(Math.max(1, Math.floor(input.length / ratio))); for (let i = 0; i < output.length; i += 1) output[i] = toInt16(input[Math.min(input.length - 1, Math.floor(i * ratio))]); return output.buffer; }
function toInt16(value: number) { return Math.max(-32768, Math.min(32767, Math.round(Math.max(-1, Math.min(1, value)) * (value < 0 ? 32768 : 32767)))); }
function toPcm(input: Float32Array) { const output = new Int16Array(input.length); for (let i = 0; i < input.length; i += 1) output[i] = toInt16(input[i]); return output.buffer; }

export class AudioRecorder {
  private stream?: MediaStream; private context?: AudioContext; private source?: MediaStreamAudioSourceNode; private processor?: ScriptProcessorNode; private vad = new EnergyVad(); private pending = new Int16Array(0);
  async start(onFrame: (frame: ArrayBuffer, isLast: boolean, rms: number) => void) {
    this.stream = await navigator.mediaDevices.getUserMedia({ audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
    try { this.context = new AudioContext({ sampleRate: 16_000 }); } catch { this.context = new AudioContext(); }
    this.source = this.context.createMediaStreamSource(this.stream); this.processor = this.context.createScriptProcessor(2048, 1, 1);
    this.processor.onaudioprocess = (event) => { const pcm = new Int16Array(floatToPcm16k(event.inputBuffer.getChannelData(0), this.context!.sampleRate)); const merged = new Int16Array(this.pending.length + pcm.length); merged.set(this.pending); merged.set(pcm, this.pending.length); this.pending = merged; while (this.pending.length >= 320) { const frame = this.pending.slice(0, 320); this.pending = this.pending.slice(320); for (const output of this.vad.push(frame)) onFrame(output.bytes, output.isLast, output.rms); } };
    this.source.connect(this.processor); this.processor.connect(this.context.destination);
  }
  stop() { this.processor?.disconnect(); this.source?.disconnect(); this.stream?.getTracks().forEach((track) => track.stop()); void this.context?.close(); this.processor = undefined; this.source = undefined; this.stream = undefined; this.pending = new Int16Array(0); this.vad.reset(); }
  reset() { this.pending = new Int16Array(0); this.vad.reset(); }
}
