import { describe, expect, it } from 'vitest';
import { VoiceServerClient, buildWsUrl, normalizeBaseUrl } from '../src/services/voice-server-client';
import { decodeVoiceMessage, encodeVoiceIndication } from '../src/services/msgpack';
import { AudioPlayer } from '../src/services/audio-player';
describe('voice server urls', () => { it('normalizes and converts https to wss', () => { expect(normalizeBaseUrl('https://api.example.com/')).toBe('https://api.example.com'); expect(buildWsUrl({ baseUrl: 'https://api.example.com/', token: '' }, 'a b')).toBe('wss://api.example.com/ws/voice/web/a%20b'); }); it('rejects unsupported schemes', () => { expect(() => normalizeBaseUrl('ftp://example.com')).toThrow(); }); });

describe('voice MessagePack protocol', () => {
  it('encodes the Rust Indication envelope and preserves binary audio/u64 timestamps', () => {
    const bytes = encodeVoiceIndication({
      type: 'audio_chunk', session_id: 'session-1', seq: 7,
      timestamp_ms: 1_735_000_123_456, data: new Uint8Array([0, 1, 255]), is_last: false,
    });
    expect(decodeVoiceMessage(bytes)).toEqual({
      type: 'audio_chunk', session_id: 'session-1', seq: 7,
      timestamp_ms: 1_735_000_123_456, data: new Uint8Array([0, 1, 255]), is_last: false,
    });
  });

  it('decodes TTS audio metadata from a downlink Indication', () => {
    const bytes = encodeVoiceIndication({
      type: 'tts_audio', session_id: 'session-1', seq: 2,
      data: new Uint8Array([1, 2]), is_last: true, sample_rate: 24_000, channels: 1, request_id: 9,
    });
    const payload = decodeVoiceMessage(bytes);
    expect(payload.type).toBe('tts_audio');
    expect(payload.data).toEqual(new Uint8Array([1, 2]));
    expect(payload.sample_rate).toBe(24_000);
    expect(payload.channels).toBe(1);
  });
});

describe('streaming TTS playback', () => {
  it('starts playback when the first PCM chunk arrives', () => {
    const starts: number[] = [];
    const context = {
      currentTime: 0,
      destination: {},
      state: 'running',
      createBuffer: (_channels: number, frames: number, sampleRate: number) => ({
        duration: frames / sampleRate,
        getChannelData: () => new Float32Array(frames),
      }),
      createBufferSource: () => ({
        buffer: undefined,
        connect: () => {},
        start: (when: number) => starts.push(when),
        stop: () => {},
        disconnect: () => {},
      }),
      resume: async () => {},
    } as unknown as AudioContext;
    const player = new AudioPlayer(context);

    player.enqueue(new Uint8Array([0, 0]), 1000, 1, false);

    expect(starts).toEqual([0]);
  });

  it('preserves PCM bytes split across chunk boundaries', () => {
    const starts: number[] = [];
    const context = {
      currentTime: 0,
      destination: {},
      state: 'running',
      createBuffer: (_channels: number, frames: number, _sampleRate: number) => ({
        duration: frames / 1000,
        getChannelData: () => new Float32Array(frames),
      }),
      createBufferSource: () => ({
        buffer: undefined,
        connect: () => {},
        start: (when: number) => starts.push(when),
        stop: () => {},
        disconnect: () => {},
      }),
      resume: async () => {},
    } as unknown as AudioContext;
    const player = new AudioPlayer(context);

    player.enqueue(new Uint8Array([0]), 1000, 1, false);
    player.enqueue(new Uint8Array([0, 0]), 1000, 1, false);

    expect(starts).toEqual([0]);
  });

  it('ignores TTS audio from a request invalidated by interrupt', () => {
    const events: unknown[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: (event) => events.push(event),
    });
    const status = encodeVoiceIndication({
      type: 'agent_status', session_id: 's', phase: 'speaking', label: '回答中',
      tool: null, request_id: 1, done: false,
    });
    const audio = encodeVoiceIndication({
      type: 'tts_audio', session_id: 's', seq: 1, data: new Uint8Array([0, 0]),
      is_last: false, request_id: 1,
    });
    (client as any).handleMessage(status.buffer);
    client.interrupt();
    (client as any).handleMessage(audio.buffer);

    expect(events.filter((event: any) => event.type === 'tts_audio')).toEqual([]);
  });

  it('ignores agent status from a request invalidated by interrupt', () => {
    const events: unknown[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: (event) => events.push(event),
    });
    const status = encodeVoiceIndication({
      type: 'agent_status', session_id: 's', phase: 'speaking', label: '回答中',
      tool: null, request_id: 2, done: false,
    });
    (client as any).handleMessage(status.buffer);
    client.interrupt();
    (client as any).handleMessage(status.buffer);

    expect(events.filter((event: any) => event.type === 'agent_status')).toHaveLength(1);
  });
});
