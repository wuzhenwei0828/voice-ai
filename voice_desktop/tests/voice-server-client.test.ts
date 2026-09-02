import { describe, expect, it, vi } from 'vitest';
import { VoiceServerClient, buildWsUrl, normalizeBaseUrl } from '../src/services/voice-server-client';
import { decodeVoiceMessage, encodeVoiceIndication } from '../src/services/msgpack';
import { AudioPlayer } from '../src/services/audio-player';
describe('voice server urls', () => { it('normalizes and converts https to wss', () => { expect(normalizeBaseUrl('https://api.example.com/')).toBe('https://api.example.com'); expect(buildWsUrl({ baseUrl: 'https://api.example.com/', token: '' }, 'a b')).toBe('wss://api.example.com/ws/voice/web/a%20b'); }); it('rejects unsupported schemes', () => { expect(() => normalizeBaseUrl('ftp://example.com')).toThrow(); }); });

describe('voice message tracing', () => {
  it('adds a UUID message_id to every outgoing websocket message', () => {
    const sent: Uint8Array[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: () => {},
    });
    (client as any).socket = { readyState: WebSocket.OPEN, send: (data: Uint8Array) => sent.push(data) };

    (client as any).send({ type: 'interrupt', session_id: 's' });
    (client as any).send({ type: 'retry', session_id: 's' });

    const first = decodeVoiceMessage(sent[0]);
    const second = decodeVoiceMessage(sent[1]);
    expect(first.message_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    expect(second.message_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    expect(second.message_id).not.toBe(first.message_id);
  });

  it('logs outgoing websocket metadata without payload contents', () => {
    const sent: Uint8Array[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: () => {},
    });
    const log = vi.spyOn(console, 'info').mockImplementation(() => {});
    (client as any).socket = { readyState: WebSocket.OPEN, send: (data: Uint8Array) => sent.push(data) };

    (client as any).send({ type: 'interrupt', session_id: 's' });

    expect(sent).toHaveLength(1);
    expect(log).toHaveBeenCalledWith('[voice-ws] send', expect.objectContaining({
      type: 'interrupt',
      bytes: expect.any(Number),
    }));
    log.mockRestore();
  });

  it('logs received websocket frame size before decoding it', () => {
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: () => {},
    });
    const log = vi.spyOn(console, 'info').mockImplementation(() => {});
    const bytes = encodeVoiceIndication({ type: 'session_ack', session_id: 's', success: true, message: '' });

    (client as any).sessionId = 's';
    (client as any).handleMessage(bytes);

    expect(log).toHaveBeenCalledWith('[voice-ws] receive', expect.objectContaining({
      sessionId: 's',
      type: 'session_ack',
      bytes: bytes.byteLength,
    }));
    log.mockRestore();
  });
});

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
  it('reports one bounded playback-start delay for the first playable chunk', () => {
    const sent: Uint8Array[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: () => {},
    });
    (client as any).sessionId = 's';
    (client as any).socket = { readyState: WebSocket.OPEN, send: (data: Uint8Array) => sent.push(data) };
    const audio = encodeVoiceIndication({
      type: 'tts_audio', session_id: 's', seq: 1, data: new Uint8Array([0, 0]),
      is_last: false, request_id: 7,
    });
    (client as any).handleMessage(audio.buffer);
    const firstAudioAt = (client as any).firstAudioReceivedAt.get(7);
    expect(firstAudioAt).toEqual(expect.any(Number));
    (client as any).reportPlaybackStarted(7, firstAudioAt + 25);
    (client as any).reportPlaybackStarted(7, firstAudioAt + 30);
    expect(sent).toHaveLength(1);
    expect(decodeVoiceMessage(sent[0])).toMatchObject({
      type: 'playback_started', session_id: 's', request_id: 7, delay_ms: 25,
    });
  });

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
