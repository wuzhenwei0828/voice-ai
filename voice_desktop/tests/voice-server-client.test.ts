import { describe, expect, it, vi } from 'vitest';
import { VoiceServerClient, buildWsUrl, normalizeBaseUrl } from '../src/services/voice-server-client';
import { decodeVoiceMessage, encodeVoiceIndication } from '../src/services/msgpack';
import { AudioPlayer } from '../src/services/audio-player';
describe('voice server urls', () => { it('normalizes and converts https to wss', () => { expect(normalizeBaseUrl('https://api.example.com/')).toBe('https://api.example.com'); expect(buildWsUrl({ baseUrl: 'https://api.example.com/', token: '' }, 'a b')).toBe('wss://api.example.com/ws/voice/web/a%20b'); }); it('rejects unsupported schemes', () => { expect(() => normalizeBaseUrl('ftp://example.com')).toThrow(); }); });

describe('voice session handshake', () => {
  it('does not send a client TTS sample-rate override', () => {
    const sent: Uint8Array[] = [];
    const originalWebSocket = globalThis.WebSocket;
    class FakeWebSocket {
      static readonly OPEN = 1;
      readyState = FakeWebSocket.OPEN;
      binaryType = '';
      onopen?: () => void;
      onmessage?: () => void;
      onerror?: () => void;
      onclose?: () => void;
      constructor() { }
      send(data: Uint8Array) { sent.push(data); }
      close() { }
    }
    (globalThis as any).WebSocket = FakeWebSocket;
    try {
      const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
        onState: () => {}, onEvent: () => {},
      });
      client.connect('session-1');
      const socket = (client as any).socket as FakeWebSocket;
      socket.onopen?.();

      expect(decodeVoiceMessage(sent[0])).not.toHaveProperty('tts_sample_rate');
    } finally {
      (globalThis as any).WebSocket = originalWebSocket;
    }
  });
});

describe('voice message tracing', () => {
  it('reuses one message_id for all audio chunks in an utterance', () => {
    const sent: Uint8Array[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: () => {},
    });
    (client as any).sessionId = 's';
    (client as any).socket = { readyState: WebSocket.OPEN, send: (data: Uint8Array) => sent.push(data) };

    client.sendAudio(new ArrayBuffer(2), false);
    client.sendAudio(new ArrayBuffer(2), true);
    client.sendAudio(new ArrayBuffer(2), true);

    const ids = sent
      .map((bytes) => decodeVoiceMessage(bytes))
      .filter((message) => message.type === 'audio_chunk')
      .map((message) => message.message_id);
    expect(ids[0]).toBe(ids[1]);
    expect(ids[2]).not.toBe(ids[1]);
  });

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

  it('preserves message_id on ASR events', () => {
    const events: unknown[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: (event) => events.push(event),
    });
    (client as any).handleMessage(encodeVoiceIndication({
      type: 'asr_partial', session_id: 's', text: '你', is_final: false, message_id: 'message-1',
    }).buffer);
    (client as any).handleMessage(encodeVoiceIndication({
      type: 'asr_partial', session_id: 's', text: '你好', is_final: true, message_id: 'message-1',
    }).buffer);

    expect(events).toEqual([
      { type: 'asr_partial', text: '你', message_id: 'message-1' },
      { type: 'asr_final', text: '你好', message_id: 'message-1' },
    ]);
  });

  it('filters pipeline events to the accepted message_id', () => {
    const events: unknown[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: (event) => events.push(event),
    });
    (client as any).handleMessage(encodeVoiceIndication({
      type: 'agent_status', session_id: 's', phase: 'speaking', label: '回答中',
      tool: null, message_id: 'message-0', done: false,
    }).buffer);
    expect((client as any).acceptAsrMessage('message-1', '你好')).toBe(true);
    (client as any).handleMessage(encodeVoiceIndication({
      type: 'tts_audio', session_id: 's', seq: 1, data: new Uint8Array([0, 0]),
      is_last: false, message_id: 'message-0',
    }).buffer);
    (client as any).handleMessage(encodeVoiceIndication({
      type: 'tts_audio', session_id: 's', seq: 2, data: new Uint8Array([0, 0]),
      is_last: false, message_id: 'message-1',
    }).buffer);

    expect(events.filter((event: any) => event.type === 'tts_audio')).toHaveLength(1);
    expect((events.find((event: any) => event.type === 'tts_audio') as any).message_id).toBe('message-1');
  });

  it('keeps current message filtering isolated between clients', () => {
    const eventsA: unknown[] = [];
    const eventsB: unknown[] = [];
    const clientA = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: (event) => eventsA.push(event),
    });
    const clientB = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: (event) => eventsB.push(event),
    });
    expect(clientA.acceptAsrMessage('message-a', '用户 A')).toBe(true);
    expect(clientB.acceptAsrMessage('message-b', '用户 B')).toBe(true);

    const tts = (messageId: string) => encodeVoiceIndication({
      type: 'tts_audio', session_id: 's', seq: 1, data: new Uint8Array([0, 0]),
      is_last: false, message_id: messageId,
    }).buffer;
    (clientA as any).handleMessage(tts('message-a'));
    (clientA as any).handleMessage(tts('message-b'));
    (clientB as any).handleMessage(tts('message-b'));
    (clientB as any).handleMessage(tts('message-a'));

    expect(eventsA.filter((event: any) => event.type === 'tts_audio')).toHaveLength(1);
    expect((eventsA.find((event: any) => event.type === 'tts_audio') as any).message_id).toBe('message-a');
    expect(eventsB.filter((event: any) => event.type === 'tts_audio')).toHaveLength(1);
    expect((eventsB.find((event: any) => event.type === 'tts_audio') as any).message_id).toBe('message-b');
  });

  it('preserves message_id on LLM and TTS response events', () => {
    const events: unknown[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: (event) => events.push(event),
    });
    const messageId = 'utterance-1';
    (client as any).handleMessage(encodeVoiceIndication({
      type: 'llm_delta', session_id: 's', delta: '你好', is_final: false,
      message_id: messageId,
    }).buffer);
    (client as any).handleMessage(encodeVoiceIndication({
      type: 'tts_audio', session_id: 's', seq: 1, data: new Uint8Array([0, 0]),
      is_last: true, message_id: messageId,
    }).buffer);

    expect(events).toEqual([
      { type: 'llm_delta', text: '你好', message_id: messageId },
      expect.objectContaining({ type: 'tts_audio', message_id: messageId }),
    ]);
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
      data: new Uint8Array([1, 2]), is_last: true, sample_rate: 24_000, channels: 1, message_id: 'message-1',
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
      is_last: false, message_id: 'message-1',
    });
    (client as any).handleMessage(audio.buffer);
    const firstAudioAt = (client as any).firstAudioReceivedAt.get('message-1');
    expect(firstAudioAt).toEqual(expect.any(Number));
    (client as any).reportPlaybackStarted('message-1', firstAudioAt + 25);
    (client as any).reportPlaybackStarted('message-1', firstAudioAt + 30);
    expect(sent).toHaveLength(1);
    expect(decodeVoiceMessage(sent[0])).toMatchObject({
      type: 'client_metric_report', session_id: 's', message_id: 'message-1',
      metric: 'first_audio_received_to_playback', duration_ms: 25,
    });
  });

  it('retries playback metric reporting when WebSocket.send throws', () => {
    const sent: Uint8Array[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: () => {},
    });
    (client as any).sessionId = 's';
    (client as any).socket = { readyState: WebSocket.OPEN, send: () => { throw new Error('queue full'); } };
    (client as any).handleMessage(encodeVoiceIndication({
      type: 'tts_audio', session_id: 's', seq: 1, data: new Uint8Array([0, 0]),
      is_last: false, message_id: 'message-1',
    }).buffer);
    const firstAudioAt = (client as any).firstAudioReceivedAt.get('message-1');

    (client as any).reportPlaybackStarted('message-1', firstAudioAt + 25);
    (client as any).socket = { readyState: WebSocket.OPEN, send: (data: Uint8Array) => sent.push(data) };
    (client as any).reportPlaybackStarted('message-1', firstAudioAt + 30);

    expect(sent).toHaveLength(1);
    expect(decodeVoiceMessage(sent[0])).toMatchObject({
      type: 'client_metric_report', metric: 'first_audio_received_to_playback', duration_ms: 30,
    });
  });

  it('reports input end to final audio send after the final frame is accepted', () => {
    const sent: Uint8Array[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: () => {},
    });
    (client as any).sessionId = 's';
    (client as any).socket = { readyState: WebSocket.OPEN, send: (data: Uint8Array) => sent.push(data) };
    (client as any).now = vi.fn().mockReturnValueOnce(100).mockReturnValueOnce(104);

    client.sendAudio(new ArrayBuffer(2), true);

    expect(sent).toHaveLength(2);
    const finalAudio = decodeVoiceMessage(sent[0]);
    const metric = decodeVoiceMessage(sent[1]);
    expect(finalAudio).toMatchObject({ type: 'audio_chunk', is_last: true });
    expect(metric).toMatchObject({
      type: 'client_metric_report',
      message_id: finalAudio.message_id,
      metric: 'input_end_to_final_audio_sent',
      duration_ms: 4,
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

  it('ignores TTS audio from a stale message_id', () => {
    const events: unknown[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: (event) => events.push(event),
    });
    (client as any).acceptAsrMessage('message-2', '新问题');
    const audio = encodeVoiceIndication({
      type: 'tts_audio', session_id: 's', seq: 1, data: new Uint8Array([0, 0]),
      is_last: false, message_id: 'message-1',
    });
    (client as any).handleMessage(audio.buffer);

    expect(events.filter((event: any) => event.type === 'tts_audio')).toEqual([]);
  });

  it('ignores agent status from a stale message_id', () => {
    const events: unknown[] = [];
    const client = new VoiceServerClient({ baseUrl: 'http://localhost', token: '' }, {
      onState: () => {}, onEvent: (event) => events.push(event),
    });
    const status = encodeVoiceIndication({
      type: 'agent_status', session_id: 's', phase: 'speaking', label: '回答中',
      tool: null, message_id: 'message-2', done: false,
    });
    (client as any).acceptAsrMessage('message-2', '当前问题');
    (client as any).handleMessage(status.buffer);
    (client as any).handleMessage(encodeVoiceIndication({
      type: 'agent_status', session_id: 's', phase: 'speaking', label: '回答中',
      tool: null, message_id: 'message-1', done: false,
    }).buffer);

    expect(events.filter((event: any) => event.type === 'agent_status')).toHaveLength(1);
  });
});
