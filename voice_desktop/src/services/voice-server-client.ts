import type { Settings } from './settings-service';
import type { VoiceClientCallbacks, VoiceEvent } from '../types/voice-protocol';
import { decodeVoiceMessage, encodeVoiceIndication } from './msgpack';

export function normalizeBaseUrl(value: string) {
  const trimmed = value.trim().replace(/\/$/, '');
  if (!/^https?:\/\//i.test(trimmed)) throw new Error('服务地址必须以 http:// 或 https:// 开头');
  return trimmed;
}

export function buildWsUrl(settings: Settings, sessionId: string) {
  const base = normalizeBaseUrl(settings.baseUrl);
  const wsBase = base.replace(/^http/i, 'ws');
  return `${wsBase}/ws/voice/web/${encodeURIComponent(sessionId)}`;
}

export class VoiceServerClient {
  private socket?: WebSocket;
  private sessionId?: string;
  private audioSeq = 0;
  private utteranceStartedAt?: number;
  private invalidatedRequestId = 0;
  private activeRequestId = 0;
  private firstAudioReceivedAt = new Map<number, number>();
  private playbackReported = new Set<number>();
  constructor(private readonly settings: Settings, private readonly callbacks: VoiceClientCallbacks) {}

  connect(sessionId = crypto.randomUUID()) {
    this.sessionId = sessionId;
    this.audioSeq = 0;
    this.utteranceStartedAt = undefined;
    this.invalidatedRequestId = 0;
    this.activeRequestId = 0;
    this.firstAudioReceivedAt.clear();
    this.playbackReported.clear();
    this.callbacks.onState('connecting');
    console.info('[voice-ws] connecting', { sessionId });
    this.socket = new WebSocket(buildWsUrl(this.settings, sessionId));
    this.socket.binaryType = 'arraybuffer';
    this.socket.onopen = () => {
      console.info('[voice-ws] connected', { sessionId });
      this.send({
        type: 'session_start', session_id: sessionId, sample_rate: 16000, channels: 1,
        codec: 'pcm_s16le', language: 'zh-CN', tts_sample_rate: 24000,
        ...(this.settings.voice?.trim() ? { voice: this.settings.voice.trim() } : {}),
      });
    };
    this.socket.onmessage = (message) => {
      this.handleMessage(message.data);
    };
    this.socket.onerror = () => {
      console.warn('[voice-ws] error', { sessionId: this.sessionId });
      this.callbacks.onState('error');
    };
    this.socket.onclose = (event) => {
      console.info('[voice-ws] closed', { sessionId: this.sessionId, code: event.code, reason: event.reason });
      this.clearPlaybackTimings();
      this.callbacks.onState('closed');
    };
  }

  sendAudio(data: ArrayBuffer, isLast = false) {
    if (this.socket?.readyState !== WebSocket.OPEN || !this.sessionId) return;
    this.utteranceStartedAt ??= Date.now();
    this.send({ type: 'audio_chunk', session_id: this.sessionId, seq: this.audioSeq++, timestamp_ms: Date.now() - this.utteranceStartedAt, data: new Uint8Array(data), is_last: isLast });
    if (isLast) this.utteranceStartedAt = undefined;
  }
  interrupt() { this.invalidatedRequestId = Math.max(this.invalidatedRequestId, this.activeRequestId); this.clearPlaybackTimings(); if (this.sessionId) this.send({ type: 'interrupt', session_id: this.sessionId }); }
  retry() { if (this.sessionId) this.send({ type: 'retry', session_id: this.sessionId }); }
  stop() { this.clearPlaybackTimings(); if (this.sessionId) this.send({ type: 'session_end', session_id: this.sessionId, reason: 'user' }); this.utteranceStartedAt = undefined; this.socket?.close(); }

  reportPlaybackStarted(requestId: number | undefined, playbackStartedAt = this.now()) {
    if (!requestId || this.playbackReported.has(requestId) || !this.sessionId || this.socket?.readyState !== WebSocket.OPEN) return;
    this.prunePlaybackTimings(playbackStartedAt);
    const receivedAt = this.firstAudioReceivedAt.get(requestId);
    if (receivedAt === undefined) return;
    const delay = playbackStartedAt - receivedAt;
    if (!Number.isFinite(delay) || delay < 0 || delay > 30_000) return;
    this.playbackReported.add(requestId);
    this.send({ type: 'playback_started', session_id: this.sessionId, request_id: requestId, delay_ms: Math.round(delay) });
    this.firstAudioReceivedAt.delete(requestId);
  }
  private send(payload: Record<string, unknown>) {
    if (this.socket?.readyState === WebSocket.OPEN) {
      const message = { ...payload, message_id: crypto.randomUUID() };
      const bytes = encodeVoiceIndication(message);
      console.info('[voice-ws] send', {
        sessionId: this.sessionId,
        type: payload['type'],
        messageId: message.message_id,
        bytes: bytes.byteLength,
      });
      this.socket.send(bytes as unknown as ArrayBuffer);
    }
  }

  private handleMessage(data: ArrayBuffer | string) {
    try {
      if (typeof data === 'string') throw new Error('服务端返回了不兼容的文本帧');
      const raw = decodeVoiceMessage(data);
      const type = String(raw.type ?? '').toLowerCase();
      console.info('[voice-ws] receive', {
        sessionId: this.sessionId,
        type,
        messageId: raw.message_id,
        bytes: data.byteLength,
      });
      let event: VoiceEvent | undefined;
      if (type === 'session_ack') {
        if (raw.success) this.callbacks.onState('connected');
        else event = { type: 'error', message: String(raw.message ?? '服务端拒绝会话') };
      } else if (type === 'asr_partial') {
        event = raw.is_final ? { type: 'asr_final', text: String(raw.text ?? '') } : { type: 'asr_partial', text: String(raw.text ?? '') };
      } else if (type === 'llm_delta') {
        event = { type: 'llm_delta', text: String(raw.delta ?? '') };
      } else if (type === 'tts_audio') {
        const audio = raw.data instanceof Uint8Array ? raw.data : new Uint8Array(raw.data ?? []);
        const requestId = Number(raw.request_id ?? 0) || 0;
        if (requestId && requestId <= this.invalidatedRequestId) return;
        if (requestId) this.activeRequestId = Math.max(this.activeRequestId, requestId);
        if (requestId && !this.firstAudioReceivedAt.has(requestId)) {
          const receivedAt = this.now();
          this.firstAudioReceivedAt.set(requestId, receivedAt);
          setTimeout(() => {
            if (this.firstAudioReceivedAt.get(requestId) === receivedAt) this.firstAudioReceivedAt.delete(requestId);
          }, 30_000);
        }
        event = { type: 'tts_audio', audio, seq: Number(raw.seq ?? 0), is_last: Boolean(raw.is_last), sample_rate: Number(raw.sample_rate ?? 0) || undefined, channels: Number(raw.channels ?? 0) || undefined, request_id: requestId || undefined };
      } else if (type === 'agent_status') {
        const requestId = Number(raw.request_id ?? 0) || 0;
        if (requestId && requestId <= this.invalidatedRequestId) return;
        if (requestId) this.activeRequestId = Math.max(this.activeRequestId, requestId);
        event = { type: 'agent_status', phase: String(raw.phase ?? ''), label: String(raw.label ?? ''), done: Boolean(raw.done), request_id: requestId || undefined };
      } else if (type === 'error') {
        event = { type: 'error', message: String(raw.message ?? '服务端错误') };
      }
      if (event) this.callbacks.onEvent(event);
    } catch (error) {
      console.warn('[voice-ws] receive decode failed', {
        sessionId: this.sessionId,
        bytes: typeof data === 'string' ? data.length : data.byteLength,
        error: String(error),
      });
      this.callbacks.onEvent({ type: 'error', message: '收到无法解析的服务端消息' });
    }
  }

  private now() { return typeof performance !== 'undefined' ? performance.now() : Date.now(); }
  private clearPlaybackTimings() { this.firstAudioReceivedAt.clear(); this.playbackReported.clear(); }
  private prunePlaybackTimings(now: number) {
    for (const [requestId, receivedAt] of this.firstAudioReceivedAt) {
      if (now - receivedAt > 30_000) this.firstAudioReceivedAt.delete(requestId);
    }
  }
}
