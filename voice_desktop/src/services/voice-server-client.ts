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
  private utteranceMessageId?: string;
  private utteranceComplete = false;
  private currentMessageId?: string;
  private firstAudioReceivedAt = new Map<string, number>();
  private playbackReported = new Map<string, number>();
  constructor(private readonly settings: Settings, private readonly callbacks: VoiceClientCallbacks) {}

  connect(sessionId: string = crypto.randomUUID()) {
    this.sessionId = sessionId;
    this.audioSeq = 0;
    this.utteranceStartedAt = undefined;
    this.utteranceMessageId = undefined;
    this.utteranceComplete = false;
    this.currentMessageId = undefined;
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
        codec: 'pcm_s16le', language: 'zh-CN',
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
      this.currentMessageId = undefined;
      this.utteranceMessageId = undefined;
      this.utteranceComplete = false;
      this.callbacks.onState('closed');
    };
  }

  sendAudio(data: ArrayBuffer, isLast = false) {
    if (this.socket?.readyState !== WebSocket.OPEN || !this.sessionId) return;
    const inputEndedAt = isLast ? this.now() : undefined;
    this.utteranceStartedAt ??= Date.now();
    if (!this.utteranceMessageId || this.utteranceComplete) {
      this.utteranceMessageId = crypto.randomUUID();
      this.utteranceComplete = false;
    }
    const messageId = this.utteranceMessageId;
    const sent = this.send({ type: 'audio_chunk', session_id: this.sessionId, seq: this.audioSeq++, timestamp_ms: Date.now() - this.utteranceStartedAt, data: new Uint8Array(data), is_last: isLast, message_id: messageId });
    if (isLast) {
      if (sent && inputEndedAt !== undefined) {
        this.reportClientMetric(messageId, 'input_end_to_final_audio_sent', this.now() - inputEndedAt);
      }
      this.utteranceStartedAt = undefined;
      this.utteranceComplete = true;
    }
  }
  interrupt() {
    this.clearPlaybackTimings();
    this.utteranceStartedAt = undefined;
    this.utteranceMessageId = undefined;
    this.utteranceComplete = false;
    this.currentMessageId = undefined;
    if (this.sessionId) this.send({ type: 'interrupt', session_id: this.sessionId });
  }
  acceptAsrMessage(messageId: string, text: string) {
    if (!text.trim() || !messageId) return false;
    if (this.utteranceMessageId && messageId !== this.utteranceMessageId) return false;
    const changed = this.currentMessageId !== messageId;
    this.currentMessageId = messageId;
    this.utteranceMessageId = undefined;
    this.utteranceComplete = false;
    return changed;
  }
  retry() { if (this.sessionId) this.send({ type: 'retry', session_id: this.sessionId }); }
  stop() { this.clearPlaybackTimings(); if (this.sessionId) this.send({ type: 'session_end', session_id: this.sessionId, reason: 'user' }); this.utteranceStartedAt = undefined; this.utteranceMessageId = undefined; this.utteranceComplete = false; this.currentMessageId = undefined; this.socket?.close(); }

  reportPlaybackStarted(messageId: string | undefined, playbackStartedAt = this.now()) {
    this.prunePlaybackTimings(playbackStartedAt);
    if (!messageId || this.playbackReported.has(messageId) || !this.sessionId || this.socket?.readyState !== WebSocket.OPEN) return;
    const receivedAt = this.firstAudioReceivedAt.get(messageId);
    if (receivedAt === undefined) return;
    const delay = playbackStartedAt - receivedAt;
    if (!Number.isFinite(delay) || delay < 0 || delay > 30_000) return;
    if (!this.reportClientMetric(messageId, 'first_audio_received_to_playback', delay)) return;
    this.playbackReported.set(messageId, playbackStartedAt);
    this.firstAudioReceivedAt.delete(messageId);
  }
  private reportClientMetric(messageId: string, metric: 'first_audio_received_to_playback' | 'input_end_to_final_audio_sent', durationMs: number) {
    if (!Number.isFinite(durationMs) || durationMs < 0 || durationMs > 30_000 || !this.sessionId) return false;
    return this.send({
      type: 'client_metric_report',
      session_id: this.sessionId,
      message_id: messageId,
      metric,
      duration_ms: Math.round(durationMs),
    });
  }
  private send(payload: Record<string, unknown>) {
    if (this.socket?.readyState === WebSocket.OPEN) {
      const message = { ...payload, message_id: payload.message_id ?? crypto.randomUUID() };
      const bytes = encodeVoiceIndication(message);
      console.info('[voice-ws] send', {
        sessionId: this.sessionId,
        type: payload['type'],
        messageId: message.message_id,
        bytes: bytes.byteLength,
      });
      try {
        this.socket.send(bytes as unknown as ArrayBuffer);
        return true;
      } catch (error) {
        console.warn('[voice-ws] send failed', {
          sessionId: this.sessionId,
          type: payload['type'],
          messageId: message.message_id,
          error: String(error),
        });
      }
    }
    return false;
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
        const messageId = String(raw.message_id ?? '');
        if (this.currentMessageId && messageId !== this.currentMessageId && messageId !== this.utteranceMessageId) return;
        event = raw.is_final
          ? { type: 'asr_final', text: String(raw.text ?? ''), message_id: messageId }
          : { type: 'asr_partial', text: String(raw.text ?? ''), message_id: messageId };
      } else if (type === 'llm_delta') {
        const messageId = String(raw.message_id ?? '');
        if (this.currentMessageId && messageId !== this.currentMessageId) return;
        event = { type: 'llm_delta', text: String(raw.delta ?? ''), message_id: messageId };
      } else if (type === 'tts_audio') {
        const audio = raw.data instanceof Uint8Array ? raw.data : new Uint8Array(raw.data ?? []);
        const messageId = String(raw.message_id ?? '');
        if (this.currentMessageId && messageId !== this.currentMessageId) return;
        if (messageId && !this.firstAudioReceivedAt.has(messageId)) {
          const receivedAt = this.now();
          this.firstAudioReceivedAt.set(messageId, receivedAt);
          setTimeout(() => {
            if (this.firstAudioReceivedAt.get(messageId) === receivedAt) this.firstAudioReceivedAt.delete(messageId);
          }, 30_000);
        }
        event = { type: 'tts_audio', audio, seq: Number(raw.seq ?? 0), is_last: Boolean(raw.is_last), sample_rate: Number(raw.sample_rate ?? 0) || undefined, channels: Number(raw.channels ?? 0) || undefined, message_id: messageId };
      } else if (type === 'agent_status') {
        const messageId = String(raw.message_id ?? '');
        if (this.currentMessageId && messageId !== this.currentMessageId) return;
        event = { type: 'agent_status', phase: String(raw.phase ?? ''), label: String(raw.label ?? ''), done: Boolean(raw.done), message_id: messageId };
      } else if (type === 'error') {
        const messageId = raw.message_id == null ? undefined : String(raw.message_id);
        if (this.currentMessageId && messageId && messageId !== this.currentMessageId) return;
        event = { type: 'error', message: String(raw.message ?? '服务端错误'), ...(messageId ? { message_id: messageId } : {}) };
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
    for (const [messageId, receivedAt] of this.firstAudioReceivedAt) {
      if (now - receivedAt > 30_000) this.firstAudioReceivedAt.delete(messageId);
    }
    for (const [messageId, reportedAt] of this.playbackReported) {
      if (now - reportedAt > 30_000) this.playbackReported.delete(messageId);
    }
    while (this.playbackReported.size > 512) {
      const oldest = this.playbackReported.keys().next().value;
      if (oldest === undefined) break;
      this.playbackReported.delete(oldest);
    }
  }
}
