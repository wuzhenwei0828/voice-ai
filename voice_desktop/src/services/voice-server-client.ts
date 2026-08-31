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
  constructor(private readonly settings: Settings, private readonly callbacks: VoiceClientCallbacks) {}

  connect(sessionId = crypto.randomUUID()) {
    this.sessionId = sessionId;
    this.audioSeq = 0;
    this.utteranceStartedAt = undefined;
    this.invalidatedRequestId = 0;
    this.activeRequestId = 0;
    this.callbacks.onState('connecting');
    this.socket = new WebSocket(buildWsUrl(this.settings, sessionId));
    this.socket.binaryType = 'arraybuffer';
    this.socket.onopen = () => {
      this.send({
        type: 'session_start', session_id: sessionId, sample_rate: 16000, channels: 1,
        codec: 'pcm_s16le', language: 'zh-CN', tts_sample_rate: 24000,
        ...(this.settings.voice?.trim() ? { voice: this.settings.voice.trim() } : {}),
      });
    };
    this.socket.onmessage = (message) => this.handleMessage(message.data);
    this.socket.onerror = () => this.callbacks.onState('error');
    this.socket.onclose = () => this.callbacks.onState('closed');
  }

  sendAudio(data: ArrayBuffer, isLast = false) {
    if (this.socket?.readyState !== WebSocket.OPEN || !this.sessionId) return;
    this.utteranceStartedAt ??= Date.now();
    this.send({ type: 'audio_chunk', session_id: this.sessionId, seq: this.audioSeq++, timestamp_ms: Date.now() - this.utteranceStartedAt, data: new Uint8Array(data), is_last: isLast });
    if (isLast) this.utteranceStartedAt = undefined;
  }
  interrupt() { this.invalidatedRequestId = Math.max(this.invalidatedRequestId, this.activeRequestId); if (this.sessionId) this.send({ type: 'interrupt', session_id: this.sessionId }); }
  retry() { if (this.sessionId) this.send({ type: 'retry', session_id: this.sessionId }); }
  stop() { if (this.sessionId) this.send({ type: 'session_end', session_id: this.sessionId, reason: 'user' }); this.utteranceStartedAt = undefined; this.socket?.close(); }
  private send(payload: Record<string, unknown>) { if (this.socket?.readyState === WebSocket.OPEN) this.socket.send(encodeVoiceIndication(payload) as unknown as ArrayBuffer); }

  private handleMessage(data: ArrayBuffer | string) {
    try {
      if (typeof data === 'string') throw new Error('服务端返回了不兼容的文本帧');
      const raw = decodeVoiceMessage(data);
      const type = String(raw.type ?? '').toLowerCase();
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
    } catch { this.callbacks.onEvent({ type: 'error', message: '收到无法解析的服务端消息' }); }
  }
}
