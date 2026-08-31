import { describe, expect, it } from 'vitest';
import { conversationReducer, getPhaseCopy, initialConversationState } from '../src/features/conversation/conversation-store';
import { EnergyVad, floatToPcm16k } from '../src/services/audio-recorder';
describe('conversation reducer', () => { it('keeps partial user text temporary and finalizes it', () => { const partial = conversationReducer(initialConversationState, { type: 'asr_partial', text: '你好' }); expect(partial.messages[0].pending).toBe(true); const final = conversationReducer(partial, { type: 'asr_final', text: '你好' }); expect(final.messages).toHaveLength(1); expect(final.messages[0].pending).toBeUndefined(); }); it('appends assistant deltas as a live response', () => { const first = conversationReducer(initialConversationState, { type: 'llm_delta', text: '你好，' }); const next = conversationReducer(first, { type: 'llm_delta', text: '世界' }); expect(next.messages[0]).toMatchObject({ role: 'assistant', text: '你好，世界', pending: true }); }); it('keeps an active connection when the server reports a pipeline error', () => { const connected = conversationReducer(initialConversationState, { type: 'connected' }); const next = conversationReducer(connected, { type: 'server_error', message: 'TTS 服务不可用' }); expect(next.connection).toBe('online'); expect(next.phase).toBe('error'); expect(next.canRetry).toBe(true); expect(next.error).toBe('TTS 服务不可用'); }); it('maps server phases and clears retry after replay', () => { const connected = conversationReducer(initialConversationState, { type: 'connected' }); const speaking = conversationReducer(connected, { type: 'agent_status', phase: 'speaking' }); expect(speaking.phase).toBe('speaking'); const replaying = conversationReducer(speaking, { type: 'retry' }); expect(replaying.phase).toBe('thinking'); expect(replaying.canRetry).toBe(false); }); it('clears error when the active session is ended', () => { const connected = conversationReducer(initialConversationState, { type: 'connected' }); const failed = conversationReducer(connected, { type: 'server_error', message: '失败' }); const ended = conversationReducer(failed, { type: 'closed' }); expect(ended.error).toBeUndefined(); expect(ended.canRetry).toBe(false); expect(ended.connection).toBe('offline'); }); it('marks a real connection failure as inactive', () => { const connected = conversationReducer(initialConversationState, { type: 'connected' }); const next = conversationReducer(connected, { type: 'error', message: 'WS 断开' }); expect(next.connection).toBe('error'); expect(next.phase).toBe('idle'); }); });
describe('agent completion', () => {
  it('returns to listening when the server marks a speaking response done', () => {
    const state = { ...initialConversationState, connection: 'online' as const, phase: 'speaking' as const };
    const next = conversationReducer(state, { type: 'agent_status', phase: 'speaking', done: true });
    expect(next.phase).toBe('listening');
  });
});

describe('PC phase copy', () => {
  it('uses the same Chinese labels and state names as the browser page', () => {
    expect(getPhaseCopy('listening')).toEqual({ label: '正在听', substatus: '请直接说话', state: 'listening' });
    expect(getPhaseCopy('searching')).toEqual({ label: '正在查资料', substatus: '我查一下相关信息', state: 'thinking' });
    expect(getPhaseCopy('speaking')).toEqual({ label: '正在回答', substatus: '可随时打断', state: 'speaking' });
    expect(getPhaseCopy('error')).toEqual({ label: '暂时遇到问题', substatus: '请稍后再试', state: 'error' });
  });

  it('falls back to the processing state for an unknown server phase', () => {
    expect(getPhaseCopy('unexpected')).toEqual({ label: '正在处理', substatus: '', state: 'thinking' });
  });
});

describe('audio capture helpers', () => {
  it('resamples float audio to signed 16-bit 16k PCM', () => {
    const pcm = new Int16Array(floatToPcm16k(new Float32Array([0, 1, -1, 0.5]), 16_000));
    expect([...pcm]).toEqual([0, 32767, -32768, 16384]);
  });

  it('emits one isLast frame after sustained silence, not every silent frame', () => {
    const vad = new EnergyVad({ startFrames: 2, endFrames: 3, prerollFrames: 1 });
    const frame = (value: number) => new Int16Array(320).fill(value);
    expect(vad.push(frame(1000))).toHaveLength(0);
    expect(vad.push(frame(1000))).toHaveLength(2);
    expect(vad.push(frame(0))).toHaveLength(1);
    expect(vad.push(frame(0))).toHaveLength(1);
    const end = vad.push(frame(0));
    expect(end).toHaveLength(1);
    expect(end[0].isLast).toBe(true);
    expect(vad.push(frame(0))).toHaveLength(0);
  });
});
