import type { ConversationState } from './conversation-types';
export const initialConversationState: ConversationState = { connection: 'offline', phase: 'idle', messages: [] };
export type PhaseCopy = { label: string; substatus: string; state: ConversationState['phase'] };
const PHASE_COPY: Record<string, PhaseCopy> = {
  listening: { label: '正在听', substatus: '请直接说话', state: 'listening' },
  transcribing: { label: '正在理解', substatus: '', state: 'thinking' },
  searching: { label: '正在查资料', substatus: '我查一下相关信息', state: 'thinking' },
  composing: { label: '正在组织答案', substatus: '我整理一下', state: 'thinking' },
  speaking: { label: '正在回答', substatus: '可随时打断', state: 'speaking' },
  error: { label: '暂时遇到问题', substatus: '请稍后再试', state: 'error' },
};
export function shouldStopPlaybackForAsr(text: string, messageId: string, currentMessageId: string | undefined): boolean {
  return Boolean(text.trim()) && Boolean(messageId) && messageId !== currentMessageId;
}
export function getPhaseCopy(phase: string): PhaseCopy { return PHASE_COPY[phase] ?? { label: '正在处理', substatus: '', state: 'thinking' }; }
export function conversationReducer(state: ConversationState, action: { type: string; text?: string; message?: string; phase?: string; done?: boolean }): ConversationState {
  switch (action.type) {
    case 'connecting': return { ...state, connection: 'connecting', error: undefined };
    case 'connected': return { ...state, connection: 'online', phase: 'listening', error: undefined, canRetry: false };
    case 'closed': return { ...state, connection: 'offline', phase: 'idle', error: undefined, canRetry: false };
    case 'error': return { ...state, connection: 'error', phase: 'idle', error: action.message ?? '连接失败' };
    case 'server_error': return { ...state, phase: 'error', error: action.message ?? '服务暂时不可用', canRetry: true };
    case 'agent_status': {
      const phase = action.phase === 'error' ? 'error' : action.done ? 'listening' : getPhaseCopy(action.phase ?? '').state;
      return { ...state, phase, error: action.phase === 'error' ? state.error : undefined, canRetry: action.phase === 'error' ? state.canRetry : false };
    }
    case 'retry': return { ...state, phase: 'thinking', error: undefined, canRetry: false };
    case 'listening': return { ...state, phase: 'listening', error: undefined, canRetry: false };
    case 'asr_partial': return { ...state, phase: 'listening', error: undefined, canRetry: false, messages: [...state.messages.filter((item) => !item.pending), { id: 'user-live', role: 'user', text: action.text ?? '', pending: true }] };
    case 'asr_final': { const messages = state.messages.filter((item) => item.id !== 'user-live'); return action.text?.trim() ? { ...state, phase: 'thinking', error: undefined, canRetry: false, messages: [...messages, { id: crypto.randomUUID(), role: 'user', text: action.text }] } : { ...state, messages }; }
    case 'llm_delta': { const current = state.messages.find((item) => item.id === 'assistant-live'); const text = `${current?.text ?? ''}${action.text ?? ''}`; return { ...state, phase: 'speaking', error: undefined, canRetry: false, messages: [...state.messages.filter((item) => item.id !== 'assistant-live'), { id: 'assistant-live', role: 'assistant', text, pending: true }] }; }
    case 'reset': return initialConversationState;
    default: return state;
  }
}
