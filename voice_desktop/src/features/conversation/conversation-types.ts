export type Message = { id: string; role: 'user' | 'assistant' | 'system'; text: string; pending?: boolean };
export type ConversationState = { connection: 'offline' | 'connecting' | 'online' | 'error'; phase: 'idle' | 'listening' | 'thinking' | 'speaking' | 'error'; messages: Message[]; error?: string; canRetry?: boolean };
