export type VoiceEvent =
  | { type: 'asr_partial'; text: string; message_id: string }
  | { type: 'asr_final'; text: string; message_id: string }
  | { type: 'llm_delta'; text: string; message_id: string }
  | { type: 'tts_audio'; audio: Uint8Array; seq?: number; is_last?: boolean; sample_rate?: number; channels?: number; message_id: string }
  | { type: 'agent_status'; phase: string; label: string; done: boolean; message_id: string }
  | { type: 'error'; message: string; message_id?: string };

export type VoiceClientCallbacks = {
  onEvent: (event: VoiceEvent) => void;
  onState: (state: 'connecting' | 'connected' | 'closed' | 'error') => void;
};
