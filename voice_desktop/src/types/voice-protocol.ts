export type VoiceEvent =
  | { type: 'asr_partial'; text: string }
  | { type: 'asr_final'; text: string }
  | { type: 'llm_delta'; text: string }
  | { type: 'tts_audio'; audio: Uint8Array; seq?: number; is_last?: boolean; sample_rate?: number; channels?: number; request_id?: number }
  | { type: 'agent_status'; phase: string; label: string; done: boolean; request_id?: number }
  | { type: 'error'; message: string };

export type VoiceClientCallbacks = {
  onEvent: (event: VoiceEvent) => void;
  onState: (state: 'connecting' | 'connected' | 'closed' | 'error') => void;
};
