import { useReducer, useRef, useState } from 'react';
import { AppShell } from '../../components/AppShell';
import { SettingsPanel } from '../../components/SettingsPanel';
import { VoiceControls } from '../../components/VoiceControls';
import { AudioPlayer } from '../../services/audio-player';
import { AudioRecorder } from '../../services/audio-recorder';
import { loadSettings, saveSettings, type Settings } from '../../services/settings-service';
import { VoiceServerClient } from '../../services/voice-server-client';
import { conversationReducer, getPhaseCopy, initialConversationState } from './conversation-store';
import type { Message } from './conversation-types';

function connectionLabel(connection: string) {
  if (connection === 'online') return ['WS：已连接', 'badge-online'];
  if (connection === 'connecting') return ['WS：连接中', 'badge-busy'];
  if (connection === 'error') return ['WS：连接失败', 'badge-offline'];
  return ['未连接', 'badge-offline'];
}

function MessageLog({ messages, error }: { messages: Message[]; error?: string }) {
  if (messages.length === 0 && !error) return <div className="empty-hint">还没开始说话</div>;
  return <>
    {messages.map((message) => <div className={`msg ${message.role}${message.pending ? ' partial' : ''}`} key={message.id}>
      <div className="role">{message.role === 'user' ? '👤 你' : message.role === 'assistant' ? '🤖 助手' : '⚙️ 系统'}</div>
      <div className="text">{message.text}</div>
    </div>)}
    {error && <div className="msg error"><div className="role">❌ 错误</div><div className="text">{error}</div></div>}
  </>;
}

export function ConversationPage() {
  const [state, dispatch] = useReducer(conversationReducer, initialConversationState);
  const [settings, setSettings] = useState<Settings>(loadSettings);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [micMuted, setMicMuted] = useState(false);
  const [speakerMuted, setSpeakerMuted] = useState(false);
  const client = useRef<VoiceServerClient | undefined>(undefined);
  const recorder = useRef(new AudioRecorder());
  const player = useRef(new AudioPlayer());
  const micMutedRef = useRef(false);
  const speakerMutedRef = useRef(false);
  const startingRef = useRef(false);
  const active = state.connection === 'online' || state.connection === 'connecting';
  const copy = state.phase === 'idle' ? { label: '准备就绪', substatus: '点下方按钮开始对话', state: 'idle' as const } : getPhaseCopy(state.phase);
  const [connText, connClass] = connectionLabel(state.connection);

  const updateSettings = (next: Settings) => { setSettings(next); saveSettings(next); };

  const start = async () => {
    if (startingRef.current || active) return;
    startingRef.current = true;
    try {
      dispatch({ type: 'connecting' });
      void player.current.resume().catch(() => {});
      let nextClient!: VoiceServerClient;
      nextClient = new VoiceServerClient(settings, {
        onState: (value) => {
          if (client.current !== nextClient) return;
          if (value === 'error') { recorder.current.stop(); player.current.stop(); }
          dispatch({ type: value });
        },
        onEvent: (event) => {
          if (client.current !== nextClient) return;
          if (event.type === 'error') dispatch({ type: 'server_error', message: event.message });
          else if (event.type === 'agent_status') dispatch({ type: 'agent_status', phase: event.phase, done: event.done });
          else if (event.type === 'tts_audio') {
            if (!speakerMutedRef.current) player.current.enqueue(event.audio, event.sample_rate, event.channels, event.is_last);
          } else if (event.type === 'asr_partial' || event.type === 'asr_final' || event.type === 'llm_delta') dispatch({ type: event.type, text: event.text });
        },
      });
      client.current = nextClient;
      nextClient.connect();
      await recorder.current.start((frame, isLast) => { if (!micMutedRef.current) nextClient.sendAudio(frame, isLast); });
    } catch (error) {
      recorder.current.stop(); player.current.stop(); client.current?.stop(); client.current = undefined;
      dispatch({ type: 'error', message: error instanceof Error ? error.message : '无法访问麦克风' });
    } finally {
      startingRef.current = false;
    }
  };

  const toggleMic = () => setMicMuted((muted) => { micMutedRef.current = !muted; return !muted; });
  const toggleSpeaker = () => setSpeakerMuted((muted) => { const next = !muted; speakerMutedRef.current = next; if (next) player.current.stop(); return next; });
  const retry = () => { if (!active || !state.canRetry) return; player.current.stop(); client.current?.retry(); dispatch({ type: 'retry' }); };
  const interrupt = () => { client.current?.interrupt(); recorder.current.reset(); player.current.stop(); dispatch({ type: 'listening' }); };
  const stop = () => { startingRef.current = false; recorder.current.stop(); player.current.stop(); client.current?.stop(); client.current = undefined; dispatch({ type: 'closed' }); };

  return <AppShell>
    <header>
      <div className="header-row">
        <h1>语音知识助手</h1>
      </div>
      <div className="status-bar" aria-label="设备状态">
        <span className={`badge ${connClass}`}>{connText}</span>
        <span className={`badge ${active || micMuted ? 'badge-busy' : 'badge-offline'}`}>麦克风：{micMuted ? '已静音' : active ? '收音中' : '关'}</span>
        <span className={`badge ${speakerMuted || state.phase === 'speaking' ? 'badge-busy' : 'badge-offline'}`}>扬声器：{speakerMuted ? '已静音' : state.phase === 'speaking' ? '播放中' : '空闲'}</span>
      </div>
    </header>

    <section className="tab-pane phone-call">
      <div className="phone-mute-controls phone-mute-top">
        <button className="mute-btn" data-muted={micMuted} onClick={toggleMic} disabled={!active} title="麦克风静音 / 恢复"><span className="mute-btn-icon">{micMuted ? '🚫🎤' : '🎤'}</span><span className="mute-btn-label">{micMuted ? '麦克风静音中' : '麦克风'}</span></button>
        <button className="mute-btn" data-muted={speakerMuted} onClick={toggleSpeaker} disabled={!active} title="扬声器静音 / 恢复"><span className="mute-btn-icon">{speakerMuted ? '🔇' : '🔊'}</span><span className="mute-btn-label">{speakerMuted ? '扬声器静音中' : '扬声器'}</span></button>
      </div>

      <div className="user-mode-layout">
        <div className="phone-stage">
          <div className="phone-avatar-wrap" data-state={copy.state}>
            <div className="phone-wave phone-wave-1" /><div className="phone-wave phone-wave-2" /><div className="phone-wave phone-wave-3" />
            <div className="phone-avatar"><span className="phone-avatar-emoji">🤖</span></div>
          </div>
          <div className="phone-status" role="status" aria-live="polite">{copy.label}</div>
          <div className="phone-substatus">{copy.substatus}</div>
          {active && state.canRetry && <button className="retry-action" type="button" onClick={retry}>重试</button>}
          <div className="phone-voice-name">音色：<span>{settings.voice?.trim() || '默认'}</span></div>
          <VoiceControls active={active} onStart={start} onInterrupt={interrupt} onStop={stop} />
        </div>

        <div className="user-mode-panel">
          <details className="phone-details transcript-details" open>
            <summary>对话字幕</summary>
            <div className="conv-log phone-conv-log" aria-live="polite"><MessageLog messages={state.messages} error={state.error} /></div>
          </details>
          <details className="phone-details source-details" hidden><summary>参考来源</summary><ol className="source-list" /></details>
        </div>
      </div>

      <details className="phone-details settings-details" open={settingsOpen} onToggle={(event) => setSettingsOpen(event.currentTarget.open)}>
        <summary>⚙️ 设置</summary>
        <SettingsPanel settings={settings} onChange={updateSettings} />
      </details>
    </section>
  </AppShell>;
}
