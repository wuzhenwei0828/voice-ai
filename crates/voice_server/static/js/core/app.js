// voice-ai web admin — 浏览器端 voice pipeline
// 流程：
//   1. WS 连接 /ws/voice/web/admin/listener
//   2. 发送 SessionStart (codec=pcm_s16le, sample_rate=16000, channels=1)
//   3. AudioContext + AudioWorklet 持续采集麦克风 → Float32 → 转 s16le → 二进制帧 → WS
//   4. AudioWorklet 内：按 FRAME_SAMPLES 攒帧 + 重采样 + RMS 能量 VAD；
//      起句前 ~100ms 前置静音随首帧一起发；静音满 SILENCE_FRAMES_TO_END（600ms）
//      的那一帧边沿标一次 is_last=true
//   5. 接收下行：
//        - AsrPartial    → 在 user bubble 里显示/替换
//        - LlmDelta      → 追加到 assistant bubble
//        - TtsAudio      → s16le chunk 到达即通过 Web Audio 排程播放
//        - Error         → 在用户状态区显示安全提示

// ====== 可测试的用户状态辅助 ======
(function initVoiceAgentUi(root) {
  const PHASE_COPY = Object.freeze({
    listening: { label: '正在听', substatus: '请直接说话', state: 'listening' },
    transcribing: { label: '正在理解', substatus: '', state: 'thinking' },
    searching: { label: '正在查资料', substatus: '我查一下相关信息', state: 'thinking' },
    composing: { label: '正在组织答案', substatus: '我整理一下', state: 'thinking' },
    speaking: { label: '正在回答', substatus: '可随时打断', state: 'speaking' },
    error: { label: '暂时遇到问题', substatus: '请稍后再试', state: 'error' },
  });

  function phaseCopy(phase) {
    return PHASE_COPY[phase] || { label: '正在处理', substatus: '', state: 'thinking' };
  }

  function agentStatusLabel(phase) {
    return phaseCopy(phase).label;
  }

  function agentStatusSubstatus(phase) {
    return phaseCopy(phase).substatus;
  }

  function shouldStopPlaybackForAsr(text, messageId, currentMessageId) {
    return Boolean(String(text || '').trim()) && Boolean(messageId) && messageId !== currentMessageId;
  }

  function executeRetry({
    canRetry,
    socketOpen,
    stopPlayback,
    sendRetry,
  }) {
    if (!canRetry || !socketOpen) return false;
    stopPlayback();
    sendRetry();
    return true;
  }

  function createClientMetricReport({ sessionId, messageId, metric, startedAt, endedAt }) {
    const allowedMetrics = new Set([
      'first_audio_received_to_playback',
      'input_end_to_final_audio_sent',
    ]);
    const durationMs = endedAt - startedAt;
    if (!sessionId || !messageId || !allowedMetrics.has(metric)
      || !Number.isFinite(durationMs) || durationMs < 0 || durationMs > 30000) return null;
    return {
      type: 'client_metric_report',
      session_id: sessionId,
      message_id: messageId,
      metric,
      duration_ms: Math.round(durationMs),
    };
  }

  function trySendWebSocket(socket, bytes) {
    try {
      socket.send(bytes);
      return true;
    } catch {
      return false;
    }
  }

  const api = {
    PHASE_COPY,
    agentStatusLabel,
    agentStatusSubstatus,
    shouldStopPlaybackForAsr,
    executeRetry,
    createClientMetricReport,
    trySendWebSocket,
  };
  root.VoiceAgentUi = api;
  if (typeof module === 'object' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);

// ====== 浏览器应用 ======

(function () {
  'use strict';

  if (typeof document === 'undefined') return;

  // ====== 配置 ======
  const SAMPLE_RATE = 16000;
  const CHANNELS = 1;
  const FRAME_SAMPLES = 320;        // 20ms @ 16kHz，一帧 640 字节
  const SILENCE_THRESHOLD = 0.01;   // RMS 阈值（Float32 量纲）
  const VAD_START_FRAMES = 5;       // 起句去抖：连续 3 帧浊音（60ms）
  const VAD_END_FRAMES = 40;        // 句尾：连续 30 帧静音（600ms）
  const VAD_MAX_FRAMES = 1500;      // 单句上限 30s，与服务端 MAX_UTTERANCE_MS 呼应
  const PREROLL_FRAMES = 5;         // 起句前保留 ~100ms 前置静音帧
  // URL 路径匹配 webhttp WS 路由 /{api_prefix}/{wsapi}/{business}/{actor}/{connid}
  //   api_prefix = "/"  wsapi = "ws"  →  "/ws/{business}/{actor}/{connid}"
  // 三段分别：business=voice, actor=web, connid=<每次 Start 重新生成的 sessionId>
  // 这样 Stop 后再 Start 会拿到全新 connid，服务端按路径生成新 session_id，
  // 避免复用旧 session 残留的 closed=true 状态。
  function buildWsUrl(sid) {
    return (location.protocol === 'https:' ? 'wss://' : 'ws://') +
           location.host + '/ws/voice/web/' + sid;
  }

  // ====== 元素 ======
  const $ = (id) => document.getElementById(id);
  const convLog = $('conv-log');
  const btnStart = $('btn-start');
  const btnInterrupt = $('btn-interrupt');
  const btnStop = $('btn-stop');
  const connStatus = $('conn-status');
  const micStatus = $('mic-status');
  const speakerStatus = $('speaker-status');
  const btnMicMute = $('btn-mic-mute');
  const btnSpeakerMute = $('btn-speaker-mute');
  const retryAction = $('retry-action');
  const sourceDetails = $('source-details');
  const sourceList = $('source-list');
  const agentUi = window.VoiceAgentUi;
  let currentMessageId = null;
  let canRetryLastRequest = false;

  // ====== 麦/喇叭手动静音 ======
  // micMuted = true 时：worklet 帧不上送（保持 WS 连接，不打断对话）
  // speakerMuted = true 时：TTS chunk 直接丢弃 + 清空已排队音频
  let micMuted = false;
  let speakerMuted = false;

  function applyMuteUi(btn, muted, label, onIcon, offIcon) {
    btn.dataset.muted = muted ? 'true' : 'false';
    btn.title = muted ? `${label}：已静音（点击恢复）` : `${label}：正常（点击静音）`;
    const icon = btn.querySelector('.mute-btn-icon');
    const lbl = btn.querySelector('.mute-btn-label');
    if (icon) icon.textContent = muted ? offIcon : onIcon;
    if (lbl) lbl.textContent = muted ? `${label}静音中` : label;
  }

  function setMicMuted(muted) {
    micMuted = !!muted;
    applyMuteUi(btnMicMute, micMuted, '麦克风', '🎤', '🚫🎤');
    if (micMuted) {
      // 徽标改为"已静音"（保留 busy 视觉提示，但 micBusy 信号置 false 避免 avatar 进入 listening）
      setStatus(micStatus, '麦克风：已静音', 'badge-busy');
      addMessage('system', '🎤 麦克风已静音 —— 服务端不再收到你的声音');
    } else {
      // 取消静音：徽标回到"已开启"；下一帧 worklet 会再把它改成"收音中"
      setStatus(micStatus, '麦克风：已开启', 'badge-online');
      addMessage('system', '🎤 麦克风已恢复收音');
    }
  }

  function setSpeakerMuted(muted) {
    speakerMuted = !!muted;
    applyMuteUi(btnSpeakerMute, speakerMuted, '扬声器', '🔊', '🔇');
    if (speakerMuted) {
      // 立即清空播放队列（不打断 WS —— TTS chunk 仍照常接收，下次取消静音可恢复）
      stopTtsPlayback();
      setStatus(speakerStatus, '扬声器：已静音', 'badge-busy');
      addMessage('system', '🔇 扬声器已静音 —— TTS 音频不再播放');
    } else {
      setStatus(speakerStatus, '扬声器：空闲', 'badge-online');
      addMessage('system', '🔊 扬声器已恢复');
    }
  }

  if (btnMicMute) btnMicMute.onclick = () => setMicMuted(!micMuted);
  if (btnSpeakerMute) btnSpeakerMute.onclick = () => setSpeakerMuted(!speakerMuted);
  // 初始 UI
  applyMuteUi(btnMicMute, false, '麦克风', '🎤', '🚫🎤');
  applyMuteUi(btnSpeakerMute, false, '扬声器', '🔊', '🔇');

  // ====== 豆包式 avatar 状态机 ======
  const avatarWrap = $('phone-avatar-wrap');
  const phoneStatus = $('agent-phase');
  const phoneSubstatus = $('agent-substatus');
  const phoneVoiceName = $('phone-voice-name');
  // 语音管道状态：idle / listening / thinking / speaking
  let phoneState = 'idle';
  let activeAgentPhase = null;
  let safeErrorShown = false;
  // 上一帧各路输入信号；状态机按"speaking > listening > thinking > idle"优先级合成
  let signals = { wsOpen: false, micBusy: false, speakerBusy: false, thinkingUntil: 0 };
  let stateTimer = null;

  function setPhoneState(next, opts = {}) {
    // 注意：state 相同时也要更新文案 —— idle→idle 也常常要切"未连接 / 对话进行中"。
    // 只有 avatar data-state 属性在相同时跳过（避免无谓的 CSS 重新评估）。
    const stateChanged = phoneState !== next;
    phoneState = next;
    if (avatarWrap && stateChanged) avatarWrap.dataset.state = next;
    if (opts.status !== undefined) phoneStatus.textContent = opts.status;
    if (opts.substatus !== undefined) phoneSubstatus.textContent = opts.substatus;
  }

  // 重新合成 avatar 状态：speaking 优先 > listening > thinking(短窗) > idle
  function recomputePhoneState() {
    const now = performance.now();
    if (activeAgentPhase) {
      const copy = agentUi.PHASE_COPY[activeAgentPhase] || {
        label: agentUi.agentStatusLabel(activeAgentPhase),
        substatus: '',
        state: 'thinking',
      };
      setPhoneState(copy.state, { status: copy.label, substatus: copy.substatus });
      return;
    }
    if (!signals.wsOpen) {
      setPhoneState('idle', { status: '未连接', substatus: '点下方按钮开始对话' });
      return;
    }
    if (signals.speakerBusy) {
      setPhoneState('speaking', { status: '正在说话...', substatus: '' });
      return;
    }
    if (signals.micBusy) {
      setPhoneState('listening', { status: '正在聆听...', substatus: '' });
      return;
    }
    if (signals.thinkingUntil > now) {
      const left = ((signals.thinkingUntil - now) / 1000).toFixed(1);
      setPhoneState('thinking', { status: '正在思考...', substatus: `${left}s` });
      return;
    }
    setPhoneState('idle', { status: '对话进行中', substatus: '等你开口' });
  }

  function setAgentPhase(phase) {
    activeAgentPhase = phase || null;
    if (phase !== 'error') safeErrorShown = false;
    if (retryAction) {
      retryAction.hidden = phase !== 'error';
      retryAction.disabled = phase !== 'error' || !canRetryLastRequest ||
        !ws || ws.readyState !== WebSocket.OPEN;
    }
    recomputePhoneState();
  }

  function clearAgentPhase() {
    activeAgentPhase = null;
    safeErrorShown = false;
    if (retryAction) retryAction.hidden = true;
    recomputePhoneState();
  }

  // 包装 setStatus：在更新状态徽标同时驱动 avatar 状态机
  function setPhoneStatus(el, text, klass) {
    setStatus(el, text, klass);
    const t = el.textContent;
    if (el === speakerStatus) {
      signals.speakerBusy = (klass === 'badge-busy');
      recomputePhoneState();
    } else if (el === micStatus) {
      // mic 实际忙 = 文本里包含"收音中"
      signals.micBusy = /收音中/.test(t);
      recomputePhoneState();
    } else if (el === connStatus) {
      signals.wsOpen = (klass === 'badge-online');
      recomputePhoneState();
    }
  }

  // 收到 asr_final → 开一个 1.5s 的"思考"窗口，期间 avatar 走 thinking 状态
  function bumpThinkingWindow() {
    signals.thinkingUntil = performance.now() + 1500;
    if (stateTimer) clearTimeout(stateTimer);
    stateTimer = setTimeout(() => {
      signals.thinkingUntil = 0;
      recomputePhoneState();
    }, 1600);
    recomputePhoneState();
  }

  // ====== Tab 切换 ======
  // 不影响现有 WS pipeline 状态；切换 tab 只显示/隐藏对应 section
  const TABS = ['pipeline', 'asr', 'llm', 'tts', 'llm_tts', 'asr_llm_tts'];
  let appMode = 'user';
  let developerTab = 'pipeline';
  function activateTab(name) {
    if (!TABS.includes(name)) name = 'pipeline';
    if (appMode === 'developer') developerTab = name;
    document.querySelectorAll('.tab-btn').forEach((b) => {
      b.classList.toggle('active', b.dataset.tab === name);
    });
    TABS.forEach((t) => {
      const el = document.getElementById('tab-' + t);
      if (el) el.hidden = (t !== name);
    });
    if (appMode === 'developer') {
      try { localStorage.setItem('voice-ai.activeTab', name); } catch (_) {}
    }
  }
  document.querySelectorAll('.tab-btn').forEach((b) => {
    b.onclick = () => activateTab(b.dataset.tab);
  });
  let initialTab = 'pipeline';
  try {
    const saved = localStorage.getItem('voice-ai.activeTab');
    if (saved && TABS.includes(saved)) initialTab = saved;
  } catch (_) {}
  developerTab = initialTab;

  // ====== 界面模式 ======
  // 仅切换 DOM 可见性和 hash；不触碰 WS、麦克风或调试脚本状态。
  const developerTools = $('developer-tools');
  const devModeLink = $('dev-mode-link');
  const userModeLink = $('user-mode-link');

  function modeFromHash(hash) {
    return String(hash || '').replace(/^#/, '').toLowerCase() === 'developer'
      ? 'developer' : 'user';
  }

  function setMode(nextMode, { updateHash = true } = {}) {
    const mode = nextMode === 'developer' ? 'developer' : 'user';
    appMode = mode;
    document.body.dataset.mode = mode;
    if (developerTools) developerTools.hidden = mode !== 'developer';
    if (devModeLink) devModeLink.hidden = mode === 'developer';
    if (userModeLink) userModeLink.hidden = mode !== 'developer';
    if (mode === 'developer') activateTab(developerTab);
    else activateTab('pipeline');
    if (updateHash) {
      const desiredHash = mode === 'developer' ? '#developer' : '#user';
      if (window.location.hash !== desiredHash) window.history.replaceState(null, '', desiredHash);
    }
  }

  window.VoiceAppMode = { modeFromHash, setMode };
  if (devModeLink) devModeLink.onclick = (event) => {
    event.preventDefault();
    setMode('developer');
  };
  if (userModeLink) userModeLink.onclick = (event) => {
    event.preventDefault();
    setMode('user');
  };
  window.addEventListener('hashchange', () => setMode(modeFromHash(window.location.hash), { updateHash: false }));
  setMode(modeFromHash(window.location.hash), { updateHash: false });

  // ====== 状态 ======
  let ws = null;
  let audioCtx = null;
  let micStream = null;
  let workletNode = null;
  let micNode = null;
  let isRecording = false;
  let seq = 0;
  // 每次 Start 重新生成，url 路径 / WS 路由 / 服务端 session_id 三处统一
  function newSessionId() {
    return 'web-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8);
  }
  let sessionId = newSessionId();

  // 对话日志相关
  let currentUserBubble = null;
  let currentAssistantBubble = null;
  let lastAsrText = '';

  // AudioWorklet 通信
  let vadCountdown = Infinity; // 离 is_last 还有多少帧
  let startedAtMs = null;
  let lastAsrStartMs = null;
  let lastTtsFirstByteMs = null;
  let sendingUtterance = false;
  let utteranceMessageId = null;
  let utteranceComplete = false;

  // ====== MessagePack 编解码（native，不用 msgpack-lite）======
  // 浏览器版 msgpack-lite（Buffer polyfill）行为不一致，干脆自己写 ~110 行 encoder/decoder
  // 支持 map / string / int / bool / null / array / float64 / bin (0xc4/0xc5/0xc6)，零依赖
  // wire format：{ "Indication": { "data": <payload> } }（与 rmp_serde externally tagged 一致）
  // Uint8Array 走 msgpack bin8/16/32（voice-proto 对 AudioChunk.data / TtsAudio.data 用
  // #[serde(with = "serde_bytes")]，rmp_serde 编出来就是 bin）

  const _encoder = new TextEncoder();
  const _decoder = new TextDecoder();

  function msgpackEncode(value) {
    const chunks = [];
    _write(value);
    let total = 0;
    for (const c of chunks) total += c.byteLength;
    const out = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) { out.set(c, off); off += c.byteLength; }
    return out;

    function _write(v) {
      if (v === null || v === undefined) {
        chunks.push(new Uint8Array([0xc0]));
      } else if (typeof v === 'boolean') {
        chunks.push(new Uint8Array([v ? 0xc3 : 0xc2]));
      } else if (typeof v === 'number') {
        if (Number.isInteger(v)) {
          if (v >= 0 && v <= 0x7f) chunks.push(new Uint8Array([v]));
          else if (v >= -32 && v < 0) chunks.push(new Uint8Array([0xe0 + (v + 32)]));
          else if (v >= 0 && v <= 0xff) chunks.push(new Uint8Array([0xcc, v]));
          else if (v >= -128 && v < 0) chunks.push(new Uint8Array([0xd0, v + 256]));
          else if (v >= 0 && v <= 0xffff) {
            const u = new Uint8Array(3); u[0] = 0xcd;
            new DataView(u.buffer).setUint16(1, v, false);
            chunks.push(u);
          } else if (v >= -32768 && v < 0) {
            const u = new Uint8Array(3); u[0] = 0xd1;
            new DataView(u.buffer).setUint16(1, v + 65536, false);
            chunks.push(u);
          } else {
            const u = new Uint8Array(5); u[0] = 0xd2;
            new DataView(u.buffer).setInt32(1, v, false);
            chunks.push(u);
          }
        } else {
          const u = new Uint8Array(9); u[0] = 0xcb;
          new DataView(u.buffer).setFloat64(1, v, false);
          chunks.push(u);
        }
      } else if (typeof v === 'string') {
        const bytes = _encoder.encode(v);
        if (bytes.length < 32) chunks.push(new Uint8Array([0xa0 + bytes.length]));
        else if (bytes.length < 256) chunks.push(new Uint8Array([0xd9, bytes.length]));
        else if (bytes.length < 0xffff) {
          const u = new Uint8Array(3); u[0] = 0xda;
          new DataView(u.buffer).setUint16(1, bytes.length, false);
          chunks.push(u);
        } else {
          const u = new Uint8Array(5); u[0] = 0xdb;
          new DataView(u.buffer).setUint32(1, bytes.length, false);
          chunks.push(u);
        }
        chunks.push(bytes);
      } else if (Array.isArray(v)) {
        if (v.length < 16) chunks.push(new Uint8Array([0x90 + v.length]));
        else {
          const u = new Uint8Array(3); u[0] = 0xdc;
          new DataView(u.buffer).setUint16(1, v.length, false);
          chunks.push(u);
        }
        for (const item of v) _write(item);
      } else if (v instanceof Uint8Array) {
        // msgpack bin（服务端 voice-proto 用 #[serde(with="serde_bytes")] 走 bin 编码）
        const len = v.length;
        if (len < 256) {
          const u = new Uint8Array(2 + len);
          u[0] = 0xc4; u[1] = len;
          u.set(v, 2);
          chunks.push(u);
        } else if (len < 0x10000) {
          const u = new Uint8Array(3 + len);
          u[0] = 0xc5;
          new DataView(u.buffer).setUint16(1, len, false);
          u.set(v, 3);
          chunks.push(u);
        } else {
          const u = new Uint8Array(5 + len);
          u[0] = 0xc6;
          new DataView(u.buffer).setUint32(1, len, false);
          u.set(v, 5);
          chunks.push(u);
        }
      } else if (typeof v === 'object') {
        const keys = Object.keys(v);
        if (keys.length < 16) chunks.push(new Uint8Array([0x80 + keys.length]));
        else {
          const u = new Uint8Array(3); u[0] = 0xde;
          new DataView(u.buffer).setUint16(1, keys.length, false);
          chunks.push(u);
        }
        for (const k of keys) {
          _write(k);
          _write(v[k]);
        }
      } else {
        throw new Error('msgpack: unsupported type ' + typeof v);
      }
    }
  }

  function msgpackDecode(bytes) {
    let pos = 0;
    function _read() {
      const b = bytes[pos++];
      if (b <= 0x7f) return b;
      if (b >= 0xe0) return b - 0x100;
      if (b >= 0xa0 && b <= 0xbf) {
        const len = b - 0xa0;
        const s = _decoder.decode(bytes.slice(pos, pos + len));
        pos += len;
        return s;
      }
      if (b >= 0x90 && b <= 0x9f) {
        const len = b - 0x90;
        const arr = [];
        for (let i = 0; i < len; i++) arr.push(_read());
        return arr;
      }
      if (b >= 0x80 && b <= 0x8f) {
        const len = b - 0x80;
        const obj = {};
        for (let i = 0; i < len; i++) {
          const key = _read();
          obj[key] = _read();
        }
        return obj;
      }
      if (b === 0xc0) return null;
      if (b === 0xc2) return false;
      if (b === 0xc3) return true;
      if (b === 0xcc) return bytes[pos++];
      if (b === 0xd0) return bytes[pos++] - 256;
      if (b === 0xcd) {
        const v = (bytes[pos] << 8) | bytes[pos + 1];
        pos += 2;
        return v;
      }
      if (b === 0xd1) {
        const v = (bytes[pos] << 8) | bytes[pos + 1];
        pos += 2;
        return v - 65536;
      }
      if (b === 0xd2) {
        const v = new DataView(bytes.buffer, bytes.byteOffset + pos, 4).getInt32(0, false);
        pos += 4;
        return v;
      }
      if (b === 0xd9) {
        const len = bytes[pos++];
        const s = _decoder.decode(bytes.slice(pos, pos + len));
        pos += len;
        return s;
      }
      if (b === 0xda) {
        const len = (bytes[pos] << 8) | bytes[pos + 1];
        pos += 2;
        const s = _decoder.decode(bytes.slice(pos, pos + len));
        pos += len;
        return s;
      }
      if (b === 0xc4) {
        const len = bytes[pos++];
        const out = bytes.slice(pos, pos + len);
        pos += len;
        return out;
      }
      if (b === 0xc5) {
        const len = (bytes[pos] << 8) | bytes[pos + 1];
        pos += 2;
        const out = bytes.slice(pos, pos + len);
        pos += len;
        return out;
      }
      if (b === 0xc6) {
        const len = new DataView(bytes.buffer, bytes.byteOffset + pos, 4).getUint32(0, false);
        pos += 4;
        const out = bytes.slice(pos, pos + len);
        pos += len;
        return out;
      }
      throw new Error('msgpack: unsupported byte 0x' + b.toString(16));
    }
    return _read();
  }

  function encodeIndication(payload) {
    return msgpackEncode({ Indication: { data: { ...payload, message_id: payload.message_id || createTraceId() } } });
  }

  function decodeMessage(bytes) {
    const u8 = bytes instanceof ArrayBuffer ? new Uint8Array(bytes) : bytes;
    const obj = msgpackDecode(u8);
    if (!obj || typeof obj !== 'object') return null;
    if ('Indication' in obj && obj.Indication && 'data' in obj.Indication) {
      return obj.Indication.data;
    }
    if (('ClientCommand' in obj || 'ServerCommand' in obj) &&
        (obj.ClientCommand || obj.ServerCommand) &&
        'command' in (obj.ClientCommand || obj.ServerCommand)) {
      return (obj.ClientCommand || obj.ServerCommand).command;
    }
    return null;
  }

  // ====== UI 辅助 ======
  // setStatus 同时驱动 avatar 状态机（仅对 connStatus / micStatus / speakerStatus 生效）。
  // busy 信号按**文本**判断，不按 class —— 这样"已静音"等非 busy 视觉态不会误触发
  // signals.*Busy，avatar 状态机不会被误导。
  function setStatus(el, text, klass) {
    el.textContent = text;
    el.className = 'badge ' + klass;
    if (el === speakerStatus) {
      signals.speakerBusy = /播放中/.test(text);
    } else if (el === micStatus) {
      signals.micBusy = /收音中/.test(text);
    } else if (el === connStatus) {
      signals.wsOpen = (klass === 'badge-online');
    }
    if (el === connStatus || el === micStatus || el === speakerStatus) {
      recomputePhoneState();
    }
  }

  function addMessage(role, text, opts = {}) {
    if (convLog.querySelector('.empty-hint')) convLog.innerHTML = '';
    const div = document.createElement('div');
    div.className = 'msg ' + role + (opts.partial ? ' partial' : '');
    const roleLabel = ({ user: '👤 你', assistant: '🤖 助手', system: '⚙️ 系统', error: '❌ 错误' })[role] || role;
    div.innerHTML = `<div class="role">${roleLabel}</div><div class="text"></div>`;
    div.querySelector('.text').textContent = text;
    convLog.appendChild(div);
    convLog.scrollTop = convLog.scrollHeight;
    return div;
  }

  function appendToMessage(div, chunk) {
    if (!div) return;
    const t = div.querySelector('.text');
    t.textContent += chunk;
    convLog.scrollTop = convLog.scrollHeight;
  }

  function replaceMessageText(div, text) {
    if (!div) return;
    div.querySelector('.text').textContent = text;
    convLog.scrollTop = convLog.scrollHeight;
  }

  function finalizeMessage(div) {
    if (!div) return;
    div.classList.remove('partial');
  }

  // 来源只渲染经过白名单筛选的纯文本字段，预留给后续来源事件接入。
  function renderSources(sources) {
    if (!sourceList || !sourceDetails) return;
    sourceList.replaceChildren();
    const safeSources = Array.isArray(sources) ? sources : [];
    safeSources.forEach((source) => {
      if (!source || typeof source !== 'object') return;
      const item = document.createElement('li');
      const title = document.createElement('div');
      title.className = 'source-title';
      title.textContent = String(source.title || '未命名来源');
      item.appendChild(title);

      const metaParts = [];
      if (source.publisher) metaParts.push(String(source.publisher));
      if (source.updated_at) metaParts.push(`更新于 ${String(source.updated_at)}`);
      if (metaParts.length > 0) {
        const meta = document.createElement('div');
        meta.className = 'source-meta';
        meta.textContent = metaParts.join(' · ');
        item.appendChild(meta);
      }
      sourceList.appendChild(item);
    });
    sourceDetails.hidden = sourceList.childElementCount === 0;
  }
  window.renderAgentSources = renderSources;

  let progressUtterance = null;
  let progressSpokenForMessageId = null;

  function stopProgressSpeech() {
    if (!progressUtterance || !('speechSynthesis' in window)) return;
    window.speechSynthesis.cancel();
    progressUtterance = null;
  }

  function maybeSpeakProgress(payload) {
    const messageId = String(payload.message_id || '');
    if (payload.phase !== 'searching' || payload.speak_progress !== true ||
        !messageId || progressSpokenForMessageId === messageId ||
        !('speechSynthesis' in window) || !('SpeechSynthesisUtterance' in window)) {
      return;
    }
    progressSpokenForMessageId = messageId;
    progressUtterance = new window.SpeechSynthesisUtterance('我查一下');
    progressUtterance.lang = 'zh-CN';
    progressUtterance.onend = () => { progressUtterance = null; };
    progressUtterance.onerror = () => { progressUtterance = null; };
    window.speechSynthesis.speak(progressUtterance);
  }

  function prepareForNewRequest() {
    activeAgentPhase = null;
    safeErrorShown = false;
    if (retryAction) retryAction.hidden = true;
    if (currentAssistantBubble) finalizeMessage(currentAssistantBubble);
    currentAssistantBubble = null;
    renderSources([]);
  }

  function handleAgentStatus(payload) {
    if (!acceptsPipelineMessage(payload)) return;
    setAgentPhase(payload.phase);
    maybeSpeakProgress(payload);
    if (payload.done && payload.phase !== 'error') setAgentPhase('listening');
  }

  function showSafeError() {
    setAgentPhase('error');
    if (!safeErrorShown) {
      safeErrorShown = true;
      addMessage('error', '服务暂时不可用，请稍后再试。');
    }
  }

  function beginLocalUtterance() {
    activeAgentPhase = null;
    safeErrorShown = false;
    if (currentAssistantBubble) finalizeMessage(currentAssistantBubble);
    currentAssistantBubble = null;
    renderSources([]);
    setAgentPhase('listening');
  }

  function resetAgentSessionUi() {
    currentMessageId = null;
    progressSpokenForMessageId = null;
    activeAgentPhase = null;
    safeErrorShown = false;
    currentUserBubble = null;
    currentAssistantBubble = null;
    sendingUtterance = false;
    utteranceMessageId = null;
    utteranceComplete = false;
    canRetryLastRequest = false;
    renderSources([]);
    if (retryAction) retryAction.hidden = true;
    recomputePhoneState();
  }

  // ====== TTS 播放：PCM chunk 到达即排程 ======
  const ttsPlayer = new window.PcmStreamPlayer({
    onState: (playing) => {
      setStatus(speakerStatus, playing ? '扬声器：播放中' : '扬声器：空闲', playing ? 'badge-busy' : 'badge-online');
    },
  });
  const ttsFirstAudioReceivedAt = new Map();
  let ttsPlaybackReportedMessageId = null;

  function playTtsAudio(dataBytes, _isLast, sampleRate = SAMPLE_RATE, channels = CHANNELS) {
    // 扬声器静音：直接丢掉这段 TTS 音频（不进队列，不消耗 audio 资源）
    if (speakerMuted) return undefined;
    return ttsPlayer.enqueue(dataBytes, sampleRate, channels);
  }

  function reportPlaybackStarted(messageId, playbackStartedAt) {
    messageId = String(messageId || '');
    if (!messageId || ttsPlaybackReportedMessageId === messageId || !Number.isFinite(playbackStartedAt)) return;
    const receivedAt = ttsFirstAudioReceivedAt.get(messageId);
    if (receivedAt === undefined) return;
    const report = agentUi.createClientMetricReport({
      sessionId,
      messageId,
      metric: 'first_audio_received_to_playback',
      startedAt: receivedAt,
      endedAt: playbackStartedAt,
    });
    if (!report || !sendWsPayload(report)) return;
    ttsPlaybackReportedMessageId = messageId;
    ttsFirstAudioReceivedAt.delete(messageId);
  }

  function stopTtsPlayback() {
    stopProgressSpeech();
    ttsPlayer.stop();
    ttsFirstAudioReceivedAt.clear();
    ttsPlaybackReportedMessageId = null;
  }

  function acceptAsrMessage(payload) {
    const text = String(payload.text || '');
    const messageId = String(payload.message_id || '');
    if (!text.trim() || !messageId) return null;
    if (utteranceMessageId && messageId !== utteranceMessageId) return null;
    const shouldStop = agentUi.shouldStopPlaybackForAsr(text, messageId, currentMessageId);
    const changed = currentMessageId !== messageId;
    currentMessageId = messageId;
    utteranceMessageId = null;
    utteranceComplete = false;
    return shouldStop && changed;
  }

  function acceptsPipelineMessage(payload) {
    const messageId = String(payload.message_id || '');
    return Boolean(currentMessageId && messageId === currentMessageId);
  }

  // ====== WebSocket ======
  function sendWsPayload(payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    const message = { ...payload, message_id: payload.message_id || createTraceId() };
    const bytes = encodeIndication(message);
    console.info('[voice-ws] send', {
      sessionId,
      type: message.type,
      messageId: message.message_id,
      bytes: bytes.byteLength,
    });
    return agentUi.trySendWebSocket(ws, bytes);
  }

  function connect() {
    return new Promise((resolve, reject) => {
      ws = new WebSocket(buildWsUrl(sessionId));
      ws.binaryType = 'arraybuffer';

      ws.onopen = () => {
        setStatus(connStatus, 'WS：已连接', 'badge-online');
        addMessage('system', '已连接服务端');
        // 发 SessionStart；voice 取自当前下拉框（None = 走服务端配置兜底）
        const voice = window.VoiceSelector.getSelected('pipeline-voice');
        const startPayload = {
          type: 'session_start',
          session_id: sessionId,
          sample_rate: SAMPLE_RATE,
          channels: CHANNELS,
          codec: 'pcm_s16le',
          language: 'zh-CN',
        };
        if (voice) startPayload.voice = voice;
        sendWsPayload(startPayload);
        console.log(`[发送] SessionStart session_id=${sessionId} sample_rate=${SAMPLE_RATE} channels=${CHANNELS} codec=pcm_s16le language=zh-CN voice=${voice || '(default)'}`);
        addMessage('system', '已发送 SessionStart');
        resolve();
      };

      ws.onerror = (e) => {
        setStatus(connStatus, 'WS：连接失败', 'badge-offline');
        const detail = e && e.message ? e.message :
                       (e && e.reason ? `reason=${e.reason}` :
                        'unknown（请检查 URL 路径、token、跨域）');
        console.error('WebSocket 连接失败:', detail);
        showSafeError();
        reject(new Error(detail));
      };

      ws.onclose = () => {
        setStatus(connStatus, 'WS：断开', 'badge-offline');
        addMessage('system', 'WS 已断开');
        ttsFirstAudioReceivedAt.clear();
        ttsPlaybackReportedMessageId = null;
        currentMessageId = null;
        utteranceMessageId = null;
        utteranceComplete = false;
        clearAgentPhase();
        stopMic();
      };

      ws.onmessage = (ev) => {
        try {
          const bytes = new Uint8Array(ev.data);
          const payload = decodeMessage(bytes);
          if (!payload || !payload.type) return;
          console.info('[voice-ws] receive', {
            sessionId,
            type: payload.type,
            messageId: payload.message_id,
            bytes: bytes.byteLength,
          });

          switch (payload.type) {
            case 'asr_partial':
              {
                const shouldStop = acceptAsrMessage(payload);
                if (shouldStop === null) break;
                if (shouldStop) stopTtsPlayback();
              }
              if (!currentUserBubble) {
                currentUserBubble = addMessage('user', '', { partial: true });
              }
              replaceMessageText(currentUserBubble, payload.text);
              lastAsrText = payload.text;
              if (payload.is_final) {
                finalizeMessage(currentUserBubble);
                currentUserBubble = null;
                lastAsrStartMs = performance.now();
                // avatar 进入 thinking 状态：等 LLM 首包 + TTS 首字节的最长 1.5s
                bumpThinkingWindow();
              }
              break;
            case 'agent_status':
              handleAgentStatus(payload);
              break;
            case 'llm_delta':
              if (!acceptsPipelineMessage(payload)) break;
              if (!currentAssistantBubble) {
                currentAssistantBubble = addMessage('assistant', '', { partial: true });
              }
              appendToMessage(currentAssistantBubble, payload.delta);
              if (payload.is_final) {
                finalizeMessage(currentAssistantBubble);
                currentAssistantBubble = null;
              }
              break;
            case 'tts_audio':
              if (!acceptsPipelineMessage(payload)) break;
              if (payload.data && payload.data.length > 0) {
                // 下行：服务端用 #[serde(with="serde_bytes")] 走 msgpack bin，JS 解码后已是 Uint8Array
                const messageId = String(payload.message_id || '');
                if (!ttsFirstAudioReceivedAt.has(messageId)) {
                  const receivedAt = performance.now();
                  ttsFirstAudioReceivedAt.set(messageId, receivedAt);
                  window.setTimeout(() => {
                    if (ttsFirstAudioReceivedAt.get(messageId) === receivedAt) ttsFirstAudioReceivedAt.delete(messageId);
                  }, 30000);
                }
                const playbackStartedAt = playTtsAudio(payload.data, payload.is_last, payload.sample_rate || SAMPLE_RATE, payload.channels || CHANNELS);
                if (!lastTtsFirstByteMs) lastTtsFirstByteMs = performance.now();
                if (playbackStartedAt !== undefined) reportPlaybackStarted(messageId, playbackStartedAt);
              }
              if (payload.is_last) {
                lastTtsFirstByteMs = null;
              }
              break;
            case 'error':
              console.error(`服务端错误 [${payload.code}]`, payload.message);
              if (!payload.message_id || acceptsPipelineMessage(payload)) {
                showSafeError();
              }
              break;
            case 'interrupt':
              // 服务端主动中断（来自另一端 Interrupt payload push），忽略
              break;
            default:
              console.log('未处理 payload.type:', payload.type);
          }
        } catch (e) {
          console.error('onmessage 解析失败:', e);
          showSafeError();
        }
      };
    });
  }

  function sendInterrupt() {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    sendWsPayload({
      type: 'interrupt',
      session_id: sessionId,
    });
    sendingUtterance = false;
    startedAtMs = null;
    utteranceMessageId = null;
    utteranceComplete = false;
    currentMessageId = null;
    console.log(`[发送] Interrupt session_id=${sessionId}`);
    addMessage('system', '已发送 Interrupt');
    // 立即停止正在播放的 TTS，并清空队列里未播的 chunk
    stopTtsPlayback();
    clearAgentPhase();
    setAgentPhase('listening');
  }

  function sendSessionEnd(reason) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    sendWsPayload({
      type: 'session_end',
      session_id: sessionId,
      reason: reason || 'normal exit',
    });
    console.log(`[发送] SessionEnd session_id=${sessionId} reason=${reason || 'normal exit'}`);
  }

  // ====== AudioWorklet 代码（内联 Blob 上传）======
  // 职责：
  //   1) 按 FRAME_SAMPLES（320 样本 = 20ms）攒帧，攒满一帧才 postMessage；
  //      不按 render quantum（128 样本）发——那是 2.6ms 的小帧，协议开销比音频还大
  //   2) 重采样：优先用 AudioContext({ sampleRate: 16000 }) 让浏览器自带抗混叠降采样，
  //      ratio == 1 时直通；浏览器不支持该采样率时退化为箱式滤波器抽取，
  //      用分数相位累加器跨 quantum 续推，不丢样本
  //   3) RMS 能量 VAD（按 20ms 帧判定，与 voice-client/src/vad.rs 的 EnergyVad 语义对齐）：
  //      - Idle 阶段不发帧，攒 PREROLL_FRAMES 前置静音；起句（VAD_START_FRAMES 去抖）
  //        时把前置帧一起发出，避免吞掉字头
  //      - 静音满 VAD_END_FRAMES 只在"边沿"那一帧标一次 is_last=true，之后回 Idle 不再发帧
  //        （旧实现把 is_last 当电平用，静音期每帧都标 true，导致服务端每帧起一次 pipeline）
  const WORKLET_SRC = `
    class PcmCaptureProcessor extends AudioWorkletProcessor {
      constructor(options) {
        super(options);
        const o = (options && options.processorOptions) || {};
        this._targetRate = o.targetRate || 16000;
        this._frameSamples = o.frameSamples || 320;
        this._threshold = o.threshold != null ? o.threshold : 0.01;
        this._startFrames = o.startFrames || 3;
        this._endFrames = o.endFrames || 30;
        this._maxFrames = o.maxFrames || 0;
        this._prerollCap = o.prerollFrames || 5;
        // sampleRate 是 worklet 全局量 = AudioContext 实际采样率
        this._ratio = sampleRate / this._targetRate;
        this._enabled = false;
        this._seq = 0;
        // 帧累积：每攒满 _frameSamples 个输出样本封一帧
        this._frame = new Int16Array(this._frameSamples);
        this._frameLen = 0;
        this._frameSq = 0;
        // 重采样相位（分数累加器，跨 process() 续推）
        this._phase = 0;
        this._acc = 0;
        this._accN = 0;
        // VAD 状态（对齐 voice-client/src/vad.rs：Phase::Idle/Speaking）
        this._phase = 0; // 0=Idle 1=Speaking
        this._voicedRun = 0;
        this._silentRun = 0;
        this._framesInSpeech = 0;
        this._preroll = []; // {bytes, rms}
        this.port.onmessage = (e) => {
          if (e.data.cmd === 'start') this._enabled = true;
          if (e.data.cmd === 'stop') this._enabled = false;
        };
      }
      process(inputs) {
        if (!this._enabled) return true;
        const input = inputs[0];
        if (!input || input.length === 0) return true;
        const ch0 = input[0];
        // 降采样到 _targetRate：ratio==1 直通；否则箱式滤波器（抗混叠）+ 分数相位，不丢样本
        for (let i = 0; i < ch0.length; i++) {
          this._acc += ch0[i];
          this._accN++;
          this._phase += this._ratio;
          if (this._phase >= 1) {
            this._phase -= 1;
            this._pushSample(this._accN > 0 ? this._acc / this._accN : 0);
            this._acc = 0;
            this._accN = 0;
          }
        }
        return true;
      }
      _pushSample(f) {
        if (f > 1) f = 1; else if (f < -1) f = -1;
        this._frame[this._frameLen++] = Math.round(f < 0 ? f * 0x8000 : f * 0x7fff);
        this._frameSq += f * f;
        if (this._frameLen < this._frameSamples) return;
        const rms = Math.sqrt(this._frameSq / this._frameSamples);
        const bytes = new Uint8Array(this._frame.buffer.slice(0));
        this._frameLen = 0;
        this._frameSq = 0;
        this._vad(rms, bytes);
      }
      _send(bytes, rms, isLast) {
        this._seq++;
        this.port.postMessage({ type: 'audio', seq: this._seq, bytes, isLast, rms });
      }
      _vad(rms, bytes) {
        const voiced = rms >= this._threshold;
        if (this._phase === 0) { // Idle：先攒前置帧（含起句去抖段），确认 SpeechStart 再批量发出
          this._preroll.push({ bytes, rms });
          while (this._preroll.length > this._prerollCap + this._startFrames) this._preroll.shift();
          if (voiced) {
            this._voicedRun++;
            if (this._voicedRun >= this._startFrames) {
              this._phase = 1;
              this._framesInSpeech = this._voicedRun; // 去抖段算进句长
              this._voicedRun = 0;
              this._silentRun = 0;
              for (const p of this._preroll) this._send(p.bytes, p.rms, false);
              this._preroll = [];
              // 触发帧本身已经在上面 _send 过了，不用再发
            }
          } else {
            this._voicedRun = 0;
          }
          return;
        }
        // Speaking
        this._framesInSpeech++;
        this._silentRun = voiced ? 0 : this._silentRun + 1;
        const timedOut = this._maxFrames > 0 && this._framesInSpeech >= this._maxFrames;
        if (this._silentRun >= this._endFrames || timedOut) {
          this._send(bytes, rms, true); // 边沿：只在 SpeechEnd 这一帧标一次
          this._phase = 0;
          this._voicedRun = 0;
          this._silentRun = 0;
          this._framesInSpeech = 0;
          this._preroll = [];
        } else {
          this._send(bytes, rms, false);
        }
      }
    }
    registerProcessor('pcm-capture', PcmCaptureProcessor);
  `;

  // ====== 启动麦克风 ======
  async function startMic() {
    try {
      micStream = await navigator.mediaDevices.getUserMedia({
        audio: {
          channelCount: 1,
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        },
      });
    } catch (e) {
      console.error('无法访问麦克风:', e);
      addMessage('error', '暂时无法访问麦克风，请检查浏览器权限后重试。');
      throw e;
    }

    // 优先按目标采样率建 AudioContext：浏览器自带抗混叠重采样，worklet 里就不用
    // 手写抽取了；不支持（或实际采样率与请求不符）就退回默认，worklet 内部做箱式降采样
    try {
      audioCtx = new AudioContext({ sampleRate: SAMPLE_RATE });
    } catch (_) {
      audioCtx = new AudioContext();
      console.warn(`[mic] 不支持 AudioContext({sampleRate: ${SAMPLE_RATE}})，用默认 ${audioCtx.sampleRate}Hz，worklet 内降采样`);
    }
    // 上传 worklet
    const workletURL = URL.createObjectURL(new Blob([WORKLET_SRC], { type: 'application/javascript' }));
    await audioCtx.audioWorklet.addModule(workletURL);

    micNode = audioCtx.createMediaStreamSource(micStream);
    workletNode = new AudioWorkletNode(audioCtx, 'pcm-capture', {
      processorOptions: {
        targetRate: SAMPLE_RATE,
        frameSamples: FRAME_SAMPLES,
        threshold: SILENCE_THRESHOLD,
        startFrames: VAD_START_FRAMES,
        endFrames: VAD_END_FRAMES,
        maxFrames: VAD_MAX_FRAMES,
        prerollFrames: PREROLL_FRAMES,
      },
    });

    workletNode.port.onmessage = (e) => {
      if (e.data.type !== 'audio') return;
      const { seq, bytes, isLast, rms } = e.data;
      // 麦克风静音：不发包、不更新状态徽标（避免误导用户以为在收音）
      // —— worklet 仍然跑着，恢复时无需重新初始化
      if (micMuted) return;
      if (ws && ws.readyState === WebSocket.OPEN) {
        if (!sendingUtterance) {
          sendingUtterance = true;
          beginLocalUtterance();
          if (!utteranceMessageId || utteranceComplete) utteranceMessageId = createTraceId();
          utteranceComplete = false;
        }
        // timestamp_ms = 本句内时间（isLast 后归零，下一句重新计时）
        if (startedAtMs === null) startedAtMs = Date.now();
        const inputEndedAt = isLast ? performance.now() : null;
        const payload = {
          type: 'audio_chunk',
          session_id: sessionId,
          seq: seq,
          timestamp_ms: Date.now() - startedAtMs,
          data: bytes,
          is_last: isLast,
          message_id: utteranceMessageId,
        };
        const sent = sendWsPayload(payload);
        if (isLast && sent) {
          const report = agentUi.createClientMetricReport({
            sessionId,
            messageId: utteranceMessageId,
            metric: 'input_end_to_final_audio_sent',
            startedAt: inputEndedAt,
            endedAt: performance.now(),
          });
          if (report) sendWsPayload(report);
        }
        // 与服务端"收到 AudioChunk"日志逐字段对齐，便于两端对比
        console.log(`[发送] AudioChunk session_id=${sessionId} seq=${seq} bytes=${bytes.length} timestamp_ms=${payload.timestamp_ms} is_last=${isLast}`);
        if (isLast) {
          sendingUtterance = false;
          utteranceComplete = true;
          canRetryLastRequest = true;
          startedAtMs = null;
          // 重置首字延迟
          lastAsrStartMs = null;
        }
        // 更新麦克风状态
        setStatus(micStatus, '麦克风：收音中 ' + (rms > 0.05 ? '🔴' : '⚪'), 'badge-busy');
      }
    };

    micNode.connect(workletNode);
    workletNode.port.postMessage({ cmd: 'start' });
    isRecording = true;
    startedAtMs = Date.now();
    setStatus(micStatus, '麦克风：已开启', 'badge-online');
    addMessage('system', '麦克风已开启，可以说话了');
  }

  function stopMic() {
    if (workletNode) {
      try { workletNode.port.postMessage({ cmd: 'stop' }); } catch (_) {}
      workletNode.disconnect();
      workletNode = null;
    }
    if (micNode) {
      try { micNode.disconnect(); } catch (_) {}
      micNode = null;
    }
    if (micStream) {
      micStream.getTracks().forEach(t => t.stop());
      micStream = null;
    }
    if (audioCtx) {
      audioCtx.close().catch(() => {});
      audioCtx = null;
    }
    isRecording = false;
    setStatus(micStatus, '麦克风：关', 'badge-offline');
  }

  // ====== 按钮 ======
  btnStart.onclick = async () => {
    btnStart.disabled = true;
    void ttsPlayer.resume().catch(() => {});
    // 开启新一轮对话：先清掉旧 TTS 队列和正在播的内容，避免残留
    stopTtsPlayback();
    resetAgentSessionUi();
    // 重新生成 sessionId → URL 路径尾段变化 → 服务端 entry() 拿到全新 session
    // 否则 Stop 后再 Start 会撞上旧 session 的 closed=true，所有上行被静默丢弃
    sessionId = newSessionId();
    seq = 0;  // 与新 session 一起重置（worklet 内部也有自己的 _seq，但 main 这边的 payload seq 不再连续）
    try {
      await connect();
      // mic 已经在 onload 启动；若用户拒绝则重试一次
      if (!isRecording) {
        addMessage('system', '麦克风未就绪，重新请求...');
        await startMic();
      }
      btnStop.disabled = false;
      btnInterrupt.disabled = false;
      addMessage('system', '准备就绪。说一句试试。');
    } catch (e) {
      btnStart.disabled = false;
      console.error('启动失败:', e);
      showSafeError();
    }
  };

  btnInterrupt.onclick = () => {
    sendInterrupt();
  };

  retryAction.onclick = () => {
    const sent = agentUi.executeRetry({
      canRetry: canRetryLastRequest,
      socketOpen: !!ws && ws.readyState === WebSocket.OPEN,
      stopPlayback: stopTtsPlayback,
      sendRetry: () => sendWsPayload({
        type: 'retry',
        session_id: sessionId,
      }),
    });
    if (!sent) return;

    canRetryLastRequest = false;
    safeErrorShown = false;
    retryAction.hidden = true;
    retryAction.disabled = true;
    if (currentAssistantBubble) finalizeMessage(currentAssistantBubble);
    currentAssistantBubble = null;
    renderSources([]);
    setAgentPhase('transcribing');
  };

  btnStop.onclick = () => {
    sendSessionEnd('user stopped');
    stopTtsPlayback();
    stopMic();
    if (ws) ws.close();
    btnStart.disabled = false;
    btnStop.disabled = true;
    btnInterrupt.disabled = true;
    currentMessageId = null;
    utteranceMessageId = null;
    utteranceComplete = false;
    clearAgentPhase();
    addMessage('system', '已结束');
  };

  // ====== 启动 ======
  // 页面加载时立即请求麦克风权限（不连 WS，只拿权限）
  async function requestMicOnLoad() {
    setStatus(micStatus, '麦克风：请求权限...', 'badge-busy');
    addMessage('system', '正在请求麦克风权限...');
    try {
      await startMic();
      addMessage('system', '麦克风权限已获得。点"开始对话"连接服务。');
    } catch (e) {
      setStatus(micStatus, '麦克风：拒绝', 'badge-offline');
      console.error('麦克风权限被拒绝:', e);
      addMessage('error', '麦克风权限未开启，请在浏览器中允许后重试。');
    }
  }

  // 立即请求麦克风
  requestMicOnLoad();
})();
