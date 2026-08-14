// voice-app web demo — 浏览器端 voice pipeline
// 流程：
//   1. WS 连接 /ws/voice/web/demo/listener
//   2. 发送 SessionStart (codec=pcm_s16le, sample_rate=16000, channels=1)
//   3. AudioContext + AudioWorklet 持续采集麦克风 → Float32 → 转 s16le → 二进制帧 → WS
//   4. AudioWorklet 内做 RMS 能量 VAD；静音 600ms 自动标 is_last=true
//   5. 接收下行：
//        - AsrPartial    → 在 user bubble 里显示/替换
//        - LlmDelta      → 追加到 assistant bubble
//        - TtsAudio      → 累积 s16le 字节 + WAV 头 → Blob URL → Audio() 播放
//        - Error         → 在 system 区显示

(function () {
  'use strict';

  // ====== 配置 ======
  const SAMPLE_RATE = 16000;
  const CHANNELS = 1;
  const FRAME_SAMPLES = 320; // 20ms @ 16kHz
  const SILENCE_THRESHOLD = 0.01; // RMS 阈值
  const SILENCE_FRAMES_TO_END = 30; // ~600ms 静音后标 is_last
  // URL 路径匹配 webhttp WS 路由 /{api_prefix}/{wsapi}/{business}/{actor}/{connid}
  //   api_prefix = "/"  wsapi = "ws"  →  "/ws/{business}/{actor}/{connid}"
  // 三段分别：business=voice, actor=web, connid=demo
  const WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') +
                 location.host + '/ws/voice/web/demo';

  // ====== 元素 ======
  const $ = (id) => document.getElementById(id);
  const convLog = $('conv-log');
  const btnStart = $('btn-start');
  const btnInterrupt = $('btn-interrupt');
  const btnStop = $('btn-stop');
  const connStatus = $('conn-status');
  const micStatus = $('mic-status');
  const speakerStatus = $('speaker-status');
  const latency = $('latency');

  // ====== 状态 ======
  let ws = null;
  let audioCtx = null;
  let micStream = null;
  let workletNode = null;
  let micNode = null;
  let isRecording = false;
  let seq = 0;
  let sessionId = 'web-' + Date.now();

  // 对话日志相关
  let currentUserBubble = null;
  let currentAssistantBubble = null;
  let lastAsrText = '';

  // TTS 播放
  let ttsChunks = []; // [{seq, data: Uint8Array, isLast}]
  let audioElement = null;

  // AudioWorklet 通信
  let vadCountdown = Infinity; // 离 is_last 还有多少帧
  let startedAtMs = null;
  let lastAsrStartMs = null;
  let lastTtsFirstByteMs = null;

  // ====== MessagePack 编解码（用 msgpack-lite CDN）======
  // voice-proto 的 wire format 是 webproto 信封：
  //   Message<F> 枚举：Indication(Indication<F>) | ClientCommand(ClientCommand<F>) | ServerCommand(ServerCommand<F>)
  // 但浏览器这边简单点：服务端 99% 收到的都是 AudioChunk（上行）和 SessionStart（上行），
  // 收到的是 Indication<VoicePayload>。下行也是 Indication<VoicePayload>。
  // msgpack-lite 的 api：msgpack.encode(obj) → Uint8Array；msgpack.decode(Uint8Array) → obj
  //
  // Indication<VoicePayload> 的结构（简化版）：
  //   { data: <VoicePayload> }
  // VoicePayload 用 serde tag 序列化：{ type: "session_start", session_id: "...", ... }
  //
  // 注意：webproto 用 rmp_serde (Rust MessagePack)，跟 msgpack-lite 的 ext-type 略有差异
  // —— 我们这里只处理业务字段，序列化策略在两边要一致。
  // 这里采用纯 JSON-like 结构 + 简单 msgpack。
  // 但 Rust 端用的是 serde 的 tagged enum + rmp。
  // 幸运的是 rmp_serde 默认就是 serde 兼容的 compact 模式，跟 msgpack-lite 的 basic 模式兼容。
  const MP = window.msgpack;

  function encodeIndication(payload) {
    return MP.encode({ data: payload });
  }

  function decodeMessage(bytes) {
    const obj = MP.decode(bytes);
    // webproto Message<F> 是一个 enum：可能返回 { data: ... } (Indication) 或 { event_id, command } (ClientCommand/ServerCommand)
    // 我们的 VoicePayload 都用 Indication 包装（除了 SessionStart），所以这里主要处理 { data: ... }
    if (obj && typeof obj === 'object' && 'data' in obj) return obj.data;
    if (obj && typeof obj === 'object' && 'command' in obj) return obj.command;
    return null;
  }

  // ====== UI 辅助 ======
  function setStatus(el, text, klass) {
    el.textContent = text;
    el.className = 'badge ' + klass;
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

  // ====== TTS 播放：拼 WAV + Audio() ======
  function playTtsAudio(dataBytes, isLast) {
    // dataBytes 已经是 s16le 16kHz mono PCM
    // 我们已经预生成/缓存了一个 WAV header（s16le, 16kHz, mono, data chunk 长度可先占 0 让我们 streaming）
    // 简化：每个 TtsAudio 都包成独立 WAV 用 Blob URL
    const wav = wrapPcmAsWav(dataBytes);
    const blob = new Blob([wav], { type: 'audio/wav' });
    const url = URL.createObjectURL(blob);
    if (!audioElement) audioElement = new Audio();
    audioElement.src = url;
    audioElement.play().catch(err => console.warn('TTS play failed:', err));
    setStatus(speakerStatus, '扬声器：播放中', 'badge-busy');
    audioElement.onended = () => {
      setStatus(speakerStatus, '扬声器：空闲', 'badge-online');
      URL.revokeObjectURL(url);
    };
  }

  function wrapPcmAsWav(pcmBytes) {
    // WAV header for s16le, 16kHz, mono
    const dataLen = pcmBytes.byteLength;
    const buf = new ArrayBuffer(44 + dataLen);
    const view = new DataView(buf);
    function writeStr(off, s) { for (let i = 0; i < s.length; i++) view.setUint8(off + i, s.charCodeAt(i)); }
    writeStr(0, 'RIFF');
    view.setUint32(4, 36 + dataLen, true);
    writeStr(8, 'WAVE');
    writeStr(12, 'fmt ');
    view.setUint32(16, 16, true);     // fmt chunk size
    view.setUint16(20, 1, true);      // PCM
    view.setUint16(22, CHANNELS, true);
    view.setUint32(24, SAMPLE_RATE, true);
    view.setUint32(28, SAMPLE_RATE * CHANNELS * 2, true); // byte rate
    view.setUint16(32, CHANNELS * 2, true);                // block align
    view.setUint16(34, 16, true);     // bits per sample
    writeStr(36, 'data');
    view.setUint32(40, dataLen, true);
    new Uint8Array(buf, 44).set(new Uint8Array(pcmBytes));
    return buf;
  }

  // ====== WebSocket ======
  function connect() {
    return new Promise((resolve, reject) => {
      ws = new WebSocket(WS_URL);
      ws.binaryType = 'arraybuffer';

      ws.onopen = () => {
        setStatus(connStatus, 'WS：已连接', 'badge-online');
        addMessage('system', '已连接服务端');
        // 发 SessionStart
        const start = encodeIndication({
          type: 'session_start',
          session_id: sessionId,
          sample_rate: SAMPLE_RATE,
          channels: CHANNELS,
          codec: 'pcm_s16le',
          language: 'zh-CN',
        });
        ws.send(start);
        addMessage('system', '已发送 SessionStart');
        resolve();
      };

      ws.onerror = (e) => {
        setStatus(connStatus, 'WS：连接失败', 'badge-offline');
        const detail = e && e.message ? e.message :
                       (e && e.reason ? `reason=${e.reason}` :
                        'unknown（请检查 URL 路径、token、跨域）');
        addMessage('error', 'WebSocket 连接失败：' + detail + '；URL=' + WS_URL);
        reject(new Error(detail));
      };

      ws.onclose = () => {
        setStatus(connStatus, 'WS：断开', 'badge-offline');
        addMessage('system', 'WS 已断开');
        stopMic();
      };

      ws.onmessage = (ev) => {
        try {
          const bytes = new Uint8Array(ev.data);
          const payload = decodeMessage(bytes);
          if (!payload || !payload.type) return;

          switch (payload.type) {
            case 'asr_partial':
              if (!currentUserBubble) {
                currentUserBubble = addMessage('user', '', { partial: true });
              }
              replaceMessageText(currentUserBubble, payload.text);
              lastAsrText = payload.text;
              if (payload.is_final) {
                finalizeMessage(currentUserBubble);
                currentUserBubble = null;
                lastAsrStartMs = performance.now();
              }
              break;
            case 'llm_delta':
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
              if (payload.data && payload.data.length > 0) {
                // 后端用 base64 编码 PCM bytes (因为 rmp serde Vec<u8> 是 bin 类型；
                // 但浏览器 msgpack-lite 不一定能直接 decode Vec<u8>，所以服务端 base64 了一下)
                // 这里 data 是 base64 字符串
                const raw = base64ToBytes(payload.data);
                playTtsAudio(raw, payload.is_last);
                if (!lastTtsFirstByteMs) lastTtsFirstByteMs = performance.now();
              }
              if (payload.is_last) {
                lastTtsFirstByteMs = null;
              }
              break;
            case 'error':
              addMessage('error', `[${payload.code}] ${payload.message}`);
              break;
            case 'interrupt':
              // 服务端主动中断（来自另一端 Interrupt payload push），忽略
              break;
            default:
              console.log('未处理 payload.type:', payload.type);
          }
        } catch (e) {
          console.error('onmessage 解析失败:', e);
          addMessage('error', '解析下行 payload 失败：' + e.message);
        }
      };
    });
  }

  function base64ToBytes(b64) {
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return bytes;
  }

  function sendInterrupt() {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    ws.send(encodeIndication({
      type: 'interrupt',
      session_id: sessionId,
    }));
    addMessage('system', '已发送 Interrupt');
    // 立即停止正在播放的 TTS
    if (audioElement) { audioElement.pause(); audioElement.src = ''; }
    setStatus(speakerStatus, '扬声器：空闲', 'badge-online');
  }

  function sendSessionEnd(reason) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    ws.send(encodeIndication({
      type: 'session_end',
      session_id: sessionId,
      reason: reason || 'normal exit',
    }));
  }

  // ====== AudioWorklet 代码（内联 base64 写文件）======
  // 处理：1) 推送 PCM 帧 2) RMS 能量检测 3) 计算静音时长
  const WORKLET_SRC = `
    class PcmCaptureProcessor extends AudioWorkletProcessor {
      constructor() {
        super();
        this._silenceCount = 0;
        this._threshold = 0.01;
        this._silenceFramesToEnd = 30;
        this._enabled = false;
        this._seq = 0;
        this.port.onmessage = (e) => {
          if (e.data.cmd === 'start') this._enabled = true;
          if (e.data.cmd === 'stop') this._enabled = false;
        };
      }
      process(inputs) {
        if (!this._enabled) return true;
        const input = inputs[0];
        if (!input || input.length === 0) return true;
        // mono
        const ch0 = input[0];
        // RMS
        let sum = 0;
        for (let i = 0; i < ch0.length; i++) sum += ch0[i] * ch0[i];
        const rms = Math.sqrt(sum / ch0.length);
        if (rms > this._threshold) this._silenceCount = 0;
        else this._silenceCount++;
        // 采样率转换：浏览器一般是 48000 Hz；服务器要 16000。
        // 这里简单做法：每 3 个 sample 取 1 个（48000/3=16000）
        const downsampleFactor = Math.round(sampleRate / 16000);
        const targetLen = Math.floor(ch0.length / downsampleFactor);
        const int16 = new Int16Array(targetLen);
        for (let i = 0; i < targetLen; i++) {
          let s = ch0[i * downsampleFactor];
          if (s > 1) s = 1; else if (s < -1) s = -1;
          int16[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
        }
        const bytes = new Uint8Array(int16.buffer);
        const isLast = this._silenceCount >= this._silenceFramesToEnd;
        this._seq++;
        this.port.postMessage({
          type: 'audio',
          seq: this._seq,
          bytes: bytes,
          isLast: isLast,
          rms: rms,
        });
        return true;
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
      addMessage('error', '无法访问麦克风：' + e.message);
      throw e;
    }

    audioCtx = new AudioContext();
    // 上传 worklet
    const workletURL = URL.createObjectURL(new Blob([WORKLET_SRC], { type: 'application/javascript' }));
    await audioCtx.audioWorklet.addModule(workletURL);

    micNode = audioCtx.createMediaStreamSource(micStream);
    workletNode = new AudioWorkletNode(audioCtx, 'pcm-capture');

    workletNode.port.onmessage = (e) => {
      if (e.data.type !== 'audio') return;
      const { seq, bytes, isLast, rms } = e.data;
      if (ws && ws.readyState === WebSocket.OPEN) {
        const payload = {
          type: 'audio_chunk',
          session_id: sessionId,
          seq: seq,
          timestamp_ms: Date.now() - (startedAtMs || Date.now()),
          data: bytesToBase64(bytes),
          is_last: isLast,
        };
        ws.send(encodeIndication(payload));
        if (isLast) {
          startedAtMs = Date.now();
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

  function bytesToBase64(bytes) {
    let bin = '';
    const chunk = 0x8000;
    for (let i = 0; i < bytes.length; i += chunk) {
      bin += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
    }
    return btoa(bin);
  }

  // ====== 按钮 ======
  btnStart.onclick = async () => {
    btnStart.disabled = true;
    try {
      await connect();
      await startMic();
      btnStop.disabled = false;
      btnInterrupt.disabled = false;
      addMessage('system', '准备就绪。说一句试试。');
    } catch (e) {
      btnStart.disabled = false;
      addMessage('error', '启动失败：' + e.message);
    }
  };

  btnInterrupt.onclick = () => {
    sendInterrupt();
  };

  btnStop.onclick = () => {
    sendSessionEnd('user stopped');
    stopMic();
    if (ws) ws.close();
    btnStart.disabled = false;
    btnStop.disabled = true;
    btnInterrupt.disabled = true;
    addMessage('system', '已结束');
  };

  // ====== 启动 ======
  addMessage('system', '页面已加载。点"开始对话"启动。');
})();