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

  // ====== Tab 切换 ======
  // 不影响现有 WS pipeline 状态；切换 tab 只显示/隐藏对应 section
  const TABS = ['pipeline', 'asr', 'llm', 'tts', 'llm_tts', 'asr_llm_tts'];
  function activateTab(name) {
    if (!TABS.includes(name)) name = 'pipeline';
    document.querySelectorAll('.tab-btn').forEach((b) => {
      b.classList.toggle('active', b.dataset.tab === name);
    });
    TABS.forEach((t) => {
      const el = document.getElementById('tab-' + t);
      if (el) el.hidden = (t !== name);
    });
    try { localStorage.setItem('voice-app.activeTab', name); } catch (_) {}
  }
  document.querySelectorAll('.tab-btn').forEach((b) => {
    b.onclick = () => activateTab(b.dataset.tab);
  });
  let initialTab = 'pipeline';
  try {
    const saved = localStorage.getItem('voice-app.activeTab');
    if (saved && TABS.includes(saved)) initialTab = saved;
  } catch (_) {}
  activateTab(initialTab);

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
    return msgpackEncode({ Indication: { data: payload } });
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
                // 下行：服务端用 #[serde(with="serde_bytes")] 走 msgpack bin，JS 解码后已是 Uint8Array
                playTtsAudio(payload.data, payload.is_last);
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
          data: bytes,
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

  // ====== 按钮 ======
  btnStart.onclick = async () => {
    btnStart.disabled = true;
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
      addMessage('error', '启动失败：' + e.message);
    }
  };

  // ====== 调试：诊断 ws.send 行为 ======
  // 测试 1：发送 native msgpack 编码结果
  function testWsSendBinary() {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      addMessage('error', 'WS 未连接');
      return;
    }
    const obj = {Indication: {data: {type: 'session_start', session_id: 'web-demo', sample_rate: 16000, channels: 1, codec: 'pcm_s16le', language: 'zh-CN'}}};
    const bytes = msgpackEncode(obj);
    console.log('[DBG] sending bytes, length=', bytes.length, 'ctor=', bytes.constructor.name);
    ws.send(bytes);
    addMessage('system', '测试：发送 native msgpack 字节');
  }
  window.__testWs = { testWsSendBinary };
  addMessage('system', '调试：控制台运行 __testWs.testWsSendBinary()');

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
  // 页面加载时立即请求麦克风权限（不连 WS，只拿权限）
  async function requestMicOnLoad() {
    setStatus(micStatus, '麦克风：请求权限...', 'badge-busy');
    addMessage('system', '正在请求麦克风权限...');
    try {
      await startMic();
      addMessage('system', '麦克风权限已获得。点"开始对话"连接服务。');
    } catch (e) {
      setStatus(micStatus, '麦克风：拒绝', 'badge-offline');
      addMessage('error', '麦克风权限被拒绝：' + e.message + '。请在浏览器地址栏左侧允许后刷新页面。');
    }
  }

  // 立即请求麦克风
  requestMicOnLoad();
})();