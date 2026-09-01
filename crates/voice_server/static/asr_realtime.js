// voice-ai frontend — 实时流式 ASR 页面（live-asr，wspool 复用）
//
// 链路：麦克风 → AudioWorklet（16k 重采样 + s16le + 100ms 帧）→
//       WS /ws/live-asr/web/<connid>（webhttp 既有路由）→ 服务端 live_asr_api →
//       voice-providers WsPool + qwen.rs adapter → DashScope 公共协议
//
// 协议：复用 voice-proto 的 VoicePayload（msgpack over binary frame）；
// 服务端把 DashScope 识别事件映射成 AsrPartial { text, is_final } 下行。

(function () {
  'use strict';

  const TAG = '[asr-live]';
  const log = (...a) => console.log(TAG, ...a);
  const err = (...a) => console.error(TAG, ...a);

  const SAMPLE_RATE = 16000;
  // 100ms 一帧：1600 样本 = 3200 字节，与服务端 ws_pool CHUNK_BYTES 对齐
  const FRAME_SAMPLES = 1600;
  const CHANNELS = 1;
  const CODEC = 'pcm';
  const LANGUAGE = 'zh-CN';

  // ===== DOM =====

  const el = {
    wsStatus: document.getElementById('ws-status'),
    micStatus: document.getElementById('mic-status'),
    streamStatus: document.getElementById('stream-status'),
    statusText: document.getElementById('status-text'),
    btnStart: document.getElementById('btn-start'),
    btnFinish: document.getElementById('btn-finish'),
    btnAbandon: document.getElementById('btn-abandon'),
    btnClear: document.getElementById('btn-clear'),
    partialLine: document.getElementById('partial-line'),
    finalList: document.getElementById('final-list'),
    finalCount: document.getElementById('final-count'),
    eventLog: document.getElementById('event-log'),
  };

  // ===== 状态 =====

  let ws = null;
  let audioCtx = null;
  let micStream = null;
  let workletNode = null;
  let running = false;
  let finishing = false;
  let finalCount = 0;

  // ====== MessagePack 编解码（从 app.js 复制以保证 wire format 一致） ======
  // 与 server-side voice-proto + rmp_serde 兼容：bin 走 bin8/0xc4 等，
  // externally tagged enum 形如 { Indication: { data: <payload> } }。

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

  // 排查阶段：把 bytes 转成 hex（最多 N 字节），下行异常时贴 console 看 wire format
  function _toHex(u8, max = 64) {
    const n = Math.min(u8.length, max);
    let out = '';
    for (let i = 0; i < n; i++) out += u8[i].toString(16).padStart(2, '0') + ' ';
    return out.trim();
  }

  // ===== UI helper =====

  function setBadge(node, text, cls) {
    node.textContent = text;
    node.className = 'badge' + (cls ? ' ' + cls : '');
  }

  function setStatus(text) { el.statusText.textContent = text; }

  function logEvent(line) {
    if (el.eventLog.querySelector('.empty-hint')) el.eventLog.replaceChildren();
    const div = document.createElement('div');
    const t = new Date().toTimeString().slice(0, 8);
    div.textContent = `[${t}] ${line}`;
    el.eventLog.appendChild(div);
    el.eventLog.scrollTop = el.eventLog.scrollHeight;
  }

  function clearTranscript() {
    el.partialLine.replaceChildren();
    const hint = document.createElement('span');
    hint.className = 'empty-hint';
    hint.textContent = '（还没开始说话）';
    el.partialLine.appendChild(hint);
    el.finalList.replaceChildren();
    finalCount = 0;
    el.finalCount.textContent = '0';
    el.btnClear.disabled = true;
  }

  // ===== partial 行渲染 helper =====
  // appendPartial(text): 把字符 append 到当前 partial 行的 span。FunASR 服务端流式
  // 推的就是增量字符（见 runtime/html5/static/main.js:362 "rec_text += text"），前端
  // 直接 append 即可，无需在浏览器侧做 diff。
  function appendPartial(text) {
    if (!text || text.length === 0) return;
    let span = el.partialLine.querySelector('span.partial');
    if (!span) {
      span = document.createElement('span');
      span.className = 'partial';
      span.textContent = '… ';
      el.partialLine.replaceChildren(span);
    }
    span.textContent += text;
  }

  // 把一条文本上屏为 final 行的 helper。
  // 形态：<idx> <ts> <text>，按时间顺序追加到 #final-list 末尾（**不删除**前面的行）。
  // 这样多次说话后所有句子都在列表里，按 #final-count 计总。
  function appendFinalLine(text, opts) {
    const isErr = opts && opts.isErr;
    const corrected = opts && opts.corrected;
    finalCount++;
    el.finalCount.textContent = String(finalCount);
    el.btnClear.disabled = false;

    const line = document.createElement('div');
    line.className = 'final' + (isErr ? ' err' : '') + (corrected ? ' corrected' : '');

    const idx = document.createElement('span');
    idx.className = 'idx';
    idx.textContent = '#' + finalCount;

    const ts = document.createElement('span');
    ts.className = 'ts';
    const now = new Date();
    ts.textContent = now.toTimeString().slice(0, 8); // HH:MM:SS

    const txt = document.createElement('span');
    txt.className = 'text';
    txt.textContent = text;

    line.appendChild(idx);
    line.appendChild(ts);
    line.appendChild(txt);
    el.finalList.appendChild(line);

    // auto-scroll：始终让最新一行可见（长 session 列表里手动往上滚的历史不丢）
    el.finalList.scrollTop = el.finalList.scrollHeight;
  }

  // finalizePartial({isErr}): 把当前 partial 行移到 final 列表 / 清空 partial
  // - isErr=true: 错误消息，整段上屏为错误行
  // 2pass-offline 修正不走这里 —— 走下面的 replaceLastFinalText
  function finalizePartial(opts) {
    const isErr = opts && opts.isErr;
    const span = el.partialLine.querySelector('span.partial');
    const partialText = span ? span.textContent.replace(/^… /, '') : '';
    if (isErr && partialText) {
      appendFinalLine('❌ ' + partialText, { isErr: true });
      logEvent('error: ' + partialText);
    } else if (span && partialText) {
      appendFinalLine(partialText, {});
      logEvent('final: ' + partialText);
    }
    // 清空 partial 行，留个 hint
    el.partialLine.replaceChildren();
    const hint = document.createElement('span');
    hint.className = 'empty-hint';
    hint.textContent = isErr
      ? '（出错了，可重试）'
      : '（可以说下一句，或点「结束」）';
    el.partialLine.appendChild(hint);
  }

  // replaceLastFinalText(text): 2pass-offline 二次纠错专用 —— 用 text 替换最近一条 final 行
  // 的文本内容（不新增行）。如果列表为空则回退为新加一行。
  function replaceLastFinalText(text) {
    const last = el.finalList.lastElementChild;
    if (!last) {
      appendFinalLine(text, { corrected: true });
      return;
    }
    // 仅替换 .text span —— 序号 / 时间戳保持原值
    const txt = last.querySelector('.text');
    if (txt) txt.textContent = text;
    last.classList.add('corrected');
    el.finalList.scrollTop = el.finalList.scrollHeight;
  }

  // resetPartial: 新会话开始时重置 partial 行（去掉上次的 hint / partial span）
  function resetPartial() {
    el.partialLine.replaceChildren();
    const hint = document.createElement('span');
    hint.className = 'empty-hint';
    hint.textContent = '（还没开始说话）';
    el.partialLine.appendChild(hint);
  }

  // ===== AudioWorklet：重采样 + 量化 + 攒帧（无客户端 VAD） =====

  const WORKLET_SRC = `
    class LiveCaptureProcessor extends AudioWorkletProcessor {
      constructor(options) {
        super(options);
        const o = (options && options.processorOptions) || {};
        this._targetRate = o.targetRate || 16000;
        this._frameSamples = o.frameSamples || 1600;
        this._ratio = sampleRate / this._targetRate;
        this._enabled = false;
        this._frame = new Int16Array(this._frameSamples);
        this._frameLen = 0;
        this._phase = 0;
        this._acc = 0;
        this._accN = 0;
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
        if (this._frameLen < this._frameSamples) return;
        this._frameLen = 0;
        this.port.postMessage({ type: 'audio', bytes: new Uint8Array(this._frame.buffer.slice(0)) });
      }
    }
    registerProcessor('live-capture', LiveCaptureProcessor);
  `;

  async function startMic() {
    micStream = await navigator.mediaDevices.getUserMedia({
      audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true },
    });
    try {
      audioCtx = new AudioContext({ sampleRate: SAMPLE_RATE });
    } catch (_) {
      audioCtx = new AudioContext();
      console.warn(TAG, `AudioContext({sampleRate:${SAMPLE_RATE}}) 不支持，用默认 ${audioCtx.sampleRate}Hz`);
    }
    const workletURL = URL.createObjectURL(new Blob([WORKLET_SRC], { type: 'application/javascript' }));
    await audioCtx.audioWorklet.addModule(workletURL);
    URL.revokeObjectURL(workletURL);

    workletNode = new AudioWorkletNode(audioCtx, 'live-capture', {
      numberOfInputs: 1, numberOfOutputs: 0,
      processorOptions: { targetRate: SAMPLE_RATE, frameSamples: FRAME_SAMPLES },
    });
    workletNode.port.onmessage = (e) => {
      if (e.data.type !== 'audio') return;
      if (ws && ws.readyState === WebSocket.OPEN && running && !finishing) {
        const seq = ++seqCounter;
        sendPayload({
          type: 'audio_chunk',
          session_id: sessionId,
          seq: seq,
          timestamp_ms: Date.now(),
          data: e.data.bytes,
          is_last: false,
        });
        // 关键证据：running=true 后第一帧推出去 = ack-gating 生效
        // 之后每 50 帧 (~5s) 打一条，防刷屏
        if (seq === 1 || seq % 50 === 0) {
          logEvent(`→ audio_chunk #${seq} bytes=${e.data.bytes.length} (running=${running})`);
        }
      }
    };

    const src = audioCtx.createMediaStreamSource(micStream);
    src.connect(workletNode);
    workletNode.port.postMessage({ cmd: 'start' });
    setBadge(el.micStatus, '麦克风：开', 'badge-online');
  }

  function stopMic() {
    if (workletNode) {
      try { workletNode.port.postMessage({ cmd: 'stop' }); } catch (_) {}
      workletNode.disconnect();
      workletNode = null;
    }
    if (micStream) {
      micStream.getTracks().forEach((t) => t.stop());
      micStream = null;
    }
    if (audioCtx) {
      audioCtx.close().catch(() => {});
      audioCtx = null;
    }
    setBadge(el.micStatus, '麦克风：关', 'badge-offline');
  }

  // ===== WS 协议 =====

  let sessionId = '';
  let seqCounter = 0;

  function wsUrl() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    return `${proto}://${location.host}/ws/live-asr/web/${sessionId}`;
  }

  function handleServerEvent(payload) {
    log('evt:', payload);
    if (!payload || !payload.type) return;
    if (payload.type === 'session_ack') {
      // 服务端处理 SessionStart 后的握手 ack：
      // success=true → 上游 WSS 已建好，可以开麦克风推 PCM
      // success=false → 上游建连失败（缺 endpoint / 握手失败 / ...），前端直接报错
      logEvent('← session_ack success=' + payload.success +
        (payload.message ? ' message=' + payload.message : ''));
      if (payload.success) {
        setBadge(el.streamStatus, '会话：识别中', 'badge-busy');
        el.btnFinish.disabled = false;
        el.btnAbandon.disabled = false;
        el.btnStart.disabled = true;
        running = true;
        resetPartial();
        setStatus('识别中…（说完一句稍停即出句终）');
      } else {
        logEvent('❌ 握手失败: ' + (payload.message || 'unknown'));
        cleanup('连接 ASR 失败：' + (payload.message || 'unknown'));
      }
    } else if (payload.type === 'asr_partial') {
      const isErr = typeof payload.text === 'string' && payload.text.startsWith('[error]');
      const replaceLast = payload.replace_last === true; // msgpack boolean 解码
      // 服务端按 FunASR 增量语义下发：
      // - is_final=false → text 是新字符，直接 append 到当前 partial 行（"说一个字前端展示一个字"）
      // - is_final=true + replace_last=false → text 是补齐当前句子的 delta，先 append 到 partial，
      //   再把整个 partial 行移到 final 列表（**追加**，不替换旧行）
      // - is_final=true + replace_last=true → text 是 2pass-offline 二次纠错的完整句子，
      //   替换最近一条 final 行的文本（**不**新增行，序号 / 时间戳保持）
      if (isErr) {
        appendPartial(payload.text);
        finalizePartial({ isErr: true });
      } else if (payload.is_final && replaceLast) {
        // 2pass-offline：替换最近一条 final 的 .text span（序号 / 时间戳不变）
        replaceLastFinalText(payload.text);
        logEvent('corrected: ' + payload.text);
      } else if (payload.is_final) {
        // 2pass-online 句终 / offline：先把 text（delta）补到 partial，再上屏为 final
        if (payload.text.length > 0) appendPartial(payload.text);
        finalizePartial({});
      } else {
        // 流式增量：直接 append
        if (payload.text.length > 0) appendPartial(payload.text);
      }
    } else if (payload.type === 'error') {
      logEvent('error: ' + (payload.code || '') + ' ' + (payload.message || ''));
      setStatus('❌ ' + (payload.message || 'error'));
    } else {
      logEvent('忽略: ' + JSON.stringify(payload));
    }
  }

  function sendPayload(payload) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      const message = { ...payload, message_id: payload.message_id || createTraceId() };
      const bytes = encodeIndication(message);
      log('[voice-ws] send', {
        sessionId,
        type: message.type,
        messageId: message.message_id,
        bytes: bytes.byteLength,
      });
      ws.send(bytes);
    }
  }

  function resetButtons() {
    el.btnStart.disabled = false;
    el.btnFinish.disabled = true;
    el.btnAbandon.disabled = true;
    el.btnClear.disabled = finalCount === 0;
  }

  function cleanup(statusText) {
    running = false;
    finishing = false;
    stopMic();
    if (ws) {
      try { ws.close(); } catch (_) {}
      ws = null;
    }
    setBadge(el.wsStatus, 'WS：未连接', 'badge-offline');
    setBadge(el.streamStatus, '会话：空闲', '');
    resetButtons();
    setStatus(statusText);
  }

  // ===== 按钮 =====

  el.btnStart.onclick = () => {
    if (running) return;
    clearTranscript();
    sessionId = 'live-' + Date.now().toString(36);
    seqCounter = 0;
    setStatus('连接 WS …');
    setBadge(el.wsStatus, 'WS：连接中…', 'badge-busy');

    ws = new WebSocket(wsUrl());
    ws.binaryType = 'arraybuffer';

    ws.onopen = () => {
      log('ws open →', wsUrl());
      setBadge(el.wsStatus, 'WS：已连接', 'badge-online');
      logEvent('ws connected ' + sessionId);
      // 第一步：发 session_start 触发服务端建上游 WSS
      sendPayload({
        type: 'session_start',
        session_id: sessionId,
        sample_rate: SAMPLE_RATE,
        channels: CHANNELS,
        codec: CODEC,
        language: LANGUAGE,
      });
      logEvent('→ session_start（等 ack 才能推 PCM）');
      setStatus('等待服务端确认上游 ASR …');
      // 第二步：等 session_ack(success=true) 后才启动麦克风 —— 由 handleServerEvent 触发
      // 先把麦克风拉起来，但 worklet 还没收到 'start' cmd 不会推帧
      startMic().catch((e) => {
        err('mic error:', e);
        logEvent('❌ 麦克风失败: ' + e.message);
        sendPayload({ type: 'session_end', session_id: sessionId, reason: 'mic-error' });
        cleanup('无法访问麦克风：' + e.message);
      });
    };

    ws.onmessage = (ev) => {
      const bytes = new Uint8Array(ev.data);
      // 排查阶段：先打 raw 看下行到底有没有来 / 解码有没有炸
      let payload;
      try {
        payload = decodeMessage(bytes);
      } catch (e) {
        logEvent('← raw ws msg decode threw: ' + e.message + ' (bytes=' + bytes.length + ')');
        console.error(TAG, 'decode threw:', e, 'raw hex:', _toHex(bytes));
        return;
      }
      if (!payload) {
        logEvent('← raw ws msg decode=null (bytes=' + bytes.length + ' hex=' + _toHex(bytes).slice(0, 32) + '…)');
        console.warn(TAG, 'decode=null; raw hex:', _toHex(bytes));
        return;
      }
      log('[voice-ws] receive', {
        sessionId,
        type: payload.type,
        messageId: payload.message_id,
        bytes: bytes.byteLength,
      });
      handleServerEvent(payload);
    };

    ws.onerror = () => {
      err('ws error');
      logEvent('❌ WS 错误');
    };

    ws.onclose = (e) => {
      log('ws close code=', e.code, 'reason=', e.reason);
      logEvent('ws closed');
      if (running) cleanup('连接断开');
    };
  };

  el.btnFinish.onclick = () => {
    if (!running || finishing) return;
    finishing = true;
    el.btnFinish.disabled = true;
    setStatus('已发 finish，等待剩余结果…');
    stopMic();
    sendPayload({ type: 'session_end', session_id: sessionId, reason: 'normal' });
    logEvent('→ session_end');
  };

  el.btnAbandon.onclick = () => {
    if (!running) return;
    logEvent('abandon: 直接断 WS，不等 finish');
    cleanup('已放弃（连接直接断开；服务端 wspool 端将由连接关闭事件 release）');
  };

  // 清空转写结果（不影响 WS 连接 / 不影响会话状态）。
  // 用于：长 session 后列表太长想重来；或想从某个时间点重新看 partial。
  el.btnClear.onclick = () => {
    if (running) {
      // 识别中清空是安全的：服务端继续推 final，前端列表从 1 重新计
      logEvent('清空 final 列表（识别继续）');
    } else {
      logEvent('清空 final 列表');
    }
    el.finalList.replaceChildren();
    finalCount = 0;
    el.finalCount.textContent = '0';
    el.btnClear.disabled = true;
  };

  window.addEventListener('beforeunload', () => {
    if (ws) { try { ws.close(); } catch (_) {} }
  });

  clearTranscript();
  setStatus('空闲');
})();
