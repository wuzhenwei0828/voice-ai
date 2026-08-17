// voice-app frontend — 实时流式 ASR 页面（qwen3-asr-flash-realtime）
//
// 链路：麦克风 → AudioWorklet（重采样 16k + s16le 量化 + 100ms 攒帧）→
//       WS /stream/asr（binary PCM 帧 ↑ / JSON 事件 ↓）→
//       voice_server asr_stream_api → voice-providers Realtime 会话 → DashScope
//
// 与 app.js 的差异：
//   - 无客户端 VAD：断句完全交给服务端 VAD（turn_detection.server_vad）
//   - 协议是独立 JSON 事件（partial / final / speech_started / speech_stopped /
//     finished / error / started / stopped），不是 VoicePayload

(function () {
  'use strict';

  const TAG = '[asr-realtime]';
  const log = (...a) => console.log(TAG, ...a);
  const err = (...a) => console.error(TAG, ...a);

  // ===== 常量 =====

  const SAMPLE_RATE = 16000;
  // 100ms 一帧：1600 样本 = 3200 字节（与服务端 CHUNK_BYTES 对齐，省一次切片）
  const FRAME_SAMPLES = 1600;

  // ===== DOM =====

  const el = {
    wsStatus: document.getElementById('ws-status'),
    micStatus: document.getElementById('mic-status'),
    vadStatus: document.getElementById('vad-status'),
    streamStatus: document.getElementById('stream-status'),
    statusText: document.getElementById('status-text'),
    btnStart: document.getElementById('btn-start'),
    btnFinish: document.getElementById('btn-finish'),
    btnAbandon: document.getElementById('btn-abandon'),
    partialLine: document.getElementById('partial-line'),
    finalList: document.getElementById('final-list'),
    eventLog: document.getElementById('event-log'),
  };

  // ===== 状态 =====

  let ws = null;
  let audioCtx = null;
  let micStream = null;
  let workletNode = null;
  let running = false;      // start..finish 全程
  let finishing = false;    // 已发 finish，等 finished 事件
  let finalCount = 0;

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
  }

  // ===== AudioWorklet：重采样 + 量化 + 攒帧（无 VAD，连续采集） =====

  const WORKLET_SRC = `
    class RtCaptureProcessor extends AudioWorkletProcessor {
      constructor() {
        super();
        const o = (this.processorOptions || {});
        this._targetRate = o.targetRate || 16000;
        this._frameSamples = o.frameSamples || 1600;
        this._ratio = sampleRate / this._targetRate;   // sampleRate 是 worklet 全局量
        this._enabled = false;
        this._frame = new Int16Array(this._frameSamples);
        this._frameLen = 0;
        // 分数相位累加器（跨 process() 续推，不丢样本）
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
        // 降采样：ratio==1 直通；否则箱式滤波（抗混叠）+ 分数相位
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
    registerProcessor('rt-capture', RtCaptureProcessor);
  `;

  async function startMic() {
    micStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount: 1,
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
      },
    });

    // 优先按目标采样率建 AudioContext（浏览器自带抗混叠）；不支持则 worklet 内降采样
    try {
      audioCtx = new AudioContext({ sampleRate: SAMPLE_RATE });
    } catch (_) {
      audioCtx = new AudioContext();
      console.warn(TAG, `AudioContext({sampleRate:${SAMPLE_RATE}}) 不支持，用默认 ${audioCtx.sampleRate}Hz，worklet 内降采样`);
    }

    const workletURL = URL.createObjectURL(new Blob([WORKLET_SRC], { type: 'application/javascript' }));
    await audioCtx.audioWorklet.addModule(workletURL);
    URL.revokeObjectURL(workletURL);

    workletNode = new AudioWorkletNode(audioCtx, 'rt-capture', {
      numberOfInputs: 1, numberOfOutputs: 0,
      processorOptions: { targetRate: SAMPLE_RATE, frameSamples: FRAME_SAMPLES },
    });
    workletNode.port.onmessage = (e) => {
      if (e.data.type !== 'audio') return;
      if (ws && ws.readyState === WebSocket.OPEN && running && !finishing) {
        ws.send(e.data.bytes);
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

  // ===== WS 协议处理 =====

  function wsUrl() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    return `${proto}://${location.host}/stream/asr`;
  }

  function handleServerEvent(evt) {
    log('evt:', evt);
    switch (evt.type) {
      case 'started':
        setStatus(`会话 ${evt.session_id} 已建立，开始说话`);
        setBadge(el.streamStatus, '会话：识别中', 'badge-busy');
        logEvent(`started session=${evt.session_id}`);
        startMic().then(() => {
          running = true;
          el.btnFinish.disabled = false;
          el.btnAbandon.disabled = false;
          el.btnStart.disabled = true;
          setStatus('识别中…（说完一句稍停即出句终结果）');
        }).catch((e) => {
          err('mic error:', e);
          setStatus('无法访问麦克风：' + e.message);
          logEvent('❌ 麦克风失败: ' + e.message);
          sendCmd('stop');
        });
        break;
      case 'partial': {
        el.partialLine.replaceChildren();
        const span = document.createElement('span');
        span.className = 'partial';
        span.textContent = '… ' + evt.text;
        el.partialLine.appendChild(span);
        setStatus('识别中…（增量）');
        break;
      }
      case 'final': {
        el.partialLine.replaceChildren();
        const hint = document.createElement('span');
        hint.className = 'empty-hint';
        hint.textContent = '（可以说下一句，或点「结束」）';
        el.partialLine.appendChild(hint);
        const line = document.createElement('span');
        line.className = 'final';
        line.textContent = `✓ ${evt.text}`;
        el.finalList.appendChild(line);
        finalCount++;
        logEvent(`final #${finalCount}: ${evt.text}`);
        break;
      }
      case 'speech_started':
        setBadge(el.vadStatus, '服务端 VAD：说话中', 'badge-online vad-speaking');
        break;
      case 'speech_stopped':
        setBadge(el.vadStatus, '服务端 VAD：静音', '');
        break;
      case 'finished':
        logEvent('finished');
        cleanup('✅ 会话结束' + (finalCount ? `（共 ${finalCount} 句）` : ''));
        break;
      case 'stopped':
        logEvent('stopped（服务端已放弃会话）');
        cleanup('已放弃');
        break;
      case 'error':
        err('server error:', evt.message);
        logEvent('❌ ' + evt.message);
        setStatus('❌ ' + evt.message);
        // 错误后服务端会话已终止；若还在收音，本地复位
        if (running) cleanup('❌ ' + evt.message);
        break;
      default:
        logEvent('未知事件: ' + JSON.stringify(evt));
    }
  }

  function sendCmd(type) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type }));
      logEvent(`→ ${type}`);
    }
  }

  function resetButtons() {
    el.btnStart.disabled = false;
    el.btnFinish.disabled = true;
    el.btnAbandon.disabled = true;
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
    setBadge(el.vadStatus, '服务端 VAD：--', '');
    setBadge(el.streamStatus, '会话：空闲', '');
    resetButtons();
    setStatus(statusText);
  }

  // ===== 按钮 =====

  el.btnStart.onclick = () => {
    if (running) return;
    clearTranscript();
    setStatus('连接 WS …');
    setBadge(el.wsStatus, 'WS：连接中…', 'badge-busy');

    ws = new WebSocket(wsUrl());
    ws.binaryType = 'arraybuffer';

    ws.onopen = () => {
      log('ws open');
      setBadge(el.wsStatus, 'WS：已连接', 'badge-online');
      logEvent('ws connected');
      sendCmd('start');
      setStatus('等待服务端会话建立…');
    };

    ws.onmessage = (e) => {
      if (typeof e.data !== 'string') return; // 本页面无下行 binary
      let evt;
      try {
        evt = JSON.parse(e.data);
      } catch (_) {
        err('bad json:', e.data);
        return;
      }
      handleServerEvent(evt);
    };

    ws.onerror = () => {
      err('ws error');
      logEvent('❌ WS 错误');
    };

    ws.onclose = () => {
      log('ws close');
      logEvent('ws closed');
      if (running) cleanup('连接断开');
    };
  };

  el.btnFinish.onclick = () => {
    if (!running || finishing) return;
    finishing = true;
    el.btnFinish.disabled = true;
    setStatus('已发 finish，等待剩余结果…');
    stopMic(); // 不再采集；等服务端吐完剩余事件 + session.finished
    sendCmd('finish');
  };

  el.btnAbandon.onclick = () => {
    if (!running) return;
    sendCmd('stop');
    cleanup('已放弃当前会话');
  };

  // 页面关闭 → WS 断开 → 服务端自动 abandon（连接归还池）
  window.addEventListener('beforeunload', () => {
    if (ws) { try { ws.close(); } catch (_) {} }
  });

  clearTranscript();
  setStatus('空闲');
})();
