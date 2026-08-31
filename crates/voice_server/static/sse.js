// voice-app frontend — SSE 流解析 helper
// 把 fetch response 的 body 流按 SSE 空行边界切成 JSON data 事件逐个 yield。
// 用于消费 /admin/asr /admin/llm /admin/tts /admin/llm_tts /admin/asr_llm_tts。

window.parseSse = async function* parseSse(response) {
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} ${response.statusText}`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  let dataLines = [];

  function* emitEvent(block) {
    const lines = block.split(/\r?\n/);
    const data = lines
      .filter((line) => line.startsWith('data:'))
      .map((line) => line.slice(5).replace(/^ /, ''))
      .join('\n');
    if (data) yield JSON.parse(data);
  }

  function* flushBlock() {
    const block = dataLines.join('\n');
    dataLines = [];
    yield* emitEvent(block);
  }

  for (;;) {
    const { value, done } = await reader.read();
    if (done) {
      buf += decoder.decode();
      break;
    }
    buf += decoder.decode(value, { stream: true });
    let newline;
    while ((newline = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, newline).replace(/\r$/, '');
      buf = buf.slice(newline + 1);
      if (line === '') {
        yield* flushBlock();
      } else if (!line.startsWith(':')) {
        dataLines.push(line);
      }
    }
  }
  if (buf) {
    const line = buf.replace(/\r$/, '');
    if (line && !line.startsWith(':')) dataLines.push(line);
  }
  if (dataLines.length) yield* flushBlock();
};

// 把 Uint8Array 拼成完整 WAV（s16le 16kHz mono），返回 Blob URL
window.pcmChunksToWavUrl = function(pcmBytes, sampleRate = 16000, channels = 1) {
  const dataLen = pcmBytes.byteLength;
  const buf = new ArrayBuffer(44 + dataLen);
  const view = new DataView(buf);
  function writeStr(off, s) { for (let i = 0; i < s.length; i++) view.setUint8(off + i, s.charCodeAt(i)); }
  writeStr(0, 'RIFF');
  view.setUint32(4, 36 + dataLen, true);
  writeStr(8, 'WAVE');
  writeStr(12, 'fmt ');
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, channels, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * channels * 2, true);
  view.setUint16(32, channels * 2, true);
  view.setUint16(34, 16, true);
  writeStr(36, 'data');
  view.setUint32(40, dataLen, true);
  new Uint8Array(buf, 44).set(pcmBytes);
  const blob = new Blob([buf], { type: 'audio/wav' });
  return URL.createObjectURL(blob);
};

// 调试页使用服务端当前 TTS 模型的实际 PCM 格式，避免写死 16kHz。
window.ttsAudioFormat = { sampleRate: 16000, channels: 1 };
window.loadTtsAudioFormat = (async function() {
  try {
    const resp = await fetch('/admin/tts/format');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    if (Number.isFinite(data.sample_rate) && data.sample_rate > 0) {
      window.ttsAudioFormat.sampleRate = data.sample_rate;
    }
    if (Number.isInteger(data.channels) && data.channels > 0) {
      window.ttsAudioFormat.channels = data.channels;
    }
  } catch (e) {
    console.warn('[tts-format] 加载当前 TTS 音频格式失败，使用默认值:', e);
  }
  return window.ttsAudioFormat;
})();

// base64 字符串 → Uint8Array
window.base64ToBytes = function(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
};

// 简易 WAV 解析（与 app.js::extractPcmFromWav 等价；复制一份避免跨 IIFE 引用）
// 防御：header 里的 dataSize / fmt 字段不可信，必须校验并按文件实际字节 clamp，
// 否则恶意/损坏的 WAV 写一个超大的 dataSize 会让 new Int16Array 报
// "Array buffer allocation failed"。
window.extractPcmFromWav = function(bytes) {
  if (bytes.length < 44) throw new Error('WAV 太短');
  const head = String.fromCharCode(bytes[0], bytes[1], bytes[2], bytes[3]);
  const wave = String.fromCharCode(bytes[8], bytes[9], bytes[10], bytes[11]);
  if (head !== 'RIFF' || wave !== 'WAVE') throw new Error('不是 WAV');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let pos = 12;
  let fmt = null;
  let dataOffset = -1;
  let dataSize = 0;
  while (pos + 8 <= bytes.length) {
    const id = String.fromCharCode(bytes[pos], bytes[pos+1], bytes[pos+2], bytes[pos+3]);
    const size = view.getUint32(pos + 4, true);
    // chunk size 声明超出文件剩余字节 → 截断到实际剩余
    const remaining = bytes.length - (pos + 8);
    const safeSize = size > remaining ? remaining : size;
    if (id === 'fmt ') {
      if (safeSize < 16) throw new Error('fmt chunk 过短');
      fmt = {
        audioFormat: view.getUint16(pos + 8, true),
        numChannels: view.getUint16(pos + 10, true),
        sampleRate: view.getUint32(pos + 12, true),
        bitsPerSample: view.getUint16(pos + 22, true),
      };
    } else if (id === 'data') {
      dataOffset = pos + 8;
      dataSize = safeSize;
      break;
    }
    // 防御：size==0 会死循环，必须前进
    const advance = 8 + safeSize + (safeSize % 2);
    if (advance === 0) break;
    pos += advance;
  }
  if (!fmt || dataOffset < 0) throw new Error('WAV chunks missing');

  // 防御：fmt 字段合理性（恶意/损坏 WAV 可能写 0、0xFFFFFFFF 之类）
  if (fmt.numChannels < 1 || fmt.numChannels > 8) {
    throw new Error('不支持的通道数: ' + fmt.numChannels);
  }
  if (fmt.sampleRate < 1 || fmt.sampleRate > 192000) {
    throw new Error('不支持的采样率: ' + fmt.sampleRate);
  }
  if (![8, 16, 24, 32].includes(fmt.bitsPerSample)) {
    throw new Error('不支持的位深: ' + fmt.bitsPerSample);
  }
  // 仅支持 PCM(1) 与 WAVEFORMATEXTENSIBLE(0xFFFE)；其它压缩格式不处理
  if (fmt.audioFormat !== 1 && fmt.audioFormat !== 0xFFFE) {
    throw new Error('不支持的编码格式 code=' + fmt.audioFormat + '（仅支持 PCM）');
  }

  // 防御：dataSize 截断到文件实际剩余字节
  if (dataOffset + dataSize > bytes.length) {
    dataSize = bytes.length - dataOffset;
  }

  const ratio = fmt.sampleRate / 16000;
  const totalFrames = Math.floor(dataSize / (fmt.numChannels * fmt.bitsPerSample / 8));
  const targetFrames = Math.floor(totalFrames / ratio);
  if (targetFrames <= 0) return new Uint8Array(0);
  // 防御：超过 2G 采样（≈ 24h @ 24kHz）直接拒，避免 Int16Array 分配失败
  if (targetFrames > 0x7FFFFFFF) {
    throw new Error('WAV 数据过大（target frames = ' + targetFrames + '）');
  }
  const out = new Int16Array(targetFrames);
  const bytesPerSample = fmt.bitsPerSample / 8;
  const bytesPerFrame = bytesPerSample * fmt.numChannels;
  for (let i = 0; i < targetFrames; i++) {
    const srcFrame = Math.floor(i * ratio);
    const srcOff = dataOffset + srcFrame * bytesPerFrame;
    // 防御：srcOff 超出文件 → 视为 0（尾部不完整采样静默补 0，不再越界读）
    if (srcOff + bytesPerFrame > bytes.length) {
      out[i] = 0;
      continue;
    }
    let sum = 0;
    for (let c = 0; c < fmt.numChannels; c++) {
      const off = srcOff + c * bytesPerSample;
      let v;
      if (fmt.bitsPerSample === 16) v = view.getInt16(off, true);
      else if (fmt.bitsPerSample === 8) v = (view.getUint8(off) - 128) * 256;
      else if (fmt.bitsPerSample === 24) {
        const b0 = view.getUint8(off), b1 = view.getUint8(off+1), b2 = view.getInt8(off+2);
        v = (b2 << 16) | (b1 << 8) | b0;
      } else if (fmt.bitsPerSample === 32) v = view.getInt32(off, true) >> 16;
      else v = 0;
      sum += v;
    }
    out[i] = Math.max(-32768, Math.min(32767, Math.round(sum / fmt.numChannels)));
  }
  return new Uint8Array(out.buffer, out.byteOffset, out.byteLength);
};
