// voice-app frontend — LLM+TTS 单能力 tab
// 输入文本 → POST /admin/llm_tts → 累积 audio chunk → 拼成 WAV → 播放/下载
// （接口只输出 TTS 音频，LLM 中间文本在服务端日志可见）

(function () {
  'use strict';

  const textEl = document.getElementById('llm_tts-text');
  const btnRun = document.getElementById('llm_tts-run');
  const status = document.getElementById('llm_tts-status');
  const audio = document.getElementById('llm_tts-audio');
  const download = document.getElementById('llm_tts-download');

  function reset() {
    if (audio.src) URL.revokeObjectURL(audio.src);
    audio.removeAttribute('src');
    audio.load();
    download.href = '#';
    download.hidden = true;
  }

  btnRun.onclick = async () => {
    const text = textEl.value.trim();
    if (!text) {
      status.textContent = '⚠️ 请先输入文本';
      return;
    }
    const voice = window.VoiceSelector.getSelected('llm_tts-voice');
    btnRun.disabled = true;
    textEl.disabled = true;
    reset();
    try {
      status.textContent = '调用 /admin/llm_tts ...';
      const body = { text };
      if (voice) body.voice = voice;
      const resp = await fetch('/admin/llm_tts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      const chunks = [];
      let totalBytes = 0;
      let chunkCount = 0;
      for await (const evt of window.parseNdjson(resp)) {
        if (evt.error) {
          status.textContent = '❌ ' + evt.error;
          return;
        }
        if (evt.audio) {
          const bytes = window.base64ToBytes(evt.audio);
          chunks.push(bytes);
          totalBytes += bytes.length;
          chunkCount++;
        }
        status.textContent = `已收 ${chunkCount} 个 chunk (seq=${evt.seq}, ${totalBytes} 字节 PCM)`;
      }

      if (totalBytes === 0) {
        status.textContent = '⚠️ 没返回音频';
        return;
      }

      const pcm = new Uint8Array(totalBytes);
      let off = 0;
      for (const c of chunks) { pcm.set(c, off); off += c.length; }

      const url = window.pcmChunksToWavUrl(pcm);
      audio.src = url;
      download.href = url;
      download.download = `llm-tts-${Date.now()}.wav`;
      download.hidden = false;
      audio.play().catch(() => {});
      status.textContent = `✅ 完成 (${chunkCount} chunks, ${totalBytes} 字节 PCM)`;
    } catch (e) {
      status.textContent = '❌ ' + e.message;
    } finally {
      btnRun.disabled = false;
      textEl.disabled = false;
    }
  };
})();