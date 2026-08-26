// voice-app frontend — TTS 单能力 tab
// 输入文本 → POST /admin/tts → 累积 audio chunk → 拼成 WAV → 播放/下载

(function () {
  'use strict';

  const textEl = document.getElementById('tts-text');
  const btnRun = document.getElementById('tts-run');
  const status = document.getElementById('tts-status');
  const audio = document.getElementById('tts-audio');
  const download = document.getElementById('tts-download');

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
    const voice = window.VoiceSelector.getSelected('tts-voice');
    btnRun.disabled = true;
    textEl.disabled = true;
    reset();
    try {
      status.textContent = '调用 /admin/tts ...';
      const body = { text };
      if (voice) body.voice = voice;
      const resp = await fetch('/admin/tts', {
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
        status.textContent = `已收 ${chunkCount} 个 chunk (${totalBytes} 字节 PCM)`;
      }

      if (totalBytes === 0) {
        status.textContent = '⚠️ TTS 没返回音频';
        return;
      }

      // 拼成完整 PCM
      const pcm = new Uint8Array(totalBytes);
      let off = 0;
      for (const c of chunks) { pcm.set(c, off); off += c.length; }

      // 拼 WAV header
      const url = window.pcmChunksToWavUrl(pcm);
      audio.src = url;
      download.href = url;
      download.download = `tts-${Date.now()}.wav`;
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