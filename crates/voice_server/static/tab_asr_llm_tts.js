// voice-app frontend — ASR→LLM→TTS 全链路 tab
// 选文件 → POST /admin/asr_llm_tts → 按 stage 分发渲染 + 累积音频拼 WAV 播放

(function () {
  'use strict';

  const fileInput = document.getElementById('asr_llm_tts-file-input');
  const btnRun = document.getElementById('asr_llm_tts-run');
  const status = document.getElementById('asr_llm_tts-status');
  const asrOut = document.getElementById('asr_llm_tts-asr-output');
  const llmOut = document.getElementById('asr_llm_tts-llm-output');
  const audio = document.getElementById('asr_llm_tts-audio');
  const download = document.getElementById('asr_llm_tts-download');

  const TAG = '[tab-asr_llm_tts]';
  const log = (...a) => console.log(TAG, ...a);
  const err = (...a) => console.error(TAG, ...a);

  let selectedFile = null;

  function setStatus(text) { status.textContent = text; }

  function setError(message) {
    setStatus('❌ ' + message);
    const el = document.createElement('div');
    el.style.color = '#f87171';
    el.textContent = '❌ ' + message;
    asrOut.appendChild(el);
  }

  function reset() {
    [asrOut, llmOut].forEach((box) => {
      box.replaceChildren();
      const hint = document.createElement('span');
      hint.className = 'empty-hint';
      hint.textContent = '（无）';
      box.appendChild(hint);
    });
    if (audio.src) URL.revokeObjectURL(audio.src);
    audio.removeAttribute('src');
    audio.load();
    download.href = '#';
    download.hidden = true;
  }

  fileInput.onchange = (e) => {
    selectedFile = e.target.files[0] || null;
    btnRun.disabled = !selectedFile;
    setStatus(selectedFile
      ? `已选择: ${selectedFile.name} (${selectedFile.size} bytes)`
      : '等待选择文件...');
    reset();
    log('onchange:', selectedFile ? selectedFile.name : 'cleared');
  };

  btnRun.onclick = async () => {
    if (!selectedFile) return;
    btnRun.disabled = true;
    fileInput.disabled = true;
    reset();

    try {
      const buf = await selectedFile.arrayBuffer();
      log('upload start:', selectedFile.name, buf.byteLength, 'B');

      setStatus('调用 /admin/asr_llm_tts ...');
      const resp = await fetch('/admin/asr_llm_tts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/octet-stream' },
        body: buf,
      });
      log('fetch response:', resp.status, resp.statusText);

      const chunks = [];
      let totalBytes = 0;
      let chunkCount = 0;
      let llmText = '';
      let asrLineEl = null;
      let llmPlaceholderRemoved = false;

      for await (const evt of window.parseNdjson(resp)) {
        if (evt.error) {
          err('stream error line:', evt);
          setError(`[code ${evt.code}] ${evt.error}`);
          return;
        }
        if (evt.stage === 'asr') {
          if (!asrLineEl) {
            asrOut.replaceChildren();
            asrLineEl = document.createElement('div');
            asrOut.appendChild(asrLineEl);
          }
          asrLineEl.textContent = (evt.is_final ? '✓ ' : '… ') + evt.text;
          asrLineEl.className = evt.is_final ? 'final' : 'partial';
          setStatus(`ASR: ${evt.text.slice(0, 30)}${evt.text.length > 30 ? '…' : ''}`);
        } else if (evt.stage === 'llm') {
          if (!llmPlaceholderRemoved) {
            llmOut.replaceChildren();
            llmPlaceholderRemoved = true;
          }
          llmText += evt.delta;
          llmOut.textContent = llmText;
          if (!evt.is_final) setStatus(`LLM 生成中 (${llmText.length} 字)`);
        } else if (evt.stage === 'tts') {
          if (evt.audio) {
            chunks.push(window.base64ToBytes(evt.audio));
            totalBytes += chunks[chunks.length - 1].length;
            chunkCount++;
            setStatus(`TTS: 已收 ${chunkCount} chunks (${totalBytes} 字节 PCM)`);
          }
        }
      }
      log('done, llm chars =', llmText.length, 'tts chunks =', chunkCount);

      if (totalBytes === 0) {
        setStatus('⚠️ 没返回音频');
        return;
      }

      const pcmAll = new Uint8Array(totalBytes);
      let off = 0;
      for (const c of chunks) { pcmAll.set(c, off); off += c.length; }

      const url = window.pcmChunksToWavUrl(pcmAll);
      audio.src = url;
      download.href = url;
      download.download = `asr-llm-tts-${Date.now()}.wav`;
      download.hidden = false;
      audio.play().catch(() => {});
      setStatus(`✅ 完成 (LLM ${llmText.length} 字, ${chunkCount} chunks, ${totalBytes} 字节 PCM)`);
    } catch (e) {
      err('outer catch:', e);
      setError(e.message);
    } finally {
      btnRun.disabled = false;
      fileInput.disabled = false;
    }
  };
})();
