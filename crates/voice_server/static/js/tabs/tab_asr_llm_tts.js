// voice-ai frontend — ASR→LLM→TTS 全链路 tab
// 选文件 → POST /admin/asr_llm_tts → 按 stage 分发渲染 + TTS chunk 流式播放

(function () {
  'use strict';

  const fileInput = document.getElementById('asr_llm_tts-file-input');
  const btnRun = document.getElementById('asr_llm_tts-run');
  const status = document.getElementById('asr_llm_tts-status');
  const asrOut = document.getElementById('asr_llm_tts-asr-output');
  const llmOut = document.getElementById('asr_llm_tts-llm-output');
  const download = document.getElementById('asr_llm_tts-download');
  const streamPlayer = new window.PcmStreamPlayer();
  const playback = window.bindPcmStreamPlaybackToggle(streamPlayer, document.getElementById('asr_llm_tts-stream-toggle'));

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
    streamPlayer.stop();
    playback.clearReplay();
    [asrOut, llmOut].forEach((box) => {
      box.replaceChildren();
      const hint = document.createElement('span');
      hint.className = 'empty-hint';
      hint.textContent = '（无）';
      box.appendChild(hint);
    });
    if (download.href.startsWith('blob:')) URL.revokeObjectURL(download.href);
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
    const voice = window.VoiceSelector.getSelected('asr_llm_tts-voice');
    btnRun.disabled = true;
    fileInput.disabled = true;
    reset();
    void streamPlayer.resume().catch(() => {});

    try {
      const format = await window.loadTtsAudioFormat;
      const buf = await selectedFile.arrayBuffer();
      log('upload start:', selectedFile.name, buf.byteLength, 'B');

      setStatus('调用 /admin/asr_llm_tts ...');
      // /admin/asr_llm_tts 是裸 PCM body，voice 走 query 参数
      const url = voice
        ? `/admin/asr_llm_tts?voice=${encodeURIComponent(voice)}`
        : '/admin/asr_llm_tts';
      const resp = await fetchWithTrace(url, {
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

      for await (const evt of window.parseSse(resp)) {
        if (evt.error) {
          err('stream error line:', evt);
          streamPlayer.stop();
          playback.clearReplay();
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
            const bytes = window.base64ToBytes(evt.audio);
            streamPlayer.enqueue(bytes, format.sampleRate, format.channels);
            chunks.push(bytes);
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

      const wavUrl = window.pcmChunksToWavUrl(pcmAll, format.sampleRate, format.channels);
      playback.setReplay(wavUrl);
      download.href = wavUrl;
      download.download = `asr-llm-tts-${Date.now()}.wav`;
      download.hidden = false;
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
