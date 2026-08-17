// voice-app frontend — ASR 单能力 tab
// 选文件 → POST /admin/asr → 流式显示识别结果（音频格式由后端处理）

(function () {
  'use strict';

  const fileInput = document.getElementById('asr-file-input');
  const btnRun = document.getElementById('asr-run');
  const status = document.getElementById('asr-status');
  const output = document.getElementById('asr-output');

  const TAG = '[tab-asr]';
  const log = (...a) => console.log(TAG, ...a);
  const err = (...a) => console.error(TAG, ...a);

  let selectedFile = null;

  function resetOutput() {
    output.replaceChildren();
    const hint = document.createElement('span');
    hint.className = 'empty-hint';
    hint.textContent = '（无）';
    output.appendChild(hint);
  }

  function setStatus(text) { status.textContent = text; }

  function setError(message) {
    setStatus('❌ ' + message);
    const el = document.createElement('div');
    el.style.color = '#f87171';
    el.textContent = '❌ ' + message;
    output.appendChild(el);
  }

  fileInput.onchange = (e) => {
    selectedFile = e.target.files[0] || null;
    btnRun.disabled = !selectedFile;
    setStatus(selectedFile
      ? `已选择: ${selectedFile.name} (${selectedFile.size} bytes)`
      : '等待选择文件...');
    resetOutput();
    log('onchange:', selectedFile ? selectedFile.name : 'cleared');
  };

  btnRun.onclick = async () => {
    if (!selectedFile) return;
    btnRun.disabled = true;
    fileInput.disabled = true;
    output.replaceChildren();

    try {
      const buf = await selectedFile.arrayBuffer();
      log('upload start:', selectedFile.name, buf.byteLength, 'B');

      setStatus('调用 /admin/asr ...');
      const resp = await fetch('/admin/asr', {
        method: 'POST',
        headers: { 'Content-Type': 'application/octet-stream' },
        body: buf,
      });
      log('fetch response:', resp.status, resp.statusText);

      // 占位行（识别中...），后续每个 chunk 复用同一行替换内容
      const lineEl = document.createElement('div');
      lineEl.className = 'partial';
      lineEl.textContent = '… （识别中...）';
      output.appendChild(lineEl);

      let chunkCount = 0;
      for await (const evt of window.parseNdjson(resp)) {
        if (evt.error) {
          err('ASR error line:', evt);
          setError(evt.error);
          continue;
        }
        chunkCount++;
        lineEl.textContent = (evt.is_final ? '✓ ' : '… ') + evt.text;
        lineEl.className = evt.is_final ? 'final' : 'partial';
        setStatus(`已收 ${chunkCount} 个 chunk${evt.is_final ? ' (final)' : ''}`);
      }
      log('done, chunks =', chunkCount);
      setStatus(`✅ 完成 (${chunkCount} chunks)`);
    } catch (e) {
      err('outer catch:', e);
      setError(e.message);
    } finally {
      btnRun.disabled = false;
      fileInput.disabled = false;
    }
  };
})();
