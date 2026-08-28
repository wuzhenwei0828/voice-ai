// voice-app frontend — LLM 单能力 tab
// 输入 prompt → POST /admin/llm → 流式文本回复

(function () {
  'use strict';

  const promptEl = document.getElementById('llm-prompt');
  const btnRun = document.getElementById('llm-run');
  const status = document.getElementById('llm-status');
  const output = document.getElementById('llm-output');

  btnRun.onclick = async () => {
    const prompt = promptEl.value.trim();
    if (!prompt) {
      status.textContent = '⚠️ 请先输入 prompt';
      return;
    }
    btnRun.disabled = true;
    promptEl.disabled = true;
    output.innerHTML = '';
    try {
      status.textContent = '调用 /admin/llm ...';
      const resp = await fetch('/admin/llm', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt }),
      });

      output.innerHTML = '<span class="empty-hint">（流式中...）</span>';
      let text = '';
      let chunkCount = 0;
      let finalEl = null;
      for await (const evt of window.parseSse(resp)) {
        if (evt.error) {
          output.innerHTML += `<div class="final" style="color:#f87171">❌ ${evt.error}</div>`;
          continue;
        }
        chunkCount++;
        text += evt.delta;
        output.innerHTML = '';
        const div = document.createElement('div');
        if (evt.is_final) {
          div.className = 'final';
          div.textContent = text;
          finalEl = div;
        } else {
          div.className = 'partial';
          div.textContent = text + ' ▍';
        }
        output.appendChild(div);
        status.textContent = `已收 ${chunkCount} 个 delta${evt.is_final ? ' (final)' : ''}`;
      }
      status.textContent = `✅ 完成 (${chunkCount} deltas)`;
    } catch (e) {
      output.innerHTML += `<div class="final" style="color:#f87171">❌ ${e.message}</div>`;
      status.textContent = '❌ ' + e.message;
    } finally {
      btnRun.disabled = false;
      promptEl.disabled = false;
    }
  };
})();
