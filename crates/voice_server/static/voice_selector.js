// voice-ai frontend —— 音色下拉加载
// 页面加载时 fetch /admin/voices → 给 4 个 tab 各一个 <select> 填上选项 + 默认选中。
//
// 4 个 select 的 id：
//   - pipeline-voice        （全流程对话，WS SessionStart 携带）
//   - tts-voice             （/admin/tts JSON body）
//   - llm_tts-voice         （/admin/llm_tts JSON body）
//   - asr_llm_tts-voice     （/admin/asr_llm_tts query 参数）

(function () {
  'use strict';

  const SELECT_IDS = [
    'pipeline-voice',
    'tts-voice',
    'llm_tts-voice',
    'asr_llm_tts-voice',
  ];

  // 暴露给 tab 脚本读当前选中的短名（None = 用服务端默认）
  window.VoiceSelector = {
    /** 取出 select 当前值；空串 → null（让服务端走兜底） */
    getSelected(selectId) {
      const el = document.getElementById(selectId);
      if (!el) return null;
      const v = el.value;
      return v && v.length > 0 ? v : null;
    },
  };

  async function loadAndPopulate() {
    let data;
    try {
      const resp = await fetch('/admin/voices');
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      data = await resp.json();
    } catch (e) {
      console.warn('[voice-selector] 加载 /admin/voices 失败:', e);
      // 失败时把 select 改成"加载失败"状态，禁止选择
      SELECT_IDS.forEach((id) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.innerHTML = '<option value="">(加载失败)</option>';
        el.disabled = true;
      });
      return;
    }

    const voices = Array.isArray(data.voices) ? data.voices : [];
    const defaultVoice = data.default || '';
    if (voices.length === 0) {
      console.warn('[voice-selector] /admin/voices 返回空列表');
      return;
    }

    SELECT_IDS.forEach((id) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.innerHTML = '';
      // 按后端给的顺序填入；default 用后端返回的 default 字段标记选中
      for (const v of voices) {
        const opt = document.createElement('option');
        opt.value = v;
        opt.textContent = v;
        if (v === defaultVoice) opt.selected = true;
        el.appendChild(opt);
      }
      el.disabled = false;
      // pipeline tab 的 voice 名还要同步显示在中央舞台
      if (id === 'pipeline-voice') {
        const nameEl = document.getElementById('phone-voice-name');
        const sync = () => { if (nameEl) nameEl.textContent = el.value || '默认'; };
        el.addEventListener('change', sync);
        sync();
      }
    });
  }

  // 页面 DOMContentLoaded 后拉取（其他 JS 已经挂载好元素即可）
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', loadAndPopulate);
  } else {
    loadAndPopulate();
  }
})();
