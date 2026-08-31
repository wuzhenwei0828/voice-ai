import type { Settings } from '../services/settings-service';
const VOICES = ['', 'aiden', 'alex', 'anna', 'bella', 'benjamin', 'charles', 'claire', 'david', 'diana', 'dylan', 'eric', 'ono_anna', 'ryan', 'serena', 'sohee', 'uncle_fu', 'vivian'];
export function SettingsPanel({ settings, onChange }: { settings: Settings; onChange: (next: Settings) => void }) {
  return <div className="phone-settings-body">
    <label className="voice-selector">TTS 音色：<select value={settings.voice ?? ''} onChange={(event) => onChange({ ...settings, voice: event.target.value })}>{VOICES.map((voice) => <option value={voice} key={voice}>{voice || '默认'}</option>)}</select></label>
    <label>服务地址<input value={settings.baseUrl} onChange={(event) => onChange({ ...settings, baseUrl: event.target.value })} placeholder="http://127.0.0.1:8080" /></label>
    <label>访问令牌<input type="password" value={settings.token} onChange={(event) => onChange({ ...settings, token: event.target.value })} placeholder="可选" /></label>
    <div className="hint">桌面端需授权麦克风；开始对话后持续收音并自动检测句尾。音色留空时使用服务端默认配置。</div>
  </div>;
}
