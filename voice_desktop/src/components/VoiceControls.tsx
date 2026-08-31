export function VoiceControls({ active, onStart, onInterrupt, onStop }: { active: boolean; onStart: () => void; onInterrupt: () => void; onStop: () => void }) {
  return <div className="phone-controls" aria-label="通话控制">
    <button className="phone-btn phone-btn-interrupt" onClick={onInterrupt} disabled={!active}>打断</button>
    <button className="phone-btn phone-btn-start" onClick={onStart} disabled={active} title="开始对话" aria-label="开始对话"><span className="phone-btn-icon" aria-hidden="true">📞</span></button>
    <button className="phone-btn phone-btn-stop" onClick={onStop} disabled={!active} title="结束" aria-label="结束"><span className="phone-btn-icon" aria-hidden="true">✕</span></button>
  </div>;
}
