//! 客户端 VAD（Voice Activity Detection）
//!
//! 服务端只认 `AudioChunk.is_last`，句尾由客户端决定（见 voice-server session.rs
//! 的"句尾判定"一节）。本 crate 提供 `VoiceActivityDetector` trait + 能量阈值 MVP
//! 实现 `EnergyVad`；后续 Silero VAD（ONNX）以另一个实现接入，调用侧不用改。
//!
//! 用法（20ms/帧，16kHz s16le mono，640 字节/帧）：
//! ```ignore
//! use voice_vad::{EnergyVad, EnergyVadConfig, VoiceActivityDetector, VadEvent};
//!
//! let mut vad = EnergyVad::new(EnergyVadConfig::default());
//! for frame in frames {
//!     match vad.process(&frame) {
//!         VadEvent::SpeechStart => { /* 从这一帧开始算有效语音 */ }
//!         VadEvent::SpeechEnd   => { /* 本帧是句尾，随 is_last=true 发出 */ }
//!         VadEvent::None        => {}
//!     }
//! }
//! // 流结束但句子没闭合（用户说到一半挂断）：
//! if vad.flush() { /* 补发一次 is_last=true */ }
//! ```
//!
//! 注意：`process` 只报事件，不缓存音频 —— 攒帧、发送节奏由调用侧控制。
//! SpeechStart 之前的帧是前置静音，调用侧可以选择丢弃。

/// 每帧 VAD 输出的事件
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VadEvent {
    /// 静音 / 句中，无事发生
    None,
    /// 语音开始：从本帧起是有效语音
    SpeechStart,
    /// 句尾：本帧是一句话的最后一帧，调用侧应随 `is_last=true` 发送
    SpeechEnd,
}

/// VAD 抽象：逐帧喂 PCM，返回该帧事件。
/// 实现要求 `Send`，方便放进采集线程。
pub trait VoiceActivityDetector: Send {
    /// 喂入一帧 PCM（s16le；采样率/帧长与配置假设一致即可）
    fn process(&mut self, frame: &[u8]) -> VadEvent;
    /// 流结束时强制收口：若仍处于句中返回 true，调用侧应补一次 `is_last=true`
    fn flush(&mut self) -> bool;
    /// 复位到静音状态（新会话复用同一实例）
    fn reset(&mut self);
}

/// 能量阈值 VAD 配置
#[derive(Debug, Clone)]
pub struct EnergyVadConfig {
    /// RMS 门限（s16 量纲，0~32768）：帧 RMS ≥ 门限判为浊音帧
    pub rms_threshold: f32,
    /// 连续多少个浊音帧确认语音开始（去抖：咳嗽/键盘单帧噪声不起句）
    pub start_frames: usize,
    /// 连续多少个静音帧确认句尾（通常对应 300~500ms 静音拖尾）
    pub end_frames: usize,
    /// 单句最长帧数，超过强制收尾（0 = 不限制；与服务端 MAX_UTTERANCE_MS 呼应）
    pub max_frames: usize,
}

impl Default for EnergyVadConfig {
    /// 默认值按 20ms/帧 假设：
    /// start 3 帧 = 60ms 去抖；end 20 帧 = 400ms 静音拖尾；max 1500 帧 = 30s
    fn default() -> Self {
        Self {
            rms_threshold: 300.0,
            start_frames: 3,
            end_frames: 20,
            max_frames: 1500,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    Speaking,
}

/// 能量阈值 VAD（MVP）。
///
/// 状态机：`Idle --连续 start_frames 个浊音帧--> Speaking`，
/// `Speaking --连续 end_frames 个静音帧 或 单句超 max_frames--> Idle + SpeechEnd`。
/// 句中短暂停顿（< end_frames）不会切断。
pub struct EnergyVad {
    cfg: EnergyVadConfig,
    phase: Phase,
    /// Idle 期间连续浊音帧数（起始去抖计数）
    voiced_run: usize,
    /// Speaking 期间连续静音帧数（句尾拖尾计数）
    silent_run: usize,
    /// 本句已持续帧数（含起始去抖段）
    frames_in_speech: usize,
}

impl EnergyVad {
    pub fn new(cfg: EnergyVadConfig) -> Self {
        Self {
            cfg,
            phase: Phase::Idle,
            voiced_run: 0,
            silent_run: 0,
            frames_in_speech: 0,
        }
    }

    fn end_speech(&mut self) {
        self.phase = Phase::Idle;
        self.voiced_run = 0;
        self.silent_run = 0;
        self.frames_in_speech = 0;
    }
}

impl Default for EnergyVad {
    fn default() -> Self {
        Self::new(EnergyVadConfig::default())
    }
}

impl VoiceActivityDetector for EnergyVad {
    fn process(&mut self, frame: &[u8]) -> VadEvent {
        let voiced = rms_s16le(frame) >= self.cfg.rms_threshold;
        match self.phase {
            Phase::Idle => {
                if voiced {
                    self.voiced_run += 1;
                    if self.voiced_run >= self.cfg.start_frames {
                        // 起始去抖段也算进本句时长
                        self.frames_in_speech = self.voiced_run;
                        self.voiced_run = 0;
                        self.silent_run = 0;
                        self.phase = Phase::Speaking;
                        VadEvent::SpeechStart
                    } else {
                        VadEvent::None
                    }
                } else {
                    self.voiced_run = 0;
                    VadEvent::None
                }
            }
            Phase::Speaking => {
                self.frames_in_speech += 1;
                if voiced {
                    self.silent_run = 0;
                } else {
                    self.silent_run += 1;
                }
                let timed_out = self.cfg.max_frames > 0
                    && self.frames_in_speech >= self.cfg.max_frames;
                if self.silent_run >= self.cfg.end_frames || timed_out {
                    self.end_speech();
                    VadEvent::SpeechEnd
                } else {
                    VadEvent::None
                }
            }
        }
    }

    fn flush(&mut self) -> bool {
        if self.phase == Phase::Speaking {
            self.end_speech();
            true
        } else {
            false
        }
    }

    fn reset(&mut self) {
        self.end_speech();
    }
}

/// 一帧 s16le PCM 的 RMS。奇数字节丢弃尾字节；空帧返回 0（按静音处理）。
fn rms_s16le(frame: &[u8]) -> f32 {
    let n = frame.len() / 2;
    if n == 0 {
        return 0.0;
    }
    let mut sum: f64 = 0.0;
    for i in 0..n {
        let s = i16::from_le_bytes([frame[2 * i], frame[2 * i + 1]]) as f64;
        sum += s * s;
    }
    (sum / n as f64).sqrt() as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试用小参数：start 2 帧、end 3 帧、单句上限 10 帧
    fn fast_cfg() -> EnergyVadConfig {
        EnergyVadConfig {
            rms_threshold: 300.0,
            start_frames: 2,
            end_frames: 3,
            max_frames: 10,
        }
    }

    /// 生成一帧 s16le 正弦（440Hz @ 16kHz），phase 跨帧续传保持连续
    fn tone_frame(samples: usize, amplitude: f64, phase: &mut f64) -> Vec<u8> {
        let mut out = Vec::with_capacity(samples * 2);
        for _ in 0..samples {
            let s = phase.sin() * amplitude;
            *phase += 2.0 * std::f64::consts::PI * 440.0 / 16000.0;
            out.extend_from_slice(&(s as i16).to_le_bytes());
        }
        out
    }

    fn silence_frame(samples: usize) -> Vec<u8> {
        vec![0u8; samples * 2]
    }

    #[test]
    fn silence_never_triggers() {
        let mut vad = EnergyVad::new(fast_cfg());
        for _ in 0..50 {
            assert_eq!(vad.process(&silence_frame(320)), VadEvent::None);
        }
        assert!(!vad.flush());
    }

    #[test]
    fn speech_start_then_end() {
        let mut vad = EnergyVad::new(fast_cfg());
        let mut phase = 0.0;
        // start_frames=2：第 2 个浊音帧出 SpeechStart
        assert_eq!(vad.process(&tone_frame(320, 5000.0, &mut phase)), VadEvent::None);
        assert_eq!(vad.process(&tone_frame(320, 5000.0, &mut phase)), VadEvent::SpeechStart);
        assert_eq!(vad.process(&tone_frame(320, 5000.0, &mut phase)), VadEvent::None);
        // end_frames=3：连续 3 个静音帧收尾
        assert_eq!(vad.process(&silence_frame(320)), VadEvent::None);
        assert_eq!(vad.process(&silence_frame(320)), VadEvent::None);
        assert_eq!(vad.process(&silence_frame(320)), VadEvent::SpeechEnd);
        assert!(!vad.flush());
    }

    #[test]
    fn short_noise_is_debounced() {
        let mut vad = EnergyVad::new(fast_cfg());
        let mut phase = 0.0;
        // 单帧噪声（< start_frames=2）后回静音：不应起句
        assert_eq!(vad.process(&tone_frame(320, 5000.0, &mut phase)), VadEvent::None);
        for _ in 0..5 {
            assert_eq!(vad.process(&silence_frame(320)), VadEvent::None);
        }
        assert!(!vad.flush());
    }

    #[test]
    fn mid_speech_pause_does_not_cut() {
        let mut vad = EnergyVad::new(fast_cfg());
        let mut phase = 0.0;
        vad.process(&tone_frame(320, 5000.0, &mut phase));
        assert_eq!(vad.process(&tone_frame(320, 5000.0, &mut phase)), VadEvent::SpeechStart);
        // 停顿 2 帧（< end_frames=3）不切断
        assert_eq!(vad.process(&silence_frame(320)), VadEvent::None);
        assert_eq!(vad.process(&silence_frame(320)), VadEvent::None);
        // 恢复说话：静音计数清零
        assert_eq!(vad.process(&tone_frame(320, 5000.0, &mut phase)), VadEvent::None);
        // 再静音满 3 帧才收尾
        assert_eq!(vad.process(&silence_frame(320)), VadEvent::None);
        assert_eq!(vad.process(&silence_frame(320)), VadEvent::None);
        assert_eq!(vad.process(&silence_frame(320)), VadEvent::SpeechEnd);
    }

    #[test]
    fn max_frames_force_end() {
        let mut vad = EnergyVad::new(fast_cfg()); // max_frames=10
        let mut phase = 0.0;
        let mut events: Vec<VadEvent> = Vec::new();
        // 一直说话不停：到帧数上限必须强制收尾，且收完还能再起句
        for _ in 0..30 {
            events.push(vad.process(&tone_frame(320, 5000.0, &mut phase)));
        }
        assert!(events.contains(&VadEvent::SpeechStart));
        assert!(events.contains(&VadEvent::SpeechEnd));
        // 30 帧 / 10 帧上限 → 至少收了三句
        assert_eq!(events.iter().filter(|e| **e == VadEvent::SpeechEnd).count(), 3);
    }

    #[test]
    fn flush_closes_open_speech() {
        let mut vad = EnergyVad::new(fast_cfg());
        let mut phase = 0.0;
        vad.process(&tone_frame(320, 5000.0, &mut phase));
        assert_eq!(vad.process(&tone_frame(320, 5000.0, &mut phase)), VadEvent::SpeechStart);
        assert!(vad.flush());
        assert!(!vad.flush());
    }

    #[test]
    fn empty_frame_counts_as_silence() {
        let mut vad = EnergyVad::new(fast_cfg());
        assert_eq!(vad.process(&[]), VadEvent::None);
    }

    #[test]
    fn reset_returns_to_idle() {
        let mut vad = EnergyVad::new(fast_cfg());
        let mut phase = 0.0;
        vad.process(&tone_frame(320, 5000.0, &mut phase));
        vad.process(&tone_frame(320, 5000.0, &mut phase)); // SpeechStart
        vad.reset();
        assert!(!vad.flush());
        // reset 后重新走 start_frames 去抖
        assert_eq!(vad.process(&tone_frame(320, 5000.0, &mut phase)), VadEvent::None);
    }
}