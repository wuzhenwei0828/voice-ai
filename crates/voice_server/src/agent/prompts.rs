//! LLM Agent 提示词模板加载
//!
//! - 模板源：[`prompts.yaml`]（编译期 `include_str!` 嵌入）
//! - 反序列化：[`AgentPrompts`]（serde_yaml）
//! - 默认实例：[`default_prompts`]（首次调用时懒加载，全进程共享一份 `Arc`）
//!
//! ## YAML 结构（与 Rust struct 一一对应）
//! ```yaml
//! llmagent:
//!   chat:
//!     systemprompts:
//!       role: |
//!         ...静态：角色定义...
//!       guidelines: |
//!         ...静态：行为准则...
//!       emotion_hint: |
//!         ...动态：情绪提示（{emotion} 占位符）...
//! ```
//!
//! ## 静态 vs 动态
//! - **静态**（`role` / `guidelines`）：无占位符，每次 [`crate::agent::LlmAgent::chat`]
//!   都自动拼到 messages 最前面作为 system message
//! - **动态**（`emotion_hint`）：带 `{emotion}` 占位符，仅当调用方传入 emotion_hint 时才注入
//!
//! `systemprompts` 是 container，未来加新提示词时在 yaml 里平铺为同级字段、
//! Rust 这边在 [`SystemPromptsSection`] 加对应字段即可。
//!
//! ## 替换 / 注入
//! - 默认使用 `prompts.yaml` —— 改 yaml、重编即可生效
//! - 测试或外部 override：用 [`AgentPrompts::from_yaml`] / [`AgentPrompts::from_str`] 解析，
//!   再通过 [`crate::agent::LlmAgent::with_prompts`] 注入

use std::sync::{Arc, OnceLock};

use serde::Deserialize;

/// 编译期嵌入的 yaml 文本。改 `prompts.yaml` 后需要重新编译才会生效。
const PROMPTS_YAML: &str = include_str!("prompts.yaml");

/// 顶层提示词配置（对应 yaml 顶层）。当前唯一持有 `llmagent.chat` 这一段；
/// 未来若加 `tts.*` 或 `asr.*` 等其他命名空间，扩展为同级字段即可。
#[derive(Debug, Clone, Deserialize)]
pub struct AgentPrompts {
    pub llmagent: LlmAgentSection,
}

/// `llmagent` 命名空间下的提示词。
#[derive(Debug, Clone, Deserialize)]
pub struct LlmAgentSection {
    pub chat: ChatSection,
}

/// `llmagent.chat` —— 聊天场景相关提示词。
#[derive(Debug, Clone, Deserialize)]
pub struct ChatSection {
    /// systemprompts container。所有 system message 模板都作为同级字段挂这里。
    pub systemprompts: SystemPromptsSection,
}

/// 所有 system message 模板都挂这里。
/// - 静态字段（`role` / `guidelines`）：无占位符，每次 chat 都注入到 messages 头部
/// - 动态字段（`emotion_hint`）：带 `{emotion}` 占位符，条件注入
/// 加新提示词时平铺在本结构体上即可。
#[derive(Debug, Clone, Deserialize)]
pub struct SystemPromptsSection {
    /// 【静态】角色定义 —— 告诉 LLM 它是谁 / 在和谁对话
    pub role: String,
    /// 【静态】行为准则 —— 回答规范（长度 / 语言 / 准确性 / 身份）
    pub guidelines: String,
    /// 【动态】情绪提示 system message 模板。`{emotion}` 会被 emotion_hint 参数替换。
    pub emotion_hint: String,
}

impl AgentPrompts {
    /// 从 yaml 文本解析。用于测试 / 外部 override。
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    /// 从内置的 `prompts.yaml` 解析。
    pub fn from_embedded() -> Result<Self, serde_yaml::Error> {
        Self::from_yaml(PROMPTS_YAML)
    }

    /// 拼接所有静态提示词（`role` + `guidelines`），返回一条完整的 system message。
    /// `role` 和 `guidelines` 都为空时返回 None（调用方据此决定不插入 system message）。
    ///
    /// 便捷方法，内部委托到 [`SystemPromptsSection::static_system_message`]，
    /// 这样调用方（[`crate::agent::LlmAgent::chat`]）不用关心 yaml 嵌套层级。
    pub fn static_system_message(&self) -> Option<String> {
        self.llmagent.chat.systemprompts.static_system_message()
    }

    /// 用给定的情绪标签渲染 emotion_hint 模板。
    /// `emotion` 为空字符串时返回 None（调用方据此决定是否插入 system message）。
    ///
    /// 便捷方法，内部委托到 [`SystemPromptsSection::render_emotion_hint`]。
    pub fn render_emotion_hint(&self, emotion: &str) -> Option<String> {
        self.llmagent.chat.systemprompts.render_emotion_hint(emotion)
    }
}

impl SystemPromptsSection {
    /// 拼接所有静态提示词（`role` + `guidelines`），返回一条完整的 system message。
    /// 两者都为空时返回 None。
    pub fn static_system_message(&self) -> Option<String> {
        let mut parts: Vec<&str> = Vec::new();
        let role = self.role.trim();
        let guidelines = self.guidelines.trim();
        if !role.is_empty() {
            parts.push(role);
        }
        if !guidelines.is_empty() {
            parts.push(guidelines);
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("\n\n"))
        }
    }

    /// 渲染 emotion_hint 模板：`{emotion}` → 给定的标签。空标签返回 None。
    pub fn render_emotion_hint(&self, emotion: &str) -> Option<String> {
        if emotion.is_empty() {
            return None;
        }
        Some(self.emotion_hint.replace("{emotion}", emotion))
    }
}

/// 全进程共享一份默认 [`AgentPrompts`]。懒加载：首次调用时反序列化 yaml。
pub(crate) fn default_prompts() -> Arc<AgentPrompts> {
    static CACHE: OnceLock<Arc<AgentPrompts>> = OnceLock::new();
    CACHE
        .get_or_init(|| {
            // yaml 编译期受控，反序列化失败 = 程序员 bug，panic 是合适的
            Arc::new(AgentPrompts::from_embedded().expect("agent/prompts.yaml 解析失败"))
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_yaml_parses_with_systemprompts_layer() {
        let p = AgentPrompts::from_embedded().expect("内置 yaml 必须能解析");
        // 防回归：所有字段必须存在
        let sp = &p.llmagent.chat.systemprompts;
        assert!(!sp.role.is_empty(), "role 字段不能为空");
        assert!(!sp.guidelines.is_empty(), "guidelines 字段不能为空");
        assert!(
            sp.emotion_hint.contains("{emotion}"),
            "emotion_hint 模板里缺 {{emotion}} 占位符：{}",
            sp.emotion_hint
        );
    }

    #[test]
    fn render_substitutes_emotion() {
        let p = AgentPrompts::from_embedded().unwrap();
        let out = p.render_emotion_hint("开心").unwrap();
        assert!(out.contains("开心"));
        assert!(!out.contains("{emotion}"));
    }

    #[test]
    fn render_empty_emotion_returns_none() {
        let p = AgentPrompts::from_embedded().unwrap();
        assert!(p.render_emotion_hint("").is_none());
    }

    #[test]
    fn static_message_combines_role_and_guidelines() {
        let p = AgentPrompts::from_embedded().unwrap();
        let msg = p.static_system_message().expect("默认 yaml 里 role/guidelines 都应有内容");
        // role + guidelines 应该都在
        assert!(msg.contains("语音助手"), "static message 应包含 role 内容，实际：{msg}");
        assert!(
            msg.contains("简洁") || msg.contains("自然"),
            "static message 应包含 guidelines 内容，实际：{msg}"
        );
        // role 和 guidelines 之间应有分隔（"\n\n"）
        assert!(msg.contains("\n\n"), "role 和 guidelines 之间应有空行分隔");
    }

    #[test]
    fn static_message_none_when_both_empty() {
        let yaml = r#"
            llmagent:
              chat:
                systemprompts:
                  role: ""
                  guidelines: ""
                  emotion_hint: |
                    [情绪] {emotion}
        "#;
        let p = AgentPrompts::from_yaml(yaml).unwrap();
        assert!(
            p.static_system_message().is_none(),
            "role + guidelines 都为空时应返回 None"
        );
    }

    #[test]
    fn static_message_only_role_when_guidelines_empty() {
        let yaml = r#"
            llmagent:
              chat:
                systemprompts:
                  role: "我是助手"
                  guidelines: ""
                  emotion_hint: |
                    [情绪] {emotion}
        "#;
        let p = AgentPrompts::from_yaml(yaml).unwrap();
        let msg = p.static_system_message().unwrap();
        assert_eq!(msg, "我是助手");
    }

    #[test]
    fn override_via_str_with_current_layout() {
        // 外部 override 场景：测试 / 灰度 —— 必须按当前结构写
        let custom = r#"
            llmagent:
              chat:
                systemprompts:
                  role: "test-role"
                  guidelines: "test-guidelines"
                  emotion_hint: |
                    [情绪] 用户当前情绪：{emotion}（测试模板）
        "#;
        let p = AgentPrompts::from_yaml(custom).unwrap();
        assert!(p.static_system_message().unwrap().contains("test-role"));
        let out = p.render_emotion_hint("焦虑").unwrap();
        assert!(out.contains("焦虑"));
        assert!(out.contains("测试模板"));
    }

    #[test]
    fn flat_layout_is_rejected() {
        // 防回归：扁平 yaml（直接 `emotion_hint: ...`）必须报错
        let flat = "emotion_hint: |\n  x\n";
        let res = AgentPrompts::from_yaml(flat);
        assert!(
            res.is_err(),
            "扁平 yaml 必须被拒绝（当前结构需要 llmagent.chat.systemprompts.*）"
        );
    }

    #[test]
    fn systemprompts_collapsed_layout_is_rejected() {
        // 防回归：之前误删 systemprompts container 的版本必须报错
        let collapsed = "llmagent:\n  chat:\n    emotion_hint: |\n      x\n";
        let res = AgentPrompts::from_yaml(collapsed);
        assert!(
            res.is_err(),
            "systemprompts container 是必需的，不能直接挂到 chat 下"
        );
    }

    #[test]
    fn missing_static_field_is_rejected() {
        // 防回归：systemprompts 下必须同时有 role + guidelines + emotion_hint，
        // 漏一个直接报错（避免 yaml 漏字段后悄悄回退到 None）
        let missing_role = r#"
            llmagent:
              chat:
                systemprompts:
                  guidelines: "x"
                  emotion_hint: "x"
        "#;
        assert!(AgentPrompts::from_yaml(missing_role).is_err());
    }

    #[test]
    fn default_prompts_returns_same_arc() {
        // 懒加载语义：多次调用返回同一份 Arc
        let a = default_prompts();
        let b = default_prompts();
        assert!(Arc::ptr_eq(&a, &b));
    }
}
