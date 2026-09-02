use serde::Deserialize;

const PROMPT_YAML: &str = include_str!("prompt.yaml");

#[derive(Debug, Clone, Deserialize)]
pub struct LlmPromptTemplates {
    pub fast: String,
    pub strong: String,
}

impl LlmPromptTemplates {
    pub fn from_embedded() -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(PROMPT_YAML)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_templates_are_complete_and_distinct() {
        let prompts = LlmPromptTemplates::from_embedded().unwrap();

        assert!(prompts.fast.starts_with("/no_think"));
        assert!(prompts.fast.contains("不要尝试调用工具"));
        assert!(!prompts.fast.contains("工具调用前校验"));
        assert!(prompts.strong.contains("强执行模型"));
        assert!(prompts.strong.contains("工具调用前校验"));
        assert!(!prompts.strong.starts_with("/no_think"));
        assert_eq!(prompts.fast.matches("{emotion}").count(), 1);
        assert_eq!(prompts.strong.matches("{emotion}").count(), 1);
    }
}
