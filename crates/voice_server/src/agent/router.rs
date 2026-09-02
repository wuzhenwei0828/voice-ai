use crate::client::llm::ModelTier;

pub const DEFAULT_STRONG_MIN_CHARS: usize = 15;

#[derive(Clone, Copy, Debug, Default)]
pub struct ModelRouter;

impl ModelRouter {
    pub fn route(&self, input: &str) -> ModelTier {
        if input.trim().chars().count() < DEFAULT_STRONG_MIN_CHARS {
            ModelTier::Fast
        } else {
            ModelTier::Strong
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_by_trimmed_unicode_character_count() {
        let router = ModelRouter;

        assert_eq!(router.route(""), ModelTier::Fast);
        assert_eq!(router.route("   "), ModelTier::Fast);
        assert_eq!(
            router.route("一二三四五六七八九十甲乙丙丁"),
            ModelTier::Fast
        );
        assert_eq!(
            router.route("一二三四五六七八九十甲乙丙丁戊"),
            ModelTier::Strong
        );
        assert_eq!(
            router.route("  一二三四五六七八九十甲乙丙丁  "),
            ModelTier::Fast
        );
    }

    #[test]
    fn counts_unicode_characters_instead_of_utf8_bytes() {
        let router = ModelRouter;

        assert_eq!(router.route("你好🙂，abc"), ModelTier::Fast);
        assert_eq!(router.route("你好🙂，abcdefghijk"), ModelTier::Strong);
    }
}
