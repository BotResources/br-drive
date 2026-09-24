use serde::{Deserialize, Serialize};

use crate::path::FileName;

pub const MAX_TITLE_CHARS: usize = 255;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid_title")]
pub struct InvalidTitle;

/// A file's human-facing title: trimmed, 1 to 255 characters, no control
/// character. Independent of the file's name — renaming never retitles, and
/// retitling never moves.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileTitle(String);

impl FileTitle {
    pub fn parse(raw: &str) -> Result<Self, InvalidTitle> {
        let title = raw.trim();
        if title.is_empty()
            || title.chars().count() > MAX_TITLE_CHARS
            || title.chars().any(char::is_control)
        {
            return Err(InvalidTitle);
        }
        Ok(Self(title.to_string()))
    }

    /// The default title of an uploaded file: its name without the extension
    /// (`report.pdf` → `report`); a name with no stem (`.profile`) is kept whole.
    pub fn from_name(name: &FileName) -> Self {
        let name = name.as_str();
        let stem = match name.rfind('.') {
            Some(dot) if dot > 0 => &name[..dot],
            _ => name,
        };
        Self::parse(stem).unwrap_or_else(|_| Self(name.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(raw: &str) -> FileName {
        FileName::parse(raw).expect("a sound name")
    }

    #[test]
    fn the_default_title_is_the_name_without_its_extension() {
        assert_eq!(FileTitle::from_name(&name("report.pdf")).as_str(), "report");
        assert_eq!(FileTitle::from_name(&name("a.tar.gz")).as_str(), "a.tar");
        assert_eq!(FileTitle::from_name(&name("README")).as_str(), "README");
        assert_eq!(FileTitle::from_name(&name(".profile")).as_str(), ".profile");
        assert_eq!(FileTitle::from_name(&name("notes .txt")).as_str(), "notes");
    }

    #[test]
    fn a_title_is_trimmed_bounded_in_characters_and_free_of_control_characters() {
        assert_eq!(
            FileTitle::parse("  Q3 review ").unwrap().as_str(),
            "Q3 review"
        );
        assert!(FileTitle::parse(&"é".repeat(255)).is_ok());
        assert_eq!(FileTitle::parse(&"é".repeat(256)), Err(InvalidTitle));
        assert_eq!(FileTitle::parse("   "), Err(InvalidTitle));
        assert_eq!(FileTitle::parse("a\nb"), Err(InvalidTitle));
    }
}
