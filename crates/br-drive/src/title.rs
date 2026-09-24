use crate::path::FileName;

pub const MAX_TITLE_CHARS: usize = 255;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid_title")]
pub struct InvalidTitle;

/// A file's human-facing title: trimmed, 1 to 255 characters, one line, no
/// control or bidirectional-override character. Independent of the file's
/// name — renaming never retitles, and retitling never moves. Built only by
/// `parse` and `from_name`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileTitle(String);

/// Characters a one-line human title never holds: line and paragraph
/// separators, and the bidirectional embeddings, overrides and isolates that
/// make a displayed title read otherwise than it is stored.
fn is_forbidden(c: char) -> bool {
    c.is_control()
        || matches!(c, '\u{2028}' | '\u{2029}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}')
}

impl FileTitle {
    pub fn parse(raw: &str) -> Result<Self, InvalidTitle> {
        let title = raw.trim();
        if title.is_empty()
            || title.chars().count() > MAX_TITLE_CHARS
            || title.chars().any(is_forbidden)
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
        // A sound name has no leading space nor control character, so its
        // stem is always a sound title.
        Self::parse(stem).expect("a sound file name always has a sound stem")
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
        for smuggled in [
            "line\u{2028}break",
            "para\u{2029}graph",
            "invoice\u{202E}fdp.exe",
            "a\u{2066}b",
        ] {
            assert_eq!(
                FileTitle::parse(smuggled),
                Err(InvalidTitle),
                "{smuggled:?}"
            );
        }
    }

    #[test]
    fn every_sound_name_has_a_sound_default_title() {
        for raw in [
            ". .txt",
            "...",
            "x\u{3000}.pdf",
            "report\u{00A0}.pdf",
            "a.b.c",
        ] {
            let title = FileTitle::from_name(&name(raw));
            assert_eq!(
                FileTitle::parse(title.as_str()).as_ref(),
                Ok(&title),
                "{raw:?}"
            );
        }
        assert_eq!(FileTitle::from_name(&name("x\u{3000}.pdf")).as_str(), "x");
    }
}
