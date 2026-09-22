use service_engine::gate::Reason;

use crate::fault::codes;

pub const MAX_SEGMENT_BYTES: usize = 255;
pub const MAX_PATH_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    #[error("invalid_path")]
    InvalidPath,
    #[error("invalid_name")]
    InvalidName,
}

impl From<PathError> for Reason {
    fn from(error: PathError) -> Self {
        match error {
            PathError::InvalidPath => codes::INVALID_PATH,
            PathError::InvalidName => codes::INVALID_NAME,
        }
    }
}

fn segment_is_sound(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && segment.len() <= MAX_SEGMENT_BYTES
        && segment.trim() == segment
        && !segment.chars().any(|c| c.is_control() || c == '\\')
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DrivePath(String);

impl DrivePath {
    pub fn root() -> Self {
        Self(String::new())
    }

    pub fn parse(raw: &str) -> Result<Self, PathError> {
        let trimmed = raw.trim_matches('/');
        if trimmed.is_empty() {
            return Ok(Self::root());
        }
        if trimmed.len() > MAX_PATH_BYTES || !trimmed.split('/').all(segment_is_sound) {
            return Err(PathError::InvalidPath);
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn is_within(&self, prefix: &DrivePath) -> bool {
        prefix.is_root()
            || self == prefix
            || self
                .0
                .strip_prefix(prefix.as_str())
                .is_some_and(|rest| rest.starts_with('/'))
    }

    pub fn rebased(&self, from: &DrivePath, to: &DrivePath) -> Option<DrivePath> {
        if !self.is_within(from) {
            return None;
        }
        let rest = if from.is_root() {
            self.0.as_str()
        } else {
            self.0[from.0.len()..].trim_start_matches('/')
        };
        let landed = match (to.is_root(), rest.is_empty()) {
            (true, _) => rest.to_string(),
            (false, true) => to.0.clone(),
            (false, false) => format!("{}/{rest}", to.0),
        };
        (landed.len() <= MAX_PATH_BYTES).then_some(DrivePath(landed))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileName(String);

impl FileName {
    pub fn parse(raw: &str) -> Result<Self, PathError> {
        if raw.contains('/') || !segment_is_sound(raw) {
            return Err(PathError::InvalidName);
        }
        Ok(Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn first_free(&self, taken: &[String]) -> FileName {
        if !taken.iter().any(|name| name == &self.0) {
            return self.clone();
        }
        let (stem, extension) = split_extension(&self.0);
        let stem = strip_counter(stem);
        (1..)
            .map(|n| format!("{stem} ({n}){extension}"))
            .find(|candidate| !taken.iter().any(|name| name == candidate))
            .map(FileName)
            .expect("the counter sequence is unbounded, so a free name always exists")
    }
}

fn split_extension(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(dot) if dot > 0 => name.split_at(dot),
        _ => (name, ""),
    }
}

fn strip_counter(stem: &str) -> &str {
    let Some(open) = stem.rfind(" (") else {
        return stem;
    };
    let counter = &stem[open + 2..];
    match counter.strip_suffix(')') {
        Some(digits) if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) => {
            &stem[..open]
        }
        _ => stem,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(raw: &str) -> DrivePath {
        DrivePath::parse(raw).expect("a sound path")
    }

    #[test]
    fn the_root_is_the_empty_path_and_slashes_around_a_path_are_normalized_away() {
        assert!(path("").is_root());
        assert!(path("/").is_root());
        assert_eq!(path("/a/b/").as_str(), "a/b");
    }

    #[test]
    fn an_empty_dot_padded_or_over_long_segment_is_refused() {
        for raw in [
            "a//b",
            "a/./b",
            "../a",
            "a/..",
            "a/ b",
            "a /b",
            "a/ /b",
            &format!("a/{}", "x".repeat(256)),
        ] {
            assert_eq!(DrivePath::parse(raw), Err(PathError::InvalidPath), "{raw}");
        }
        assert!(DrivePath::parse(&"x".repeat(255)).is_ok());
        assert!(DrivePath::parse("a b/c d").is_ok());
        assert_eq!(DrivePath::parse("a\u{0}b"), Err(PathError::InvalidPath));
    }

    #[test]
    fn the_whole_path_is_capped_at_parse_and_at_rebase() {
        let segment = "s".repeat(200);
        let five = [segment.as_str(); 5].join("/");
        assert_eq!(five.len(), 1004);
        assert!(DrivePath::parse(&five).is_ok());
        let six = [segment.as_str(); 6].join("/");
        assert_eq!(DrivePath::parse(&six), Err(PathError::InvalidPath));
        let deep = path(&[segment.as_str(); 4].join("/"));
        assert!(path(&five).rebased(&path(&segment), &deep).is_none());
    }

    #[test]
    fn a_name_carries_no_slash_no_padding_and_is_never_a_dot_segment() {
        assert!(FileName::parse("report.pdf").is_ok());
        assert!(FileName::parse("my report.pdf").is_ok());
        assert_eq!(FileName::parse("a/b"), Err(PathError::InvalidName));
        assert_eq!(FileName::parse(""), Err(PathError::InvalidName));
        assert_eq!(FileName::parse(".."), Err(PathError::InvalidName));
        assert_eq!(FileName::parse(" padded"), Err(PathError::InvalidName));
        assert_eq!(FileName::parse("a\tb"), Err(PathError::InvalidName));
    }

    #[test]
    fn within_holds_for_the_folder_itself_and_its_descendants_only() {
        assert!(path("a/b").is_within(&path("a")));
        assert!(path("a").is_within(&path("a")));
        assert!(path("a/b").is_within(&DrivePath::root()));
        assert!(!path("ab").is_within(&path("a")));
        assert!(!path("b/a").is_within(&path("a")));
    }

    #[test]
    fn rebasing_moves_the_tail_under_the_new_prefix() {
        assert_eq!(
            path("a/b/c").rebased(&path("a"), &path("z")),
            Some(path("z/b/c"))
        );
        assert_eq!(
            path("a").rebased(&path("a"), &path("z/y")),
            Some(path("z/y"))
        );
        assert_eq!(
            path("a/b").rebased(&path("a"), &DrivePath::root()),
            Some(path("b"))
        );
        assert_eq!(path("q").rebased(&path("a"), &path("z")), None);
    }

    #[test]
    fn a_taken_name_gets_a_counter_before_its_extension() {
        let taken = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let name = FileName::parse("report.pdf").unwrap();
        assert_eq!(name.first_free(&taken(&[])).as_str(), "report.pdf");
        assert_eq!(
            name.first_free(&taken(&["report.pdf"])).as_str(),
            "report (1).pdf"
        );
        assert_eq!(
            name.first_free(&taken(&["report.pdf", "report (1).pdf"]))
                .as_str(),
            "report (2).pdf"
        );
        let counted = FileName::parse("report (1).pdf").unwrap();
        assert_eq!(
            counted
                .first_free(&taken(&["report (1).pdf", "report (2).pdf"]))
                .as_str(),
            "report (3).pdf"
        );
        let dotfile = FileName::parse(".env").unwrap();
        assert_eq!(dotfile.first_free(&taken(&[".env"])).as_str(), ".env (1)");
        let bare = FileName::parse("notes").unwrap();
        assert_eq!(bare.first_free(&taken(&["notes"])).as_str(), "notes (1)");
    }
}
