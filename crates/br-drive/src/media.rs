use serde::{Deserialize, Serialize};

pub const MAX_MEDIA_TYPE_BYTES: usize = 255;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid_media_type")]
pub struct InvalidMediaType;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MediaType(String);

fn token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '!' | '#' | '$' | '&' | '-' | '^' | '_' | '.' | '+')
}

fn token(value: &str) -> bool {
    !value.is_empty() && value.chars().all(token_char)
}

impl MediaType {
    pub fn parse(raw: &str) -> Result<Self, InvalidMediaType> {
        if raw.len() > MAX_MEDIA_TYPE_BYTES {
            return Err(InvalidMediaType);
        }
        let (kind, subtype) = raw.split_once('/').ok_or(InvalidMediaType)?;
        if !token(kind) || !token(subtype) {
            return Err(InvalidMediaType);
        }
        Ok(Self(raw.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_type_subtype_token_pair_parses_and_is_lowercased() {
        assert_eq!(
            MediaType::parse("Text/Plain").unwrap().as_str(),
            "text/plain"
        );
        assert!(MediaType::parse("application/vnd.ms-excel").is_ok());
        assert!(MediaType::parse("image/svg+xml").is_ok());
    }

    #[test]
    fn a_missing_slash_a_parameter_or_a_space_is_refused() {
        for raw in [
            "",
            "text",
            "text/",
            "/plain",
            "text/plain; charset=utf-8",
            "text plain",
            "a/b/c",
            &format!("text/{}", "x".repeat(255)),
        ] {
            assert_eq!(MediaType::parse(raw), Err(InvalidMediaType), "{raw:?}");
        }
    }
}
