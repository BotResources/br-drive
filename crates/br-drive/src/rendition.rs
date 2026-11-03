//! The name and the kind of a file rendition: a binary file a runner derived
//! from the source and handed back, stored beside it.

use serde::{Deserialize, Serialize};

pub const MAX_RENDITION_NAME_BYTES: usize = 64;
pub const MAX_RENDITION_KIND_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid_rendition_name")]
pub struct InvalidRenditionName;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid_rendition_kind")]
pub struct InvalidRenditionKind;

/// A rendition's name, unique per file: `[a-z0-9][a-z0-9._-]*`, 1 to
/// `MAX_RENDITION_NAME_BYTES` bytes (`base.duckdb`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RenditionName(String);

impl RenditionName {
    pub fn parse(raw: &str) -> Result<Self, InvalidRenditionName> {
        let mut bytes = raw.bytes();
        let sound = raw.len() <= MAX_RENDITION_NAME_BYTES
            && bytes
                .next()
                .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            && bytes.all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
            });
        if sound {
            Ok(Self(raw.to_string()))
        } else {
            Err(InvalidRenditionName)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What a rendition is, as a code a host and a runner agree on:
/// `[a-z][a-z0-9_]*`, 1 to `MAX_RENDITION_KIND_BYTES` bytes (`duckdb`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RenditionKind(String);

impl RenditionKind {
    pub fn parse(raw: &str) -> Result<Self, InvalidRenditionKind> {
        let mut bytes = raw.bytes();
        let sound = raw.len() <= MAX_RENDITION_KIND_BYTES
            && bytes.next().is_some_and(|b| b.is_ascii_lowercase())
            && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if sound {
            Ok(Self(raw.to_string()))
        } else {
            Err(InvalidRenditionKind)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rendition_name_is_lowercase_with_dots_dashes_and_underscores() {
        for good in ["base.duckdb", "a", "0", "v2_export-final.parquet", "x.y.z"] {
            assert!(RenditionName::parse(good).is_ok(), "{good}");
        }
        assert!(RenditionName::parse(&"a".repeat(MAX_RENDITION_NAME_BYTES)).is_ok());
        for bad in [
            "",
            ".hidden",
            "-dash",
            "_under",
            "Base.duckdb",
            "a b",
            "a/b",
            "é.duckdb",
            &"a".repeat(MAX_RENDITION_NAME_BYTES + 1),
        ] {
            assert_eq!(
                RenditionName::parse(bad),
                Err(InvalidRenditionName),
                "{bad}"
            );
        }
    }

    #[test]
    fn a_rendition_kind_is_a_lower_snake_code() {
        for good in ["duckdb", "parquet", "pdf_a", "v2"] {
            assert!(RenditionKind::parse(good).is_ok(), "{good}");
        }
        assert!(RenditionKind::parse(&"a".repeat(MAX_RENDITION_KIND_BYTES)).is_ok());
        for bad in [
            "",
            "2fast",
            "_x",
            "DuckDB",
            "duck-db",
            "duck.db",
            &"a".repeat(MAX_RENDITION_KIND_BYTES + 1),
        ] {
            assert_eq!(
                RenditionKind::parse(bad),
                Err(InvalidRenditionKind),
                "{bad}"
            );
        }
    }
}
