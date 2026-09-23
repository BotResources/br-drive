//! Labels: one catalogue per host service, assigned to files as a target set.

mod gestures;
mod store;
mod view;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::name::NounName;
use service_engine::wire::Noun;
use uuid::Uuid;

use crate::fault::{DriveFault, codes};

pub use gestures::{
    CreateLabel, DeleteLabel, SetFileLabels, UpdateLabel, create_label, delete_label,
    set_file_labels, update_label,
};
pub use store::label_ids_of_files;
pub use view::{DriveLabel, DriveLabels, LabelWindow};

pub const MAX_LABEL_NAME_CHARS: usize = 100;
pub const MAX_LABEL_DESCRIPTION_BYTES: usize = 1024;

pub struct Label;

impl Noun for Label {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("drive_label");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum LabelCause {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelRow {
    pub id: Uuid,
    pub name: String,
    pub color: String,
    pub description: String,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A label name: trimmed, 1–100 characters.
pub fn validate_name(raw: &str) -> Result<String, DriveFault> {
    let name = raw.trim();
    if name.is_empty() || name.chars().count() > MAX_LABEL_NAME_CHARS {
        return Err(DriveFault::Refused(codes::INVALID_LABEL));
    }
    Ok(name.to_string())
}

/// A label description: free text, at most 1 KiB.
pub fn validate_description(raw: &str) -> Result<String, DriveFault> {
    if raw.len() > MAX_LABEL_DESCRIPTION_BYTES {
        return Err(DriveFault::Refused(codes::INVALID_LABEL));
    }
    Ok(raw.to_string())
}

/// A label colour: `#rrggbb`, lowercase hexadecimal (an uppercase input is
/// accepted and lowercased).
pub fn validate_color(raw: &str) -> Result<String, DriveFault> {
    let color = raw.trim().to_ascii_lowercase();
    let sound = color.len() == 7
        && color.starts_with('#')
        && color[1..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if !sound {
        return Err(DriveFault::Refused(codes::INVALID_LABEL));
    }
    Ok(color)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_is_trimmed_and_bounded_in_characters_like_the_database_check() {
        assert_eq!(validate_name("  Urgent ").unwrap(), "Urgent");
        assert!(validate_name("   ").is_err());
        assert!(validate_name(&"x".repeat(101)).is_err());
        let accented = "é".repeat(100);
        assert!(
            accented.len() > MAX_LABEL_NAME_CHARS,
            "two bytes per character"
        );
        assert!(validate_name(&accented).is_ok());
        assert!(validate_name(&"é".repeat(101)).is_err());
    }

    #[test]
    fn a_description_is_bounded_in_bytes() {
        assert!(validate_description(&"x".repeat(1024)).is_ok());
        assert!(validate_description(&"é".repeat(600)).is_err());
    }

    #[test]
    fn a_colour_is_a_lowercase_hex_triplet() {
        assert_eq!(validate_color("#A1B2C3").unwrap(), "#a1b2c3");
        assert!(validate_color("a1b2c3").is_err());
        assert!(validate_color("#a1b2c").is_err());
        assert!(validate_color("#g1b2c3").is_err());
    }
}
