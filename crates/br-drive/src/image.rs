use serde::{Deserialize, Serialize};

pub const MAX_IMAGE_NAME_BYTES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid_image_name")]
pub struct InvalidImageName;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ImageName(String);

impl ImageName {
    pub fn parse(raw: &str) -> Result<Self, InvalidImageName> {
        page_of(raw).map(|_| Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn page(&self) -> i32 {
        page_of(&self.0).unwrap_or(0)
    }
}

/// Splits `raw` at its leading ASCII digits.
fn leading_digits(raw: &str) -> (&str, &str) {
    let end = raw
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(raw.len());
    raw.split_at(end)
}

/// A zero-padded counter of at least `width` digits, spelled the one way
/// `format!("{:0width$}")` spells it: wider only when the value needs it.
fn padded(digits: &str, width: usize) -> bool {
    digits.len() == width || (digits.len() > width && !digits.starts_with('0'))
}

/// `p{page:03}-img{index:02}.{ext}`: at least three page digits and two index
/// digits, more only when the number needs them, so every image has exactly one
/// name. Pages start at 1; an index is never zero.
fn page_of(raw: &str) -> Result<i32, InvalidImageName> {
    if raw.len() > MAX_IMAGE_NAME_BYTES {
        return Err(InvalidImageName);
    }
    let rest = raw.strip_prefix('p').ok_or(InvalidImageName)?;
    let (page, rest) = leading_digits(rest);
    let rest = rest.strip_prefix("-img").ok_or(InvalidImageName)?;
    let (index, rest) = leading_digits(rest);
    let extension = rest.strip_prefix('.').ok_or(InvalidImageName)?;
    let extension_sound = (1..=8).contains(&extension.len())
        && extension
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    if !padded(page, 3) || !padded(index, 2) || !extension_sound {
        return Err(InvalidImageName);
    }
    let page: i32 = page.parse().map_err(|_| InvalidImageName)?;
    if page < 1 || index.bytes().all(|b| b == b'0') {
        return Err(InvalidImageName);
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_page_scoped_convention_parses_and_yields_its_page() {
        let name = ImageName::parse("p003-img01.png").unwrap();
        assert_eq!(name.page(), 3);
        assert_eq!(ImageName::parse("p120-img12.jpeg").unwrap().page(), 120);
    }

    #[test]
    fn a_page_past_999_and_an_image_past_99_keep_one_canonical_name() {
        assert_eq!(ImageName::parse("p1000-img01.png").unwrap().page(), 1000);
        assert_eq!(ImageName::parse("p12345-img100.png").unwrap().page(), 12345);
        assert_eq!(ImageName::parse("p003-img100.webp").unwrap().page(), 3);
        for (page, index) in [(1, 1), (999, 99), (1000, 100), (2_000_000, 1234)] {
            let name = format!("p{page:03}-img{index:02}.png");
            assert_eq!(ImageName::parse(&name).unwrap().page(), page, "{name}");
        }
    }

    #[test]
    fn anything_off_the_convention_is_refused() {
        for raw in [
            "",
            "img01.png",
            "p3-img01.png",
            "p003-img1.png",
            "p003-img01",
            "p003-img01.PNG",
            "p000-img01.png",
            "p003-img00.png",
            "p003-img01.toolongext",
            "p003-img01.png/x",
            "p0003-img01.png",
            "p003-img001.png",
            "p003-img000.png",
            "p-img01.png",
            "p003-img.png",
            "p99999999999-img01.png",
        ] {
            assert_eq!(ImageName::parse(raw), Err(InvalidImageName), "{raw:?}");
        }
    }
}
