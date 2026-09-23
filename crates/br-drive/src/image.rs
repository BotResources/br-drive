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

fn page_of(raw: &str) -> Result<i32, InvalidImageName> {
    if raw.len() > MAX_IMAGE_NAME_BYTES {
        return Err(InvalidImageName);
    }
    let rest = raw.strip_prefix('p').ok_or(InvalidImageName)?;
    let (page, rest) = rest.split_at_checked(3).ok_or(InvalidImageName)?;
    let rest = rest.strip_prefix("-img").ok_or(InvalidImageName)?;
    let (index, rest) = rest.split_at_checked(2).ok_or(InvalidImageName)?;
    let extension = rest.strip_prefix('.').ok_or(InvalidImageName)?;
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    let extension_sound = (1..=8).contains(&extension.len())
        && extension
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    if !digits(page) || !digits(index) || !extension_sound {
        return Err(InvalidImageName);
    }
    let page: i32 = page.parse().map_err(|_| InvalidImageName)?;
    if page < 1 || index == "00" {
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
        ] {
            assert_eq!(ImageName::parse(raw), Err(InvalidImageName), "{raw:?}");
        }
    }
}
