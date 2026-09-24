//! The rules every rendition write obeys, whoever writes it — a runner's
//! report or a host's import.

use std::collections::HashSet;

use crate::fault::{DriveFault, codes};
use crate::file::FileRow;
use crate::runner::MAX_REPORT_PAGES;

/// The rules every rendition write obeys, from a runner or from an import:
/// at most `MAX_REPORT_PAGES` pages, numbered from 1 and distinct; the summary
/// and the page count together, the token estimate optional and only with
/// them, none negative. Answers whether the write carries an indexing.
pub(crate) fn validate_rendition(
    numbers: &[i32],
    summary: Option<&str>,
    page_count: Option<i32>,
    estimated_tokens: Option<i64>,
) -> Result<bool, DriveFault> {
    if numbers.len() > MAX_REPORT_PAGES {
        return Err(DriveFault::Refused(codes::BATCH_TOO_LARGE));
    }
    let mut seen = HashSet::with_capacity(numbers.len());
    if numbers
        .iter()
        .any(|number| *number < 1 || !seen.insert(*number))
    {
        return Err(DriveFault::Refused(codes::INVALID_PAGE));
    }
    let indexed = summary.is_some();
    if indexed != page_count.is_some() || (estimated_tokens.is_some() && !indexed) {
        return Err(DriveFault::Refused(codes::INDEXER_FIELDS_TOGETHER));
    }
    if page_count.is_some_and(|count| count < 0)
        || estimated_tokens.is_some_and(|tokens| tokens < 0)
    {
        return Err(DriveFault::Refused(codes::INVALID_INDEXER_VALUE));
    }
    Ok(indexed)
}

/// Records an indexing on the file; `true` when it changed anything. An
/// indexing without an estimate clears a previous one: the three fields
/// describe one indexing, never two.
pub(crate) fn apply_indexing<H>(
    file: &mut FileRow<H>,
    summary: String,
    page_count: i32,
    estimated_tokens: Option<i64>,
) -> bool {
    let changed = file.summary.as_deref() != Some(summary.as_str())
        || file.page_count != Some(page_count)
        || file.estimated_tokens != estimated_tokens;
    if changed {
        file.summary = Some(summary);
        file.page_count = Some(page_count);
        file.estimated_tokens = estimated_tokens;
    }
    changed
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    fn code(result: Result<bool, DriveFault>) -> &'static str {
        match result {
            Err(DriveFault::Refused(reason)) => reason.code(),
            Err(DriveFault::Engine(error)) => panic!("{error}"),
            Ok(_) => "OK",
        }
    }

    #[test]
    fn a_rendition_write_is_bounded_numbered_and_indexed_as_a_pair() {
        let many: Vec<i32> = (1..=513).collect();
        assert_eq!(
            code(validate_rendition(&many, None, None, None)),
            "BATCH_TOO_LARGE"
        );
        assert_eq!(
            code(validate_rendition(&many[..512], None, None, None)),
            "OK"
        );
        assert_eq!(
            code(validate_rendition(&[0], None, None, None)),
            "INVALID_PAGE"
        );
        assert_eq!(
            code(validate_rendition(&[2, 2], None, None, None)),
            "INVALID_PAGE"
        );
        for (summary, count, tokens) in [
            (Some("s"), None, None),
            (None, Some(1), None),
            (None, None, Some(1)),
        ] {
            assert_eq!(
                code(validate_rendition(&[], summary, count, tokens)),
                "INDEXER_FIELDS_TOGETHER",
                "{summary:?} {count:?} {tokens:?}"
            );
        }
        assert_eq!(
            code(validate_rendition(&[], Some("s"), Some(-1), None)),
            "INVALID_INDEXER_VALUE"
        );
        assert_eq!(
            code(validate_rendition(&[], Some("s"), Some(1), Some(-1))),
            "INVALID_INDEXER_VALUE"
        );
        assert!(matches!(
            validate_rendition(&[1], Some("s"), Some(1), None),
            Ok(true)
        ));
        assert!(matches!(
            validate_rendition(&[1], None, None, None),
            Ok(false)
        ));
    }

    #[test]
    fn an_indexing_is_recorded_whole_and_an_absent_estimate_clears_the_previous_one() {
        let mut file = crate::file::tests_support::processing_file::<()>(0, Utc::now());
        assert!(apply_indexing(&mut file, "s".into(), 3, Some(99)));
        assert!(
            !apply_indexing(&mut file, "s".into(), 3, Some(99)),
            "unchanged"
        );
        assert!(
            apply_indexing(&mut file, "s".into(), 3, None),
            "the estimate alone"
        );
        assert_eq!(file.estimated_tokens, None);
        assert_eq!(file.page_count, Some(3));
    }
}
