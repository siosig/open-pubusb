//! Contract tests for the filter grammar: every row of the filter grammar's
//! test-case table, plus a proptest proving evaluation never panics on
//! an arbitrary attribute map.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use proptest::prelude::*;

use super::compile;

#[test]
fn empty_filter_compiles_to_none() {
    assert_eq!(compile("").unwrap(), None);
    assert_eq!(compile("   ").unwrap(), None);
}

#[test]
fn non_empty_filter_compiles_to_some() {
    assert!(compile(r#"attributes.k = "v""#).unwrap().is_some());
}

#[test]
fn invalid_filter_is_rejected() {
    assert!(compile(r#"data = "x""#).is_err());
}

fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn contract_table_attributes_type_eq_order_matches() {
    let f = compile(r#"attributes.type = "order""#).unwrap().unwrap();
    assert!(f.matches(&attrs(&[("type", "order")])));
}

#[test]
fn contract_table_attributes_type_ne_order_absent_does_not_match() {
    let f = compile(r#"attributes.type != "order""#).unwrap().unwrap();
    assert!(!f.matches(&attrs(&[])));
}

#[test]
fn contract_table_attributes_region_has_attr_matches() {
    let f = compile("attributes:region").unwrap().unwrap();
    assert!(f.matches(&attrs(&[("region", "")])));
}

#[test]
fn contract_table_has_prefix_matches() {
    let f = compile(r#"hasPrefix(attributes.name, "us-")"#)
        .unwrap()
        .unwrap();
    assert!(f.matches(&attrs(&[("name", "us-east1")])));
}

#[test]
fn contract_table_not_and_or_with_parens_matches() {
    let f = compile(
        r#"NOT attributes:debug AND (attributes.env = "prod" OR attributes.env = "stage")"#,
    )
    .unwrap()
    .unwrap();
    assert!(f.matches(&attrs(&[("env", "stage")])));
}

#[test]
fn contract_table_and_or_mixing_without_parens_is_invalid_argument() {
    let err =
        compile(r#"attributes.a = "1" AND attributes.b = "2" OR attributes.c = "3""#).unwrap_err();
    assert!(matches!(err, crate::error::Error::InvalidArgument { .. }));
}

#[test]
fn contract_table_quoted_key_with_dot_matches() {
    let f = compile(r#"attributes."my.key" = "v""#).unwrap().unwrap();
    assert!(f.matches(&attrs(&[("my.key", "v")])));
}

#[test]
fn contract_table_attribute_named_data_is_syntactically_valid() {
    assert!(compile(r#"attributes.data = "x""#).unwrap().is_some());
}

#[test]
fn contract_table_bare_data_field_is_invalid_argument() {
    let err = compile(r#"data = "x""#).unwrap_err();
    assert!(matches!(err, crate::error::Error::InvalidArgument { .. }));
}

#[test]
fn contract_table_257_chars_is_invalid_argument() {
    let filter = "attributes.k".to_string() + &" ".repeat(300);
    assert!(filter.chars().count() > 256);
    let err = compile(&filter).unwrap_err();
    assert!(matches!(err, crate::error::Error::InvalidArgument { .. }));
}

proptest! {
    /// However `filter` and the attribute map are constructed, evaluating
    /// (when the filter compiles at all) must never panic — a filter is
    /// evaluated on the delivery hot path (`crate::delivery::engine`) for
    /// every candidate message, so a panic there would be a livelock/DoS
    /// on that subscription, not just a wrong answer.
    #[test]
    fn evaluation_never_panics(
        filter_src in "(attributes\\.[a-z]{1,5} (=|!=) \"[a-z0-9]{0,5}\"|attributes:[a-z]{1,5}|hasPrefix\\(attributes\\.[a-z]{1,5}, \"[a-z]{0,3}\"\\))( (AND|OR) (attributes\\.[a-z]{1,5} (=|!=) \"[a-z0-9]{0,5}\"|attributes:[a-z]{1,5})){0,2}",
        attr_keys in proptest::collection::vec("[a-z]{1,5}", 0..5),
        attr_values in proptest::collection::vec("[a-z0-9]{0,5}", 0..5),
    ) {
        let attrs: HashMap<String, String> = attr_keys
            .into_iter()
            .zip(attr_values)
            .collect();
        if let Ok(Some(compiled)) = compile(&filter_src) {
            let _ = compiled.matches(&attrs);
        }
    }
}
