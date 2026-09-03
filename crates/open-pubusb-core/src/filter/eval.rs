//! Evaluates a parsed filter [`Expr`] against a message's attributes, per
//! the filter grammar's evaluation-rules table.

use std::collections::HashMap;

use super::ast::{CompareOp, Expr};

impl Expr {
    /// `true` iff `attrs` satisfies this expression.
    pub fn matches(&self, attrs: &HashMap<String, String>) -> bool {
        match self {
            Expr::Compare { attr, op, value } => {
                let Some(actual) = attrs.get(attr) else {
                    // "k != v" is false when k is absent too (per the
                    // contract's evaluation table), not vacuously true —
                    // both comparison kinds require the attribute to
                    // exist.
                    return false;
                };
                match op {
                    CompareOp::Eq => actual == value,
                    CompareOp::Ne => actual != value,
                }
            }
            Expr::HasAttr { attr } => attrs.contains_key(attr),
            Expr::HasPrefix { attr, prefix } => attrs
                .get(attr)
                .is_some_and(|v| v.starts_with(prefix.as_str())),
            Expr::Not(inner) => !inner.matches(attrs),
            Expr::And(operands) => operands.iter().all(|e| e.matches(attrs)),
            Expr::Or(operands) => operands.iter().any(|e| e.matches(attrs)),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::parser::parse;
    use std::collections::HashMap;

    fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn eq_matches_when_present_and_equal() {
        let e = parse(r#"attributes.type = "order""#).unwrap();
        assert!(e.matches(&attrs(&[("type", "order")])));
        assert!(!e.matches(&attrs(&[("type", "other")])));
        assert!(!e.matches(&attrs(&[])));
    }

    #[test]
    fn ne_is_false_when_attribute_absent() {
        let e = parse(r#"attributes.type != "order""#).unwrap();
        assert!(!e.matches(&attrs(&[])));
        assert!(e.matches(&attrs(&[("type", "other")])));
        assert!(!e.matches(&attrs(&[("type", "order")])));
    }

    #[test]
    fn has_attr_ignores_value() {
        let e = parse("attributes:region").unwrap();
        assert!(e.matches(&attrs(&[("region", "")])));
        assert!(!e.matches(&attrs(&[])));
    }

    #[test]
    fn has_prefix_matches_prefix() {
        let e = parse(r#"hasPrefix(attributes.name, "us-")"#).unwrap();
        assert!(e.matches(&attrs(&[("name", "us-east1")])));
        assert!(!e.matches(&attrs(&[("name", "eu-west1")])));
    }

    #[test]
    fn not_and_or_combine_per_evaluation_table() {
        let e = parse(
            r#"NOT attributes:debug AND (attributes.env = "prod" OR attributes.env = "stage")"#,
        )
        .unwrap();
        assert!(e.matches(&attrs(&[("env", "stage")])));
        assert!(e.matches(&attrs(&[("env", "prod")])));
        assert!(!e.matches(&attrs(&[("env", "dev")])));
        assert!(!e.matches(&attrs(&[("env", "stage"), ("debug", "true")])));
    }
}
