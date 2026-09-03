//! Parses a filter string into an [`Expr`] AST, per the filter grammar.

use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

use super::ast::{CompareOp, Expr};
use crate::error::Error;
use crate::limits::MAX_FILTER_CHARS;

#[derive(Parser)]
#[grammar = "filter/grammar.pest"]
struct FilterGrammar;

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidArgument {
        field: "filter".to_string(),
        message: message.into(),
    }
}

/// Parses `input` into an [`Expr`]. An empty (or all-whitespace) string is
/// rejected here — "no filter" is represented by the *absence* of a
/// compiled filter one layer up ([`super::compile`]), not by an empty
/// `Expr`.
pub fn parse(input: &str) -> Result<Expr, Error> {
    if input.chars().count() > MAX_FILTER_CHARS {
        return Err(invalid(format!(
            "filter exceeds the {MAX_FILTER_CHARS}-character limit"
        )));
    }

    let mut pairs = FilterGrammar::parse(Rule::filter, input)
        .map_err(|e| invalid(format!("syntax error: {e}")))?;
    let filter_pair = pairs.next().ok_or_else(|| invalid("empty filter"))?;
    let expr_pair = filter_pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::expr)
        .ok_or_else(|| invalid("empty filter"))?;
    build_expr(expr_pair)
}

fn build_expr(pair: Pair<Rule>) -> Result<Expr, Error> {
    debug_assert_eq!(pair.as_rule(), Rule::expr);
    let mut inner = pair.into_inner();
    let first_unary = inner
        .next()
        .ok_or_else(|| invalid("internal error: expr with no unary"))?;
    let first = build_unary(first_unary)?;

    let Some(chain) = inner.next() else {
        return Ok(first);
    };

    let chain_rule = chain.as_rule();
    let mut operands = vec![first];
    for unary_pair in chain.into_inner() {
        operands.push(build_unary(unary_pair)?);
    }

    match chain_rule {
        Rule::and_chain => Ok(Expr::And(operands)),
        Rule::or_chain => Ok(Expr::Or(operands)),
        other => Err(invalid(format!(
            "internal error: unexpected chain rule {other:?}"
        ))),
    }
}

fn build_unary(pair: Pair<Rule>) -> Result<Expr, Error> {
    debug_assert_eq!(pair.as_rule(), Rule::unary);
    let mut negated = false;
    let mut primary_pair = None;
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::negation => negated = true,
            Rule::primary => primary_pair = Some(p),
            other => {
                return Err(invalid(format!(
                    "internal error: unexpected rule {other:?} in unary"
                )))
            }
        }
    }
    let primary_pair =
        primary_pair.ok_or_else(|| invalid("internal error: unary with no primary"))?;
    let expr = build_primary(primary_pair)?;
    Ok(if negated {
        Expr::Not(Box::new(expr))
    } else {
        expr
    })
}

fn build_primary(pair: Pair<Rule>) -> Result<Expr, Error> {
    debug_assert_eq!(pair.as_rule(), Rule::primary);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| invalid("internal error: empty primary"))?;
    match inner.as_rule() {
        Rule::comparison => build_comparison(inner),
        Rule::has_attr => build_has_attr(inner),
        Rule::function => build_function(inner),
        Rule::expr => build_expr(inner),
        other => Err(invalid(format!(
            "internal error: unexpected primary rule {other:?}"
        ))),
    }
}

fn build_comparison(pair: Pair<Rule>) -> Result<Expr, Error> {
    let mut inner = pair.into_inner();
    let attr = build_attribute(inner.next().ok_or_else(|| invalid("missing attribute"))?)?;
    let op_pair = inner.next().ok_or_else(|| invalid("missing operator"))?;
    let op = match op_pair.as_str() {
        "=" => CompareOp::Eq,
        "!=" => CompareOp::Ne,
        other => {
            return Err(invalid(format!(
                "internal error: unexpected operator {other:?}"
            )))
        }
    };
    let value = build_string(inner.next().ok_or_else(|| invalid("missing value"))?)?;
    Ok(Expr::Compare { attr, op, value })
}

fn build_has_attr(pair: Pair<Rule>) -> Result<Expr, Error> {
    let key_pair = pair
        .into_inner()
        .next()
        .ok_or_else(|| invalid("missing key after attributes:"))?;
    Ok(Expr::HasAttr {
        attr: build_key(key_pair)?,
    })
}

fn build_function(pair: Pair<Rule>) -> Result<Expr, Error> {
    let mut inner = pair.into_inner();
    let attr = build_attribute(inner.next().ok_or_else(|| invalid("missing attribute"))?)?;
    let prefix = build_string(inner.next().ok_or_else(|| invalid("missing prefix"))?)?;
    Ok(Expr::HasPrefix { attr, prefix })
}

fn build_attribute(pair: Pair<Rule>) -> Result<String, Error> {
    debug_assert_eq!(pair.as_rule(), Rule::attribute);
    let key_pair = pair
        .into_inner()
        .next()
        .ok_or_else(|| invalid("missing key after attributes."))?;
    build_key(key_pair)
}

fn build_key(pair: Pair<Rule>) -> Result<String, Error> {
    debug_assert_eq!(pair.as_rule(), Rule::key);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| invalid("internal error: empty key"))?;
    match inner.as_rule() {
        Rule::identifier => Ok(inner.as_str().to_string()),
        Rule::string => build_string(inner),
        other => Err(invalid(format!(
            "internal error: unexpected key rule {other:?}"
        ))),
    }
}

fn build_string(pair: Pair<Rule>) -> Result<String, Error> {
    debug_assert_eq!(pair.as_rule(), Rule::string);
    let raw = pair
        .into_inner()
        .next()
        .ok_or_else(|| invalid("internal error: empty string"))?;
    debug_assert_eq!(raw.as_rule(), Rule::string_inner);
    unescape(raw.as_str())
}

fn unescape(raw: &str) -> Result<String, Error> {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('u') => {
                let hex: String = chars.by_ref().take(4).collect();
                let code =
                    u32::from_str_radix(&hex, 16).map_err(|_| invalid("invalid \\u escape"))?;
                out.push(char::from_u32(code).ok_or_else(|| invalid("invalid \\u escape"))?);
            }
            _ => return Err(invalid("invalid escape sequence")),
        }
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_comparison() {
        let expr = parse(r#"attributes.type = "order""#).unwrap();
        assert_eq!(
            expr,
            Expr::Compare {
                attr: "type".to_string(),
                op: CompareOp::Eq,
                value: "order".to_string(),
            }
        );
    }

    #[test]
    fn rejects_mixed_and_or_without_parens() {
        assert!(
            parse(r#"attributes.a = "1" AND attributes.b = "2" OR attributes.c = "3""#).is_err()
        );
    }

    #[test]
    fn accepts_mixed_and_or_with_parens() {
        assert!(parse(
            r#"NOT attributes:debug AND (attributes.env = "prod" OR attributes.env = "stage")"#
        )
        .is_ok());
    }

    #[test]
    fn rejects_bare_data_field() {
        assert!(parse(r#"data = "x""#).is_err());
    }

    #[test]
    fn accepts_attribute_literally_named_data() {
        assert!(parse(r#"attributes.data = "x""#).is_ok());
    }

    #[test]
    fn accepts_quoted_key_with_dot() {
        let expr = parse(r#"attributes."my.key" = "v""#).unwrap();
        assert_eq!(
            expr,
            Expr::Compare {
                attr: "my.key".to_string(),
                op: CompareOp::Eq,
                value: "v".to_string(),
            }
        );
    }

    #[test]
    fn rejects_over_256_chars() {
        let long_value = "x".repeat(300);
        let filter = format!(r#"attributes.a = "{long_value}""#);
        assert!(parse(&filter).is_err());
    }
}
