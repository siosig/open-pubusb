//! Subscription attribute filters (`Subscription.filter`, FR-011),
//! implementing the Cloud Pub/Sub filter grammar and evaluation rules
//! exactly.
//!
//! [`compile`] parses a filter string once (at subscription create time,
//! and again at recovery — see `crate::subscription`); the resulting
//! [`CompiledFilter`] is cheap to evaluate repeatedly against each
//! candidate message's attributes in the delivery hot path
//! (`crate::delivery::engine`).

pub mod ast;
mod eval;
mod parser;
#[cfg(test)]
mod tests;

use std::collections::HashMap;

use ast::Expr;

/// A parsed, ready-to-evaluate filter. An *absent* `CompiledFilter`
/// (`Option::None` at call sites) means "no filter" — an empty filter
/// string compiles to `None`, not to some vacuously-true `Expr`, per the
/// contract ("an empty filter string means 'no filter'").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledFilter(Expr);

impl CompiledFilter {
    /// Whether `attrs` matches this filter, per the filter grammar's
    /// evaluation rules.
    pub fn matches(&self, attrs: &HashMap<String, String>) -> bool {
        self.0.matches(attrs)
    }
}

/// Compiles `filter`. An empty (or all-whitespace) string yields
/// `Ok(None)` ("no filter" — everything matches). A non-empty string
/// that fails to parse yields `Err(Error::InvalidArgument)`.
pub fn compile(filter: &str) -> crate::error::Result<Option<CompiledFilter>> {
    if filter.trim().is_empty() {
        return Ok(None);
    }
    parser::parse(filter).map(|expr| Some(CompiledFilter(expr)))
}
