//! Parsed representation of a subscription filter expression, per the
//! filter grammar.

/// A parsed filter expression. `attributes` keys/values are always owned
/// `String`s — filters are compiled once (at subscription create/recover
/// time) and evaluated many times, so paying an allocation at compile
/// time to avoid one at every evaluation is the right trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// `attributes.{attr} = "{value}"` / `!=` — `op` says which.
    Compare {
        /// The attribute key being compared.
        attr: String,
        /// Equality or inequality.
        op: CompareOp,
        /// The literal value compared against.
        value: String,
    },
    /// `attributes:{attr}` — true iff the message has this attribute key
    /// at all, regardless of value.
    HasAttr {
        /// The attribute key being tested for presence.
        attr: String,
    },
    /// `hasPrefix(attributes.{attr}, "{prefix}")`.
    HasPrefix {
        /// The attribute key whose value is tested.
        attr: String,
        /// The prefix the attribute's value must start with.
        prefix: String,
    },
    /// `NOT` negation of a sub-expression.
    Not(Box<Expr>),
    /// 2 or more operands, all joined by AND (the grammar forbids mixing
    /// AND/OR at one level without parens, so a flat `Vec` here is exact,
    /// not just an optimization).
    And(Vec<Expr>),
    /// 2 or more operands, all joined by OR.
    Or(Vec<Expr>),
}

/// The comparison operator in [`Expr::Compare`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    /// `=`
    Eq,
    /// `!=`
    Ne,
}
