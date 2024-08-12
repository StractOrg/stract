/*!

Kuchiki (朽木), a HTML/XML tree manipulation library for Rust.

*/

#![deny(missing_docs)]

#[macro_use]
extern crate html5ever;
#[macro_use]
extern crate matches;

mod attributes;
mod cell_extras;
pub mod iter;
mod node_data_ref;
mod parser;
mod select;
mod serializer;
mod xpath;

#[cfg(test)]
mod tests;
mod tree;

pub use attributes::{Attribute, Attributes, ExpandedName};
pub use node_data_ref::NodeDataRef;
pub use parser::{parse_fragment, parse_html, parse_html_with_options, ParseOpts, Sink};
pub use select::{Selector, Selectors, Specificity};
pub use tree::{Doctype, DocumentData, ElementData, Node, NodeData, NodeRef};

type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
/// The error type for Kuchiki.
pub enum Error {
    /// The given css selector is invalid.
    #[error("CSS parse error")]
    CssParseError,
}

/// This module re-exports a number of traits that are useful when using Kuchiki.
/// It can be used with:
///
/// ```rust
/// use kuchiki::traits::*;
/// ```
pub mod traits {
    pub use crate::iter::{ElementIterator, NodeIterator};
    pub use html5ever::tendril::TendrilSink;
}
