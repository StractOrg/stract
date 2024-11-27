use crate::attributes::ExpandedName;
use crate::iter::{NodeIterator, Select};
use crate::node_data_ref::NodeDataRef;
use crate::tree::{ElementData, Node, NodeData, NodeRef};
use crate::{Error, Result};
use cssparser::{CowRcStr, ParseError, SourceLocation, ToCss};
use html5ever::Namespace;
use selectors::attr::{AttrSelectorOperation, CaseSensitivity, NamespaceConstraint};
use selectors::context::{IgnoreNthChildForInvalidation, QuirksMode};
use selectors::parser::SelectorParseErrorKind;
use selectors::parser::{
    NonTSPseudoClass, Parser, Selector as GenericSelector, SelectorImpl, SelectorList,
};
use selectors::{matching, NthIndexCache, OpaqueElement};
use std::fmt;

/// The definition of whitespace per CSS Selectors Level 3 § 4.
///
/// Copied from rust-selectors.
static SELECTOR_WHITESPACE: &[char] = &[' ', '\t', '\n', '\r', '\x0C'];

#[derive(Debug, Clone)]
pub struct KuchikiSelectors;

impl SelectorImpl for KuchikiSelectors {
    type AttrValue = AttrValue;
    type Identifier = LocalName;
    type LocalName = LocalName;
    type NamespacePrefix = LocalName;
    type NamespaceUrl = Namespace;
    type BorrowedNamespaceUrl = Namespace;
    type BorrowedLocalName = LocalName;

    type NonTSPseudoClass = PseudoClass;
    type PseudoElement = PseudoElement;

    type ExtraMatchingData<'a> = ();
}

struct KuchikiParser;

impl<'i> Parser<'i> for KuchikiParser {
    type Impl = KuchikiSelectors;
    type Error = SelectorParseErrorKind<'i>;

    fn parse_non_ts_pseudo_class(
        &self,
        location: SourceLocation,
        name: CowRcStr<'i>,
    ) -> std::result::Result<PseudoClass, ParseError<'i, SelectorParseErrorKind<'i>>> {
        use self::PseudoClass::{
            Active, AnyLink, Checked, Disabled, Enabled, Focus, Hover, Indeterminate, Link, Visited,
        };
        if name.eq_ignore_ascii_case("any-link") {
            Ok(AnyLink)
        } else if name.eq_ignore_ascii_case("link") {
            Ok(Link)
        } else if name.eq_ignore_ascii_case("visited") {
            Ok(Visited)
        } else if name.eq_ignore_ascii_case("active") {
            Ok(Active)
        } else if name.eq_ignore_ascii_case("focus") {
            Ok(Focus)
        } else if name.eq_ignore_ascii_case("hover") {
            Ok(Hover)
        } else if name.eq_ignore_ascii_case("enabled") {
            Ok(Enabled)
        } else if name.eq_ignore_ascii_case("disabled") {
            Ok(Disabled)
        } else if name.eq_ignore_ascii_case("checked") {
            Ok(Checked)
        } else if name.eq_ignore_ascii_case("indeterminate") {
            Ok(Indeterminate)
        } else {
            Err(
                location.new_custom_error(SelectorParseErrorKind::UnsupportedPseudoClassOrElement(
                    name,
                )),
            )
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub enum PseudoClass {
    AnyLink,
    Link,
    Visited,
    Active,
    Focus,
    Hover,
    Enabled,
    Disabled,
    Checked,
    Indeterminate,
}

impl NonTSPseudoClass for PseudoClass {
    type Impl = KuchikiSelectors;

    fn is_active_or_hover(&self) -> bool {
        matches!(*self, PseudoClass::Active | PseudoClass::Hover)
    }

    fn is_user_action_state(&self) -> bool {
        matches!(
            *self,
            PseudoClass::Active | PseudoClass::Hover | PseudoClass::Focus
        )
    }
}

impl ToCss for PseudoClass {
    fn to_css<W>(&self, dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        dest.write_str(match *self {
            PseudoClass::AnyLink => ":any-link",
            PseudoClass::Link => ":link",
            PseudoClass::Visited => ":visited",
            PseudoClass::Active => ":active",
            PseudoClass::Focus => ":focus",
            PseudoClass::Hover => ":hover",
            PseudoClass::Enabled => ":enabled",
            PseudoClass::Disabled => ":disabled",
            PseudoClass::Checked => ":checked",
            PseudoClass::Indeterminate => ":indeterminate",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AttrValue(String);
impl ToCss for AttrValue {
    fn to_css<W>(&self, dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        dest.write_str(&self.0)
    }
}

impl<'a> From<&'a str> for AttrValue {
    fn from(s: &'a str) -> Self {
        AttrValue(s.to_string())
    }
}

impl AsRef<str> for AttrValue {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct LocalName(html5ever::LocalName);
impl ToCss for LocalName {
    fn to_css<W>(&self, dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        dest.write_str(&self.0)
    }
}

impl<'a> From<&'a str> for LocalName {
    fn from(s: &'a str) -> Self {
        LocalName(html5ever::LocalName::from(s))
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub enum PseudoElement {}

impl ToCss for PseudoElement {
    fn to_css<W>(&self, _dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        Ok(())
    }
}

impl selectors::parser::PseudoElement for PseudoElement {
    type Impl = KuchikiSelectors;
}

impl selectors::Element for NodeDataRef<ElementData> {
    type Impl = KuchikiSelectors;

    #[inline]
    fn opaque(&self) -> OpaqueElement {
        let node: &Node = self.as_node();
        OpaqueElement::new(node)
    }

    #[inline]
    fn is_html_slot_element(&self) -> bool {
        false
    }
    #[inline]
    fn parent_node_is_shadow_root(&self) -> bool {
        false
    }
    #[inline]
    fn containing_shadow_host(&self) -> Option<Self> {
        None
    }

    #[inline]
    fn parent_element(&self) -> Option<Self> {
        self.as_node().parent().and_then(NodeRef::into_element_ref)
    }
    #[inline]
    fn prev_sibling_element(&self) -> Option<Self> {
        self.as_node().preceding_siblings().elements().next()
    }
    #[inline]
    fn next_sibling_element(&self) -> Option<Self> {
        self.as_node().following_siblings().elements().next()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.as_node().children().all(|child| match *child.data() {
            NodeData::Element(_) => false,
            NodeData::Text(ref text) => text.borrow().is_empty(),
            _ => true,
        })
    }
    #[inline]
    fn is_root(&self) -> bool {
        match self.as_node().parent() {
            None => false,
            Some(parent) => matches!(*parent.data(), NodeData::Document(_)),
        }
    }

    #[inline]
    fn is_html_element_in_html_document(&self) -> bool {
        // FIXME: Have a notion of HTML document v.s. XML document?
        self.name.ns == ns!(html)
    }

    #[inline]
    fn has_local_name(&self, name: &LocalName) -> bool {
        self.name.local == name.0
    }
    #[inline]
    fn has_namespace(&self, namespace: &Namespace) -> bool {
        self.name.ns == *namespace
    }

    #[inline]
    fn is_part(&self, _name: &LocalName) -> bool {
        false
    }

    #[inline]
    fn imported_part(&self, _: &LocalName) -> Option<LocalName> {
        None
    }

    #[inline]
    fn is_pseudo_element(&self) -> bool {
        false
    }

    #[inline]
    fn is_same_type(&self, other: &Self) -> bool {
        self.name == other.name
    }

    #[inline]
    fn is_link(&self) -> bool {
        self.name.ns == ns!(html)
            && matches!(
                self.name.local,
                local_name!("a") | local_name!("area") | local_name!("link")
            )
            && self
                .attributes
                .borrow()
                .map
                .contains_key(&ExpandedName::new(ns!(), local_name!("href")))
    }

    #[inline]
    fn has_id(&self, id: &LocalName, case_sensitivity: CaseSensitivity) -> bool {
        self.attributes
            .borrow()
            .get(local_name!("id"))
            .is_some_and(|id_attr| case_sensitivity.eq(id.0.as_bytes(), id_attr.as_bytes()))
    }

    #[inline]
    fn has_class(&self, name: &LocalName, case_sensitivity: CaseSensitivity) -> bool {
        let name = name.0.as_bytes();
        !name.is_empty()
            && if let Some(class_attr) = self.attributes.borrow().get(local_name!("class")) {
                class_attr
                    .split(SELECTOR_WHITESPACE)
                    .any(|class| case_sensitivity.eq(class.as_bytes(), name))
            } else {
                false
            }
    }

    #[inline]
    fn attr_matches(
        &self,
        ns: &NamespaceConstraint<&Namespace>,
        local_name: &LocalName,
        operation: &AttrSelectorOperation<&AttrValue>,
    ) -> bool {
        let attrs = self.attributes.borrow();
        match *ns {
            NamespaceConstraint::Any => attrs
                .map
                .iter()
                .any(|(name, attr)| name.local == local_name.0 && operation.eval_str(&attr.value)),
            NamespaceConstraint::Specific(ns_url) => attrs
                .map
                .get(&ExpandedName::new(ns_url, local_name.0.clone()))
                .is_some_and(|attr| operation.eval_str(&attr.value)),
        }
    }

    fn match_pseudo_element(
        &self,
        _pseudo: &PseudoElement,
        _context: &mut matching::MatchingContext<KuchikiSelectors>,
    ) -> bool {
        true
    }

    fn match_non_ts_pseudo_class(
        &self,
        pseudo: &PseudoClass,
        _context: &mut matching::MatchingContext<KuchikiSelectors>,
    ) -> bool {
        use self::PseudoClass::{
            Active, AnyLink, Checked, Disabled, Enabled, Focus, Hover, Indeterminate, Link, Visited,
        };
        match *pseudo {
            Active | Focus | Hover | Enabled | Disabled | Checked | Indeterminate | Visited => {
                false
            }
            AnyLink | Link => {
                self.name.ns == ns!(html)
                    && matches!(
                        self.name.local,
                        local_name!("a") | local_name!("area") | local_name!("link")
                    )
                    && self.attributes.borrow().contains(local_name!("href"))
            }
        }
    }

    fn first_element_child(&self) -> Option<Self> {
        self.as_node().children().elements().next()
    }

    fn apply_selector_flags(&self, _flags: matching::ElementSelectorFlags) {}
}

/// A pre-compiled list of CSS Selectors.
pub struct Selectors(pub Vec<Selector>);

/// A pre-compiled CSS Selector.
pub struct Selector(GenericSelector<KuchikiSelectors>);

/// The specificity of a selector.
///
/// Opaque, but ordered.
///
/// Determines precedence in the cascading algorithm.
/// When equal, a rule later in source order takes precedence.
#[derive(Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Specificity(u32);

impl Selectors {
    /// Compile a list of selectors. This may fail on syntax errors or unsupported selectors.
    #[inline]
    pub fn compile(s: &str) -> Result<Selectors> {
        let mut input = cssparser::ParserInput::new(s);
        match SelectorList::parse(
            &KuchikiParser,
            &mut cssparser::Parser::new(&mut input),
            selectors::parser::ParseRelative::ForNesting,
        ) {
            Ok(list) => Ok(Selectors(list.0.into_iter().map(Selector).collect())),
            Err(_) => Err(Error::CssParseError),
        }
    }

    /// Returns whether the given element matches this list of selectors.
    #[inline]
    #[must_use]
    pub fn matches(&self, element: &NodeDataRef<ElementData>) -> bool {
        self.0.iter().any(|s| s.matches(element))
    }

    /// Filter an element iterator, yielding those matching this list of selectors.
    #[inline]
    pub fn filter<I>(&self, iter: I) -> Select<I, &Selectors>
    where
        I: Iterator<Item = NodeDataRef<ElementData>>,
    {
        Select {
            iter,
            selectors: self,
        }
    }
}

impl Selector {
    /// Returns whether the given element matches this selector.
    #[inline]
    #[must_use]
    pub fn matches(&self, element: &NodeDataRef<ElementData>) -> bool {
        let mut cache = NthIndexCache::default();
        let mut context = matching::MatchingContext::new(
            matching::MatchingMode::Normal,
            None,
            &mut cache,
            QuirksMode::NoQuirks,
            matching::NeedsSelectorFlags::No,
            IgnoreNthChildForInvalidation::No,
        );
        matching::matches_selector(&self.0, 0, None, element, &mut context)
    }

    /// Return the specificity of this selector.
    #[must_use]
    pub fn specificity(&self) -> Specificity {
        Specificity(self.0.specificity())
    }
}

impl ::std::str::FromStr for Selectors {
    type Err = Error;
    #[inline]
    fn from_str(s: &str) -> Result<Selectors> {
        Selectors::compile(s)
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.to_css(f)
    }
}

impl fmt::Display for Selectors {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut iter = self.0.iter();
        let first = iter
            .next()
            .expect("Empty Selectors, should contain at least one selector");
        first.0.to_css(f)?;
        for selector in iter {
            f.write_str(", ")?;
            selector.0.to_css(f)?;
        }
        Ok(())
    }
}

impl fmt::Debug for Selector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Debug for Selectors {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
