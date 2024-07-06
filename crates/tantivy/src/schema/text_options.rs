use std::borrow::Cow;
use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use super::flags::{CoerceFlag, ColumnarFlag};
use crate::schema::flags::{SchemaFlagList, StoredFlag};
use crate::schema::IndexRecordOption;

/// Define how a text field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct TextOptions {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    indexing: Option<TextFieldIndexing>,
    #[serde(default)]
    stored: bool,
    #[serde(default)]
    pub(crate) columnar: ColumnFieldTextOptions,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_false")]
    /// coerce values into string if they are not of type string
    coerce: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// Enum to control how the columnar field setting of a text field.
pub(crate) enum ColumnFieldTextOptions {
    /// Flag to enable/disable
    IsEnabled(bool),
    /// Enable with tokenizer. The tokenizer must be available on the columnar field tokenizer manager.
    /// `Index::column_field_tokenizer`.
    EnabledWithTokenizer { with_tokenizer: TokenizerName },
}

impl Default for ColumnFieldTextOptions {
    fn default() -> Self {
        ColumnFieldTextOptions::IsEnabled(false)
    }
}

impl BitOr<ColumnFieldTextOptions> for ColumnFieldTextOptions {
    type Output = ColumnFieldTextOptions;

    fn bitor(self, other: ColumnFieldTextOptions) -> ColumnFieldTextOptions {
        match (self, other) {
            (
                ColumnFieldTextOptions::EnabledWithTokenizer {
                    with_tokenizer: tokenizer,
                },
                _,
            )
            | (
                _,
                ColumnFieldTextOptions::EnabledWithTokenizer {
                    with_tokenizer: tokenizer,
                },
            ) => ColumnFieldTextOptions::EnabledWithTokenizer {
                with_tokenizer: tokenizer,
            },
            (ColumnFieldTextOptions::IsEnabled(true), _)
            | (_, ColumnFieldTextOptions::IsEnabled(true)) => {
                ColumnFieldTextOptions::IsEnabled(true)
            }
            (_, ColumnFieldTextOptions::IsEnabled(false)) => {
                ColumnFieldTextOptions::IsEnabled(false)
            }
        }
    }
}

fn is_false(val: &bool) -> bool {
    !val
}

impl TextOptions {
    /// Returns the indexing options.
    #[inline]
    pub fn get_indexing_options(&self) -> Option<&TextFieldIndexing> {
        self.indexing.as_ref()
    }

    /// Returns true if the text is to be stored.
    #[inline]
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns true if and only if the value is a columnar field.
    #[inline]
    pub fn is_columnar(&self) -> bool {
        matches!(self.columnar, ColumnFieldTextOptions::IsEnabled(true))
            || matches!(
                &self.columnar,
                ColumnFieldTextOptions::EnabledWithTokenizer { with_tokenizer: _ }
            )
    }

    /// Returns true if and only if the value is a columnar field.
    #[inline]
    pub fn get_column_field_tokenizer_name(&self) -> Option<&str> {
        match &self.columnar {
            ColumnFieldTextOptions::IsEnabled(true) | ColumnFieldTextOptions::IsEnabled(false) => {
                None
            }
            ColumnFieldTextOptions::EnabledWithTokenizer {
                with_tokenizer: tokenizer,
            } => Some(tokenizer.name()),
        }
    }

    /// Returns true if values should be coerced to strings (numbers, null).
    #[inline]
    pub fn should_coerce(&self) -> bool {
        self.coerce
    }

    /// Set the field as a columnar field.
    ///
    /// Columnar fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// Text columnar fields will have the term ids stored in the columnar field.
    ///
    /// The effective cardinality depends on the tokenizer. Without a tokenizer, the text will be
    /// stored as is, which equals to the "raw" tokenizer. The tokenizer can be used to apply
    /// normalization like lower case.
    /// The passed tokenizer_name must be available on the columnar field tokenizer manager.
    /// `Index::column_field_tokenizer`.
    ///
    /// The original text can be retrieved via
    /// [`TermDictionary::ord_to_term()`](crate::termdict::TermDictionary::ord_to_term)
    /// from the dictionary.
    #[must_use]
    pub fn set_columnar(mut self, tokenizer_name: Option<&str>) -> TextOptions {
        if let Some(tokenizer) = tokenizer_name {
            let tokenizer = TokenizerName::from_name(tokenizer);
            self.columnar = ColumnFieldTextOptions::EnabledWithTokenizer {
                with_tokenizer: tokenizer,
            }
        } else {
            self.columnar = ColumnFieldTextOptions::IsEnabled(true);
        }
        self
    }

    /// Coerce values if they are not of type string. Defaults to false.
    #[must_use]
    pub fn set_coerce(mut self) -> TextOptions {
        self.coerce = true;
        self
    }

    /// Sets the field as stored.
    #[must_use]
    pub fn set_stored(mut self) -> TextOptions {
        self.stored = true;
        self
    }

    /// Sets the field as indexed, with the specific indexing options.
    #[must_use]
    pub fn set_indexing_options(mut self, indexing: TextFieldIndexing) -> TextOptions {
        self.indexing = Some(indexing);
        self
    }
}

#[derive(Clone, PartialEq, Debug, Eq, Serialize, Deserialize)]
pub(crate) struct TokenizerName(Cow<'static, str>);

const DEFAULT_TOKENIZER_NAME: &str = "default";

const NO_TOKENIZER_NAME: &str = "raw";

impl Default for TokenizerName {
    fn default() -> Self {
        TokenizerName::from_static(DEFAULT_TOKENIZER_NAME)
    }
}

impl TokenizerName {
    pub const fn from_static(name: &'static str) -> Self {
        TokenizerName(Cow::Borrowed(name))
    }
    pub(crate) fn from_name(name: &str) -> Self {
        TokenizerName(Cow::Owned(name.to_string()))
    }
    pub(crate) fn name(&self) -> &str {
        &self.0
    }
}

/// Configuration defining indexing for a text field.
///
/// It defines
/// - The amount of information that should be stored about the presence of a term in a document.
///     Essentially, should we store the term frequency and/or the positions (See
///     [`IndexRecordOption`]).
/// - The name of the `Tokenizer` that should be used to process the field.
/// - Flag indicating, if fieldnorms should be stored (See [fieldnorm](crate::fieldnorm)). Defaults
///   to `true`.
#[derive(Clone, PartialEq, Debug, Eq, Serialize, Deserialize)]
pub struct TextFieldIndexing {
    #[serde(default)]
    record: IndexRecordOption,
    #[serde(default = "default_fieldnorms")]
    fieldnorms: bool,
    #[serde(default)]
    tokenizer: TokenizerName,
}

pub(crate) fn default_fieldnorms() -> bool {
    true
}

impl Default for TextFieldIndexing {
    fn default() -> TextFieldIndexing {
        TextFieldIndexing {
            tokenizer: TokenizerName::default(),
            record: IndexRecordOption::default(),
            fieldnorms: default_fieldnorms(),
        }
    }
}

impl TextFieldIndexing {
    /// Sets the tokenizer to be used for a given field.
    #[must_use]
    pub fn set_tokenizer(mut self, tokenizer_name: &str) -> TextFieldIndexing {
        self.tokenizer = TokenizerName::from_name(tokenizer_name);
        self
    }

    /// Returns the tokenizer that will be used for this field.
    pub fn tokenizer(&self) -> &str {
        self.tokenizer.name()
    }

    /// Sets fieldnorms
    #[must_use]
    pub fn set_fieldnorms(mut self, fieldnorms: bool) -> TextFieldIndexing {
        self.fieldnorms = fieldnorms;
        self
    }

    /// Returns true if and only if [fieldnorms](crate::fieldnorm) are stored.
    pub fn fieldnorms(&self) -> bool {
        self.fieldnorms
    }

    /// Sets which information should be indexed with the tokens.
    ///
    /// See [`IndexRecordOption`] for more detail.
    #[must_use]
    pub fn set_index_option(mut self, index_option: IndexRecordOption) -> TextFieldIndexing {
        self.record = index_option;
        self
    }

    /// Returns the indexing options associated with this field.
    ///
    /// See [`IndexRecordOption`] for more detail.
    pub fn index_option(&self) -> IndexRecordOption {
        self.record
    }
}

/// The field will be untokenized and indexed.
pub const STRING: TextOptions = TextOptions {
    indexing: Some(TextFieldIndexing {
        tokenizer: TokenizerName::from_static(NO_TOKENIZER_NAME),
        fieldnorms: true,
        record: IndexRecordOption::Basic,
    }),
    stored: false,
    columnar: ColumnFieldTextOptions::IsEnabled(false),
    coerce: false,
};

/// The field will be tokenized and indexed.
pub const TEXT: TextOptions = TextOptions {
    indexing: Some(TextFieldIndexing {
        tokenizer: TokenizerName::from_static(DEFAULT_TOKENIZER_NAME),
        fieldnorms: true,
        record: IndexRecordOption::WithFreqsAndPositions,
    }),
    stored: false,
    coerce: false,
    columnar: ColumnFieldTextOptions::IsEnabled(false),
};

impl<T: Into<TextOptions>> BitOr<T> for TextOptions {
    type Output = TextOptions;

    fn bitor(self, other: T) -> TextOptions {
        let other = other.into();
        TextOptions {
            indexing: self.indexing.or(other.indexing),
            stored: self.stored | other.stored,
            columnar: self.columnar | other.columnar,
            coerce: self.coerce | other.coerce,
        }
    }
}

impl From<()> for TextOptions {
    fn from(_: ()) -> TextOptions {
        TextOptions::default()
    }
}

impl From<StoredFlag> for TextOptions {
    fn from(_: StoredFlag) -> TextOptions {
        TextOptions {
            indexing: None,
            stored: true,
            columnar: ColumnFieldTextOptions::default(),
            coerce: false,
        }
    }
}

impl From<CoerceFlag> for TextOptions {
    fn from(_: CoerceFlag) -> TextOptions {
        TextOptions {
            indexing: None,
            stored: false,
            columnar: ColumnFieldTextOptions::default(),
            coerce: true,
        }
    }
}

impl From<ColumnarFlag> for TextOptions {
    fn from(_: ColumnarFlag) -> TextOptions {
        TextOptions {
            indexing: None,
            stored: false,
            columnar: ColumnFieldTextOptions::IsEnabled(true),
            coerce: false,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for TextOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::text_options::{ColumnFieldTextOptions, TokenizerName};
    use crate::schema::*;

    #[test]
    fn test_field_options() {
        let field_options = STORED | TEXT;
        assert!(field_options.is_stored());
        assert!(field_options.get_indexing_options().is_some());
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let field = schema.get_field("body").unwrap();
        let field_entry = schema.get_field_entry(field);
        assert!(matches!(field_entry.field_type(),
                FieldType::Str(text_options)
                if text_options.get_indexing_options().unwrap().tokenizer() == "default"));
    }

    #[test]
    fn test_cmp_index_record_option() {
        assert!(IndexRecordOption::WithFreqsAndPositions > IndexRecordOption::WithFreqs);
        assert!(IndexRecordOption::WithFreqs > IndexRecordOption::Basic);
    }

    #[test]
    fn serde_default_test() {
        let json = r#"
        {
            "indexing": {
                "record": "basic",
                "fieldnorms": true,
                "tokenizer": "default"
            },
            "stored": false
        }
        "#;
        let options: TextOptions = serde_json::from_str(json).unwrap();
        let options2: TextOptions = serde_json::from_str("{\"indexing\": {}}").unwrap();
        assert_eq!(options, options2);
        assert_eq!(options.indexing.unwrap().record, IndexRecordOption::Basic);
        let options3: TextOptions = serde_json::from_str("{}").unwrap();
        assert_eq!(options3.indexing, None);
    }

    #[test]
    fn serde_column_field_tokenizer() {
        let json = r#" {
            "columnar": { "with_tokenizer": "default" }
        } "#;
        let options: TextOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            options.columnar,
            ColumnFieldTextOptions::EnabledWithTokenizer {
                with_tokenizer: TokenizerName::from_static("default")
            }
        );
        let options: TextOptions =
            serde_json::from_str(&serde_json::to_string(&options).unwrap()).unwrap();
        assert_eq!(
            options.columnar,
            ColumnFieldTextOptions::EnabledWithTokenizer {
                with_tokenizer: TokenizerName::from_static("default")
            }
        );

        let json = r#" {
            "columnar": true
        } "#;
        let options: TextOptions = serde_json::from_str(json).unwrap();
        assert_eq!(options.columnar, ColumnFieldTextOptions::IsEnabled(true));
        let options: TextOptions =
            serde_json::from_str(&serde_json::to_string(&options).unwrap()).unwrap();
        assert_eq!(options.columnar, ColumnFieldTextOptions::IsEnabled(true));

        let json = r#" {
            "columnar": false
        } "#;
        let options: TextOptions = serde_json::from_str(json).unwrap();
        assert_eq!(options.columnar, ColumnFieldTextOptions::IsEnabled(false));
        let options: TextOptions =
            serde_json::from_str(&serde_json::to_string(&options).unwrap()).unwrap();
        assert_eq!(options.columnar, ColumnFieldTextOptions::IsEnabled(false));
    }
}
