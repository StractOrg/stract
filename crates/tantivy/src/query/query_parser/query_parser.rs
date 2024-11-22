use std::net::{AddrParseError, IpAddr};
use std::num::{ParseFloatError, ParseIntError};
use std::ops::Bound;
use std::str::{FromStr, ParseBoolError};

use crate::query::grammar::{UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use itertools::Itertools;
use rustc_hash::FxHashMap;

use super::logical_ast::*;
use crate::index::Index;
use crate::json_utils::convert_to_columnar_value_and_append_to_json_term;
use crate::query::range_query::{is_type_valid_for_columnfield_range_query, RangeQuery};
use crate::query::{
    AllQuery, BooleanQuery, BoostQuery, EmptyQuery, FuzzyTermQuery, Occur, PhrasePrefixQuery,
    PhraseQuery, Query, TermQuery, TermSetQuery,
};
use crate::schema::{
    Field, FieldType, IndexRecordOption, IntoIpv6Addr, JsonObjectOptions, Schema, Term,
    TextFieldIndexing, Type,
};
use crate::time::format_description::well_known::Rfc3339;
use crate::time::OffsetDateTime;
use crate::tokenizer::{TextAnalyzer, TokenizerManager};
use crate::{DateTime, Score};

/// Possible error that may happen when parsing a query.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum QueryParserError {
    /// Error in the query syntax
    #[error("Syntax Error: {0}")]
    SyntaxError(String),
    /// This query is unsupported.
    #[error("Unsupported query: {0}")]
    UnsupportedQuery(String),
    /// The query references a field that is not in the schema
    #[error("Field does not exist: '{0}'")]
    FieldDoesNotExist(String),
    /// The query contains a term for a `u64` or `i64`-field, but the value
    /// is neither.
    #[error("Expected a valid integer: '{0:?}'")]
    ExpectedInt(#[from] ParseIntError),
    /// The query contains a term for a bytes field, but the value is not valid
    /// base64.
    #[error("Expected base64: '{0:?}'")]
    ExpectedBase64(#[from] base64::DecodeError),
    /// The query contains a term for a `f64`-field, but the value
    /// is not a f64.
    #[error("Invalid query: Only excluding terms given")]
    ExpectedFloat(#[from] ParseFloatError),
    /// The query contains a term for a bool field, but the value
    /// is not a bool.
    #[error("Expected a bool value: '{0:?}'")]
    ExpectedBool(#[from] ParseBoolError),
    /// It is forbidden queries that are only "excluding". (e.g. -title:pop)
    #[error("Invalid query: Only excluding terms given")]
    AllButQueryForbidden,
    /// If no default field is declared, running a query without any
    /// field specified is forbbidden.
    #[error("No default field declared and no field specified in query")]
    NoDefaultFieldDeclared,
    /// The field searched for is not declared
    /// as indexed in the schema.
    #[error("The field '{0}' is not declared as indexed")]
    FieldNotIndexed(String),
    /// A phrase query was requested for a field that does not
    /// have any positions indexed.
    #[error("The field '{0}' does not have positions indexed")]
    FieldDoesNotHavePositionsIndexed(String),
    /// A phrase-prefix query requires at least two terms
    #[error(
        "The phrase '{phrase:?}' does not produce at least two terms using the tokenizer \
         '{tokenizer:?}'"
    )]
    PhrasePrefixRequiresAtLeastTwoTerms {
        /// The phrase which triggered the issue
        phrase: String,
        /// The tokenizer configured for the field
        tokenizer: String,
    },
    /// The tokenizer for the given field is unknown
    /// The two argument strings are the name of the field, the name of the tokenizer
    #[error("The tokenizer '{tokenizer:?}' for the field '{field:?}' is unknown")]
    UnknownTokenizer {
        /// The name of the tokenizer
        tokenizer: String,
        /// The field name
        field: String,
    },
    /// The query contains a range query with a phrase as one of the bounds.
    /// Only terms can be used as bounds.
    #[error("A range query cannot have a phrase as one of the bounds")]
    RangeMustNotHavePhrase,
    /// The format for the date field is not RFC 3339 compliant.
    #[error("The date field has an invalid format")]
    DateFormatError(#[from] time::error::Parse),
    /// The format for the ip field is invalid.
    #[error("The ip field is malformed: {0}")]
    IpFormatError(#[from] AddrParseError),
}

/// Recursively remove empty clause from the AST
///
/// Returns `None` if and only if the `logical_ast` ended up being empty.
fn trim_ast(logical_ast: LogicalAst) -> Option<LogicalAst> {
    match logical_ast {
        LogicalAst::Clause(children) => {
            let trimmed_children = children
                .into_iter()
                .flat_map(|(occur, child)| {
                    trim_ast(child).map(|trimmed_child| (occur, trimmed_child))
                })
                .collect::<Vec<_>>();
            if trimmed_children.is_empty() {
                None
            } else {
                Some(LogicalAst::Clause(trimmed_children))
            }
        }
        _ => Some(logical_ast),
    }
}

/// Tantivy's Query parser
///
/// The language covered by the current parser is extremely simple.
///
/// * simple terms: "e.g.: `Barack Obama` will be seen as a sequence of two tokens Barack and Obama.
///   By default, the query parser will interpret this as a disjunction (see
///   `.set_conjunction_by_default()`) and will match all documents that contains either "Barack" or
///   "Obama" or both. Since we did not target a specific field, the query parser will look into the
///   so-called default fields (as set up in the constructor).
///
///   Assuming that the default fields are `body` and `title`, and the query parser is set with
///     conjunction   as a default, our query will be interpreted as.
///   `(body:Barack OR title:Barack) AND (title:Obama OR body:Obama)`.
///   By default, all tokenized and indexed fields are default fields.
///
///   It is possible to explicitly target a field by prefixing the text by the `fieldname:`.
///   Note this only applies to the term directly following.
///   For instance, assuming the query parser is configured to use conjunction by default,
///   `body:Barack Obama` is not interpreted as `body:Barack AND body:Obama` but as
///   `body:Barack OR (body:Barack OR text:Obama)` .
///
/// * boolean operators `AND`, `OR`. `AND` takes precedence over `OR`, so that `a AND b OR c` is
///   interpreted
///     as `(a AND b) OR c`.
///
/// * In addition to the boolean operators, the `-`, `+` can help define. These operators are
///   sufficient to express all queries using boolean operators. For instance `x AND y OR z` can be
///   written (`(+x +y) z`). In addition, these operators can help define "required optional"
///   queries. `(+x y)` matches the same document set as simply `x`, but `y` will help refining the
///   score.
///
/// * negative terms: By prepending a term by a `-`, a term can be excluded from the search. This is
///   useful for disambiguating a query. e.g. `apple -fruit`
///
/// * must terms: By prepending a term by a `+`, a term can be made required for the search.
///
/// * phrase terms: Quoted terms become phrase searches on fields that have positions indexed. e.g.,
///   `title:"Barack Obama"` will only find documents that have "barack" immediately followed by
///   "obama". Single quotes can also be used. If the text to be searched contains quotation mark,
///   it is possible to escape them with a `\`.
///
/// * range terms: Range searches can be done by specifying the start and end bound. These can be
///   inclusive or exclusive. e.g., `title:[a TO c}` will find all documents whose title contains a
///   word lexicographically between `a` and `c` (inclusive lower bound, exclusive upper bound).
///   Inclusive bounds are `[]`, exclusive are `{}`.
///
/// * set terms: Using the `IN` operator, a field can be matched against a set of literals, e.g.
///   `title: IN [a b cd]` will match documents where `title` is either `a`, `b` or `cd`, but do so
///   more efficiently than the alternative query `title:a OR title:b OR title:c` does.
///
/// * date values: The query parser supports rfc3339 formatted dates. For example
///   `"2002-10-02T15:00:00.05Z"` or `some_date_field:[2002-10-02T15:00:00Z TO
///   2002-10-02T18:00:00Z}`
///
/// * all docs query: A plain `*` will match all documents in the index.
///
/// Parts of the queries can be boosted by appending `^boostfactor`.
/// For instance, `"SRE"^2.0 OR devops^0.4` will boost documents containing `SRE` instead of
/// devops. Negative boosts are not allowed.
///
/// It is also possible to define a boost for a some specific field, at the query parser level.
/// (See [`set_field_boost(...)`](QueryParser::set_field_boost)). Typically you may want to boost a
/// title field.
///
/// Additionally, specific fields can be marked to use fuzzy term queries for each literal
/// via the [`QueryParser::set_field_fuzzy`] method.
///
/// Phrase terms support the `~` slop operator which allows to set the phrase's matching
/// distance in words. `"big wolf"~1` will return documents containing the phrase `"big bad wolf"`.
///
/// Phrase terms also support the `*` prefix operator which switches the phrase's matching
/// to consider all documents which contain the last term as a prefix, e.g. `"big bad wo"*` will
/// match `"big bad wolf"`.
#[derive(Clone)]
pub struct QueryParser {
    schema: Schema,
    default_fields: Vec<Field>,
    conjunction_by_default: bool,
    tokenizer_manager: TokenizerManager,
    boost: FxHashMap<Field, Score>,
    fuzzy: FxHashMap<Field, Fuzzy>,
}

#[derive(Clone)]
struct Fuzzy {
    prefix: bool,
    distance: u8,
    transpose_cost_one: bool,
}

fn all_negative(ast: &LogicalAst) -> bool {
    match ast {
        LogicalAst::Leaf(_) => false,
        LogicalAst::Boost(ref child_ast, _) => all_negative(child_ast),
        LogicalAst::Clause(children) => children
            .iter()
            .all(|(ref occur, child)| (*occur == Occur::MustNot) || all_negative(child)),
    }
}

// Make an all-negative ast into a normal ast. Must not be used on an already okay ast.
fn make_non_negative(ast: &mut LogicalAst) {
    match ast {
        LogicalAst::Leaf(_) => (),
        LogicalAst::Boost(ref mut child_ast, _) => make_non_negative(child_ast),
        LogicalAst::Clause(children) => children.push((Occur::Should, LogicalLiteral::All.into())),
    }
}

/// Similar to the try/? macro, but returns a tuple of (None, Vec<Error>) instead of Err(Error)
macro_rules! try_tuple {
    ($expr:expr) => {{
        match $expr {
            Ok(val) => val,
            Err(e) => return (None, vec![e.into()]),
        }
    }};
}

impl QueryParser {
    /// Creates a `QueryParser`, given
    /// * schema - index Schema
    /// * default_fields - fields used to search if no field is specifically defined in the query.
    pub fn new(
        schema: Schema,
        default_fields: Vec<Field>,
        tokenizer_manager: TokenizerManager,
    ) -> QueryParser {
        QueryParser {
            schema,
            default_fields,
            tokenizer_manager,
            conjunction_by_default: false,
            boost: Default::default(),
            fuzzy: Default::default(),
        }
    }

    // Splits a full_path as written in a query, into a field name and a
    // json path.
    pub(crate) fn split_full_path<'a>(&self, full_path: &'a str) -> Option<(Field, &'a str)> {
        self.schema.find_field(full_path)
    }

    /// Creates a `QueryParser`, given
    ///  * an index
    ///  * a set of default fields used to search if no field is specifically defined
    ///     in the query.
    pub fn for_index(index: &Index, default_fields: Vec<Field>) -> QueryParser {
        QueryParser::new(index.schema(), default_fields, index.tokenizers().clone())
    }

    /// Set the default way to compose queries to a conjunction.
    ///
    /// By default, the query `happy tax payer` is equivalent to the query
    /// `happy OR tax OR payer`. After calling `.set_conjunction_by_default()`
    /// `happy tax payer` will be interpreted by the parser as `happy AND tax AND payer`.
    pub fn set_conjunction_by_default(&mut self) {
        self.conjunction_by_default = true;
    }

    /// Sets a boost for a specific field.
    ///
    /// The parse query will automatically boost this field.
    ///
    /// If the query defines a query boost through the query language (e.g: `country:France^3.0`),
    /// the two boosts (the one defined in the query, and the one defined in the `QueryParser`)
    /// are multiplied together.
    pub fn set_field_boost(&mut self, field: Field, boost: Score) {
        self.boost.insert(field, boost);
    }

    /// Sets the given [field][`Field`] to use [fuzzy term queries][`FuzzyTermQuery`]
    ///
    /// If set, the parse will produce queries using fuzzy term queries
    /// with the given parameters for each literal matched against the given field.
    ///
    /// See the [`FuzzyTermQuery::new`] and [`FuzzyTermQuery::new_prefix`] methods
    /// for the meaning of the individual parameters.
    pub fn set_field_fuzzy(
        &mut self,
        field: Field,
        prefix: bool,
        distance: u8,
        transpose_cost_one: bool,
    ) {
        self.fuzzy.insert(
            field,
            Fuzzy {
                prefix,
                distance,
                transpose_cost_one,
            },
        );
    }

    /// Parse a query
    ///
    /// Note that `parse_query` returns an error if the input
    /// is not a valid query.
    pub fn parse_query(&self, query: &str) -> Result<Box<dyn Query>, QueryParserError> {
        let logical_ast = self.parse_query_to_logical_ast(query)?;
        Ok(convert_to_query(&self.fuzzy, logical_ast))
    }

    /// Parse a query leniently
    ///
    /// This variant parses invalid query on a best effort basis. If some part of the query can't
    /// reasonably be executed (range query without field, searching on a non existing field,
    /// searching without precising field when no default field is provided...), they may get
    /// turned into a "match-nothing" subquery.
    ///
    /// In case it encountered such issues, they are reported as a Vec of errors.
    pub fn parse_query_lenient(&self, query: &str) -> (Box<dyn Query>, Vec<QueryParserError>) {
        let (logical_ast, errors) = self.parse_query_to_logical_ast_lenient(query);
        (convert_to_query(&self.fuzzy, logical_ast), errors)
    }

    /// Build a query from an already parsed user input AST
    ///
    /// This can be useful if the user input AST parsed using [`crate::query::grammar`]
    /// needs to be inspected before the query is re-interpreted w.r.t.
    /// index specifics like field names and tokenizers.
    pub fn build_query_from_user_input_ast(
        &self,
        user_input_ast: UserInputAst,
    ) -> Result<Box<dyn Query>, QueryParserError> {
        let (logical_ast, mut err) = self.compute_logical_ast_lenient(user_input_ast);
        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }
        Ok(convert_to_query(&self.fuzzy, logical_ast))
    }

    /// Build leniently a query from an already parsed user input AST.
    ///
    /// See also [`QueryParser::build_query_from_user_input_ast`]
    pub fn build_query_from_user_input_ast_lenient(
        &self,
        user_input_ast: UserInputAst,
    ) -> (Box<dyn Query>, Vec<QueryParserError>) {
        let (logical_ast, errors) = self.compute_logical_ast_lenient(user_input_ast);
        (convert_to_query(&self.fuzzy, logical_ast), errors)
    }

    /// Parse the user query into an AST.
    fn parse_query_to_logical_ast(&self, query: &str) -> Result<LogicalAst, QueryParserError> {
        let user_input_ast = crate::query::grammar::parse_query(query)
            .map_err(|_| QueryParserError::SyntaxError(query.to_string()))?;
        let (ast, mut err) = self.compute_logical_ast_lenient(user_input_ast);
        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }
        Ok(ast)
    }

    /// Parse the user query into an AST.
    fn parse_query_to_logical_ast_lenient(
        &self,
        query: &str,
    ) -> (LogicalAst, Vec<QueryParserError>) {
        let (user_input_ast, errors) = crate::query::grammar::parse_query_lenient(query);
        let mut errors: Vec<_> = errors
            .into_iter()
            .map(|error| {
                QueryParserError::SyntaxError(format!(
                    "{} at position {}",
                    error.message, error.pos
                ))
            })
            .collect();
        let (ast, mut ast_errors) = self.compute_logical_ast_lenient(user_input_ast);
        errors.append(&mut ast_errors);
        (ast, errors)
    }

    fn compute_logical_ast_lenient(
        &self,
        user_input_ast: UserInputAst,
    ) -> (LogicalAst, Vec<QueryParserError>) {
        let (mut ast, mut err) = self.compute_logical_ast_with_occur_lenient(user_input_ast);
        if let LogicalAst::Clause(children) = &ast {
            if children.is_empty() {
                return (ast, err);
            }
        }
        if all_negative(&ast) {
            err.push(QueryParserError::AllButQueryForbidden);
            make_non_negative(&mut ast);
        }
        (ast, err)
    }

    fn compute_boundary_term(
        &self,
        field: Field,
        json_path: &str,
        phrase: &str,
    ) -> Result<Term, QueryParserError> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let field_supports_ff_range_queries = field_type.is_columnar()
            && is_type_valid_for_columnfield_range_query(field_type.value_type());

        if !field_type.is_indexed() && !field_supports_ff_range_queries {
            return Err(QueryParserError::FieldNotIndexed(
                field_entry.name().to_string(),
            ));
        }
        if !json_path.is_empty() && field_type.value_type() != Type::Json {
            return Err(QueryParserError::UnsupportedQuery(format!(
                "Json path is not supported for field {:?}",
                field_entry.name()
            )));
        }
        match *field_type {
            FieldType::U64(_) => {
                let val: u64 = u64::from_str(phrase)?;
                Ok(Term::from_field_u64(field, val))
            }
            FieldType::U128(_) => {
                let val: u128 = u128::from_str(phrase)?;
                Ok(Term::from_field_u128(field, val))
            }
            FieldType::I64(_) => {
                let val: i64 = i64::from_str(phrase)?;
                Ok(Term::from_field_i64(field, val))
            }
            FieldType::F64(_) => {
                let val: f64 = f64::from_str(phrase)?;
                Ok(Term::from_field_f64(field, val))
            }
            FieldType::Bool(_) => {
                let val: bool = bool::from_str(phrase)?;
                Ok(Term::from_field_bool(field, val))
            }
            FieldType::Date(_) => {
                let dt = OffsetDateTime::parse(phrase, &Rfc3339)?;
                Ok(Term::from_field_date(field, DateTime::from_utc(dt)))
            }
            FieldType::Str(ref str_options) => {
                let option = str_options.get_indexing_options().ok_or_else(|| {
                    // This should have been seen earlier really.
                    QueryParserError::FieldNotIndexed(field_entry.name().to_string())
                })?;
                let mut text_analyzer =
                    self.tokenizer_manager
                        .get(option.tokenizer())
                        .ok_or_else(|| QueryParserError::UnknownTokenizer {
                            field: field_entry.name().to_string(),
                            tokenizer: option.tokenizer().to_string(),
                        })?;
                let mut terms: Vec<Term> = Vec::new();
                let mut token_stream = text_analyzer.token_stream(phrase);
                token_stream.process(&mut |token| {
                    let term = Term::from_field_text(field, &token.text);
                    terms.push(term);
                });
                if terms.len() != 1 {
                    return Err(QueryParserError::UnsupportedQuery(format!(
                        "Range query boundary cannot have multiple tokens: {phrase:?}."
                    )));
                }
                Ok(terms.into_iter().next().unwrap())
            }
            FieldType::JsonObject(_) => {
                // Json range are not supported.
                Err(QueryParserError::UnsupportedQuery(
                    "Range query are not supported on json field.".to_string(),
                ))
            }
            FieldType::Bytes(_) => {
                let bytes = BASE64
                    .decode(phrase)
                    .map_err(QueryParserError::ExpectedBase64)?;
                Ok(Term::from_field_bytes(field, &bytes))
            }
            FieldType::IpAddr(_) => {
                let ip_v6 = IpAddr::from_str(phrase)?.into_ipv6_addr();
                Ok(Term::from_field_ip_addr(field, ip_v6))
            }
        }
    }

    fn compute_logical_ast_for_leaf(
        &self,
        field: Field,
        json_path: &str,
        phrase: &str,
        slop: u32,
        prefix: bool,
    ) -> Result<Vec<LogicalLiteral>, QueryParserError> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let field_name = field_entry.name();
        if !field_type.is_indexed() {
            return Err(QueryParserError::FieldNotIndexed(field_name.to_string()));
        }
        if field_type.value_type() != Type::Json && !json_path.is_empty() {
            let field_name = self.schema.get_field_name(field);
            return Err(QueryParserError::FieldDoesNotExist(format!(
                "{field_name}.{json_path}"
            )));
        }
        match *field_type {
            FieldType::U64(_) => {
                let val: u64 = u64::from_str(phrase)?;
                let i64_term = Term::from_field_u64(field, val);
                Ok(vec![LogicalLiteral::Term(i64_term)])
            }
            FieldType::U128(_) => {
                let val: u128 = u128::from_str(phrase)?;
                let u128_term = Term::from_field_u128(field, val);
                Ok(vec![LogicalLiteral::Term(u128_term)])
            }
            FieldType::I64(_) => {
                let val: i64 = i64::from_str(phrase)?;
                let i64_term = Term::from_field_i64(field, val);
                Ok(vec![LogicalLiteral::Term(i64_term)])
            }
            FieldType::F64(_) => {
                let val: f64 = f64::from_str(phrase)?;
                let f64_term = Term::from_field_f64(field, val);
                Ok(vec![LogicalLiteral::Term(f64_term)])
            }
            FieldType::Bool(_) => {
                let val: bool = bool::from_str(phrase)?;
                let bool_term = Term::from_field_bool(field, val);
                Ok(vec![LogicalLiteral::Term(bool_term)])
            }
            FieldType::Date(_) => {
                let dt = OffsetDateTime::parse(phrase, &Rfc3339)?;
                let dt_term = Term::from_field_date(field, DateTime::from_utc(dt));
                Ok(vec![LogicalLiteral::Term(dt_term)])
            }
            FieldType::Str(ref str_options) => {
                let indexing_options = str_options.get_indexing_options().ok_or_else(|| {
                    // This should have been seen earlier really.
                    QueryParserError::FieldNotIndexed(field_name.to_string())
                })?;
                let mut text_analyzer = self
                    .tokenizer_manager
                    .get(indexing_options.tokenizer())
                    .ok_or_else(|| QueryParserError::UnknownTokenizer {
                        field: field_name.to_string(),
                        tokenizer: indexing_options.tokenizer().to_string(),
                    })?;
                Ok(generate_literals_for_str(
                    field_name,
                    field,
                    phrase,
                    slop,
                    prefix,
                    indexing_options,
                    &mut text_analyzer,
                )?
                .into_iter()
                .collect())
            }
            FieldType::JsonObject(ref json_options) => generate_literals_for_json_object(
                field_name,
                field,
                json_path,
                phrase,
                &self.tokenizer_manager,
                json_options,
            ),
            FieldType::Bytes(_) => {
                let bytes = BASE64
                    .decode(phrase)
                    .map_err(QueryParserError::ExpectedBase64)?;
                let bytes_term = Term::from_field_bytes(field, &bytes);
                Ok(vec![LogicalLiteral::Term(bytes_term)])
            }
            FieldType::IpAddr(_) => {
                let ip_v6 = IpAddr::from_str(phrase)?.into_ipv6_addr();
                let term = Term::from_field_ip_addr(field, ip_v6);
                Ok(vec![LogicalLiteral::Term(term)])
            }
        }
    }

    fn default_occur(&self) -> Occur {
        if self.conjunction_by_default {
            Occur::Must
        } else {
            Occur::Should
        }
    }

    fn resolve_bound(
        &self,
        field: Field,
        json_path: &str,
        bound: &UserInputBound,
    ) -> Result<Bound<Term>, QueryParserError> {
        if bound.term_str() == "*" {
            return Ok(Bound::Unbounded);
        }
        let term = self.compute_boundary_term(field, json_path, bound.term_str())?;
        match *bound {
            UserInputBound::Inclusive(_) => Ok(Bound::Included(term)),
            UserInputBound::Exclusive(_) => Ok(Bound::Excluded(term)),
            UserInputBound::Unbounded => Ok(Bound::Unbounded),
        }
    }

    fn compute_logical_ast_with_occur_lenient(
        &self,
        user_input_ast: UserInputAst,
    ) -> (LogicalAst, Vec<QueryParserError>) {
        match user_input_ast {
            UserInputAst::Clause(sub_queries) => {
                let default_occur = self.default_occur();
                let mut logical_sub_queries: Vec<(Occur, LogicalAst)> = Vec::new();
                let mut errors = Vec::new();
                for (occur_opt, sub_ast) in sub_queries {
                    let (sub_ast, mut sub_errors) =
                        self.compute_logical_ast_with_occur_lenient(sub_ast);
                    let occur = occur_opt.unwrap_or(default_occur);
                    logical_sub_queries.push((occur, sub_ast));
                    errors.append(&mut sub_errors);
                }
                (LogicalAst::Clause(logical_sub_queries), errors)
            }
            UserInputAst::Boost(ast, boost) => {
                let (ast, errors) = self.compute_logical_ast_with_occur_lenient(*ast);
                (ast.boost(boost as Score), errors)
            }
            UserInputAst::Leaf(leaf) => {
                let (ast, errors) = self.compute_logical_ast_from_leaf_lenient(*leaf);
                // if the error is not recoverable, replace it with an empty clause. We will end up
                // trimming those later
                (
                    ast.unwrap_or_else(|| LogicalAst::Clause(Vec::new())),
                    errors,
                )
            }
        }
    }

    fn field_boost(&self, field: Field) -> Score {
        self.boost.get(&field).cloned().unwrap_or(1.0)
    }

    fn default_indexed_json_fields(&self) -> impl Iterator<Item = Field> + '_ {
        let schema = self.schema.clone();
        self.default_fields.iter().cloned().filter(move |field| {
            let field_type = schema.get_field_entry(*field).field_type();
            field_type.value_type() == Type::Json && field_type.is_indexed()
        })
    }

    /// Given a literal, returns the list of terms that should be searched.
    ///
    /// The terms are identified by a triplet:
    /// - tantivy field
    /// - field_path: tantivy has JSON fields. It is possible to target a member of a JSON
    ///     object by naturally extending the json field name with a "." separated field_path
    /// - field_phrase: the phrase that is being searched.
    ///
    /// The literal identifies the targeted field by a so-called *full field path*,
    /// specified before the ":". (e.g. identity.username:fulmicoton).
    ///
    /// The way we split the full field path into (field_name, field_path) can be ambiguous,
    /// because field_names can contain "." themselves.
    // For instance if a field is named `one.two` and another one is named `one`,
    /// should `one.two:three` target `one.two` with field path `` or or `one` with
    /// the field path `two`.
    ///
    /// In this case tantivy, just picks the solution with the longest field name.
    ///
    /// Quirk: As a hack for quickwit, we do not split over a dot that appear escaped '\.'.
    fn compute_path_triplets_for_literal<'a>(
        &self,
        literal: &'a UserInputLiteral,
    ) -> Result<Vec<(Field, &'a str, &'a str)>, QueryParserError> {
        let full_path = if let Some(full_path) = &literal.field_name {
            full_path
        } else {
            // The user did not specify any path...
            // We simply target default fields.
            if self.default_fields.is_empty() {
                return Err(QueryParserError::NoDefaultFieldDeclared);
            }
            return Ok(self
                .default_fields
                .iter()
                .map(|default_field| (*default_field, "", literal.phrase.as_str()))
                .collect::<Vec<(Field, &str, &str)>>());
        };
        if let Some((field, path)) = self.split_full_path(full_path) {
            return Ok(vec![(field, path, literal.phrase.as_str())]);
        }
        // We need to add terms associated with json default fields.
        let triplets: Vec<(Field, &str, &str)> = self
            .default_indexed_json_fields()
            .map(|json_field| (json_field, full_path.as_str(), literal.phrase.as_str()))
            .collect();
        if triplets.is_empty() {
            return Err(QueryParserError::FieldDoesNotExist(full_path.to_string()));
        }
        Ok(triplets)
    }

    fn compute_logical_ast_from_leaf_lenient(
        &self,
        leaf: UserInputLeaf,
    ) -> (Option<LogicalAst>, Vec<QueryParserError>) {
        match leaf {
            UserInputLeaf::Literal(literal) => {
                let term_phrases: Vec<(Field, &str, &str)> =
                    try_tuple!(self.compute_path_triplets_for_literal(&literal));
                let mut asts: Vec<LogicalAst> = Vec::new();
                let mut errors: Vec<QueryParserError> = Vec::new();
                for (field, json_path, phrase) in term_phrases {
                    let unboosted_asts = match self.compute_logical_ast_for_leaf(
                        field,
                        json_path,
                        phrase,
                        literal.slop,
                        literal.prefix,
                    ) {
                        Ok(asts) => asts,
                        Err(e) => {
                            errors.push(e);
                            continue;
                        }
                    };
                    for ast in unboosted_asts {
                        // Apply some field specific boost defined at the query parser level.
                        let boost = self.field_boost(field);
                        asts.push(LogicalAst::Leaf(Box::new(ast)).boost(boost));
                    }
                }
                let result_ast: LogicalAst = if asts.len() == 1 {
                    asts.into_iter().next().unwrap()
                } else {
                    LogicalAst::Clause(asts.into_iter().map(|ast| (Occur::Should, ast)).collect())
                };
                (Some(result_ast), errors)
            }
            UserInputLeaf::All => (
                Some(LogicalAst::Leaf(Box::new(LogicalLiteral::All))),
                Vec::new(),
            ),
            UserInputLeaf::Range {
                field: full_field_opt,
                lower,
                upper,
            } => {
                let Some(full_path) = full_field_opt else {
                    return (
                        None,
                        vec![QueryParserError::UnsupportedQuery(
                            "Range query need to target a specific field.".to_string(),
                        )],
                    );
                };
                let (field, json_path) = try_tuple!(self
                    .split_full_path(&full_path)
                    .ok_or_else(|| QueryParserError::FieldDoesNotExist(full_path.clone())));
                let field_entry = self.schema.get_field_entry(field);
                let value_type = field_entry.field_type().value_type();
                let mut errors = Vec::new();
                let lower = match self.resolve_bound(field, json_path, &lower) {
                    Ok(bound) => bound,
                    Err(error) => {
                        errors.push(error);
                        Bound::Unbounded
                    }
                };
                let upper = match self.resolve_bound(field, json_path, &upper) {
                    Ok(bound) => bound,
                    Err(error) => {
                        errors.push(error);
                        Bound::Unbounded
                    }
                };
                if lower == Bound::Unbounded && upper == Bound::Unbounded {
                    // this range is useless, either because a user requested [* TO *], or because
                    // we failed to parse something. Either way, there is no point emiting it
                    return (None, errors);
                }
                let logical_ast = LogicalAst::Leaf(Box::new(LogicalLiteral::Range {
                    field: self.schema.get_field_name(field).to_string(),
                    value_type,
                    lower,
                    upper,
                }));
                (Some(logical_ast), errors)
            }
            UserInputLeaf::Set {
                field: full_field_opt,
                elements,
            } => {
                let full_path = try_tuple!(full_field_opt.ok_or_else(|| {
                    QueryParserError::UnsupportedQuery(
                        "Range query need to target a specific field.".to_string(),
                    )
                }));
                let (field, json_path) = try_tuple!(self
                    .split_full_path(&full_path)
                    .ok_or_else(|| QueryParserError::FieldDoesNotExist(full_path.clone())));
                let (elements, errors) = elements
                    .into_iter()
                    .map(|element| self.compute_boundary_term(field, json_path, &element))
                    .partition_result();
                let logical_ast = LogicalAst::Leaf(Box::new(LogicalLiteral::Set { elements }));
                (Some(logical_ast), errors)
            }
            UserInputLeaf::Exists { .. } => (
                None,
                vec![QueryParserError::UnsupportedQuery(
                    "Range query need to target a specific field.".to_string(),
                )],
            ),
        }
    }
}

fn convert_literal_to_query(
    fuzzy: &FxHashMap<Field, Fuzzy>,
    logical_literal: LogicalLiteral,
) -> Box<dyn Query> {
    match logical_literal {
        LogicalLiteral::Term(term) => {
            if let Some(fuzzy) = fuzzy.get(&term.field()) {
                if fuzzy.prefix {
                    Box::new(FuzzyTermQuery::new_prefix(
                        term,
                        fuzzy.distance,
                        fuzzy.transpose_cost_one,
                    ))
                } else {
                    Box::new(FuzzyTermQuery::new(
                        term,
                        fuzzy.distance,
                        fuzzy.transpose_cost_one,
                    ))
                }
            } else {
                Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs))
            }
        }
        LogicalLiteral::Phrase {
            terms,
            slop,
            prefix,
        } => {
            if prefix {
                Box::new(PhrasePrefixQuery::new_with_offset(terms))
            } else {
                Box::new(PhraseQuery::new_with_offset_and_slop(terms, slop))
            }
        }
        LogicalLiteral::Range {
            field,
            value_type,
            lower,
            upper,
        } => Box::new(RangeQuery::new_term_bounds(
            field, value_type, &lower, &upper,
        )),
        LogicalLiteral::Set { elements, .. } => Box::new(TermSetQuery::new(elements)),
        LogicalLiteral::All => Box::new(AllQuery),
    }
}

fn generate_literals_for_str(
    field_name: &str,
    field: Field,
    phrase: &str,
    slop: u32,
    prefix: bool,
    indexing_options: &TextFieldIndexing,
    text_analyzer: &mut TextAnalyzer,
) -> Result<Option<LogicalLiteral>, QueryParserError> {
    let mut terms: Vec<(usize, Term)> = Vec::new();
    let mut token_stream = text_analyzer.token_stream(phrase);
    token_stream.process(&mut |token| {
        let term = Term::from_field_text(field, &token.text);
        terms.push((token.position, term));
    });
    if terms.len() <= 1 {
        if prefix {
            return Err(QueryParserError::PhrasePrefixRequiresAtLeastTwoTerms {
                phrase: phrase.to_owned(),
                tokenizer: indexing_options.tokenizer().to_owned(),
            });
        }
        let term_literal_opt = terms
            .into_iter()
            .next()
            .map(|(_, term)| LogicalLiteral::Term(term));
        return Ok(term_literal_opt);
    }
    if !indexing_options.index_option().has_positions() {
        return Err(QueryParserError::FieldDoesNotHavePositionsIndexed(
            field_name.to_string(),
        ));
    }
    Ok(Some(LogicalLiteral::Phrase {
        terms,
        slop,
        prefix,
    }))
}

fn generate_literals_for_json_object(
    field_name: &str,
    field: Field,
    json_path: &str,
    phrase: &str,
    tokenizer_manager: &TokenizerManager,
    json_options: &JsonObjectOptions,
) -> Result<Vec<LogicalLiteral>, QueryParserError> {
    let text_options = json_options.get_text_indexing_options().ok_or_else(|| {
        // This should have been seen earlier really.
        QueryParserError::FieldNotIndexed(field_name.to_string())
    })?;
    let mut text_analyzer = tokenizer_manager
        .get(text_options.tokenizer())
        .ok_or_else(|| QueryParserError::UnknownTokenizer {
            field: field_name.to_string(),
            tokenizer: text_options.tokenizer().to_string(),
        })?;
    let index_record_option = text_options.index_option();
    let mut logical_literals = Vec::new();

    let get_term_with_path =
        || Term::from_field_json_path(field, json_path, json_options.is_expand_dots_enabled());

    // Try to convert the phrase to a columnar value
    if let Some(term) =
        convert_to_columnar_value_and_append_to_json_term(get_term_with_path(), phrase)
    {
        logical_literals.push(LogicalLiteral::Term(term));
    }

    // Try to tokenize the phrase and create Terms.
    let mut positions_and_terms = Vec::<(usize, Term)>::new();
    let mut token_stream = text_analyzer.token_stream(phrase);
    token_stream.process(&mut |token| {
        let mut term = get_term_with_path();
        term.append_type_and_str(&token.text);
        positions_and_terms.push((token.position, term.clone()));
    });

    if positions_and_terms.len() <= 1 {
        for (_, term) in positions_and_terms {
            logical_literals.push(LogicalLiteral::Term(term));
        }
        return Ok(logical_literals);
    }
    if !index_record_option.has_positions() {
        return Err(QueryParserError::FieldDoesNotHavePositionsIndexed(
            field_name.to_string(),
        ));
    }
    logical_literals.push(LogicalLiteral::Phrase {
        terms: positions_and_terms,
        slop: 0,
        prefix: false,
    });
    Ok(logical_literals)
}

fn convert_to_query(fuzzy: &FxHashMap<Field, Fuzzy>, logical_ast: LogicalAst) -> Box<dyn Query> {
    match trim_ast(logical_ast) {
        Some(LogicalAst::Clause(trimmed_clause)) => {
            let occur_subqueries = trimmed_clause
                .into_iter()
                .map(|(occur, subquery)| (occur, convert_to_query(fuzzy, subquery)))
                .collect::<Vec<_>>();
            assert!(
                !occur_subqueries.is_empty(),
                "Should not be empty after trimming"
            );
            Box::new(BooleanQuery::new(occur_subqueries))
        }
        Some(LogicalAst::Leaf(trimmed_logical_literal)) => {
            convert_literal_to_query(fuzzy, *trimmed_logical_literal)
        }
        Some(LogicalAst::Boost(ast, boost)) => {
            let query = convert_to_query(fuzzy, *ast);
            let boosted_query = BoostQuery::new(query, boost);
            Box::new(boosted_query)
        }
        None => Box::new(EmptyQuery),
    }
}
