use tantivy::schema::{
    Cardinality, IndexRecordOption, NumericOptions, TextFieldIndexing, TextOptions,
};

#[derive(Clone)]
pub enum Field {
    Title,
    Body,
    Url,
    BacklinkText,
    Centrality,
}
pub static ALL_FIELDS: [Field; 5] = [
    Field::Title,
    Field::Body,
    Field::Url,
    Field::BacklinkText,
    Field::Centrality,
];

impl Field {
    fn default_text_options(&self) -> tantivy::schema::TextOptions {
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("en_stem")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
    }

    fn options(&self) -> IndexingOption {
        match self {
            Field::Title => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::Body => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::Url => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::BacklinkText => IndexingOption::Text(self.default_text_options()),
            Field::Centrality => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Field::Title => "title",
            Field::Body => "body",
            Field::Url => "url",
            Field::BacklinkText => "backlink_text",
            Field::Centrality => "centrality",
        }
    }
}

pub fn create_schema() -> tantivy::schema::Schema {
    let mut builder = tantivy::schema::Schema::builder();

    for field in &ALL_FIELDS {
        match field.options() {
            IndexingOption::Text(options) => builder.add_text_field(field.as_str(), options),
            IndexingOption::Numeric(options) => builder.add_f64_field(field.as_str(), options),
        };
    }

    builder.build()
}

enum IndexingOption {
    Text(tantivy::schema::TextOptions),
    Numeric(tantivy::schema::NumericOptions),
}
