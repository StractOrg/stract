use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};

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
    fn default_options(&self) -> tantivy::schema::TextOptions {
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("en_stem")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
    }

    pub fn options(&self) -> tantivy::schema::TextOptions {
        match self {
            Field::Title => self.default_options().set_stored(),
            Field::Body => self.default_options().set_stored(),
            Field::Url => self.default_options().set_stored(),
            Field::BacklinkText => self.default_options(),
            Field::Centrality => self.default_options().set_fast(),
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
        builder.add_text_field(field.as_str(), field.options());
    }

    builder.build()
}
