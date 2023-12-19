// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use crate::{
    ceil_char_boundary,
    prehashed::hash,
    schema::{FastField, FieldMapping, TextField},
    simhash, split_u128, tokenizer,
    webpage::url_ext::UrlExt,
    Error, Result,
};
use tantivy::{
    tokenizer::{PreTokenizedString, Tokenizer},
    TantivyDocument,
};
use whatlang::Lang;

use super::{find_recipe_first_ingredient_tag_id, schema_org, Html};

use crate::schema::{Field, FLOAT_SCALING};

impl Html {
    fn pretokenize_title(&self) -> Result<PreTokenizedString> {
        let title = self.title();

        if title.is_none() {
            return Err(Error::EmptyField("title").into());
        }
        let title = title.unwrap();

        Ok(self.pretokenize_string(title))
    }

    fn pretokenize_all_text(&self) -> Result<PreTokenizedString> {
        let all_text = self.all_text();

        if all_text.is_none() {
            return Err(Error::EmptyField("all body").into());
        }
        let all_text = all_text.unwrap();

        Ok(self.pretokenize_string(all_text))
    }

    fn pretokenize_clean_text(&self) -> PreTokenizedString {
        let clean_text = self.clean_text().cloned().unwrap_or_default();
        self.pretokenize_string(clean_text)
    }

    fn pretokenize_url(&self) -> PreTokenizedString {
        let url = self.url().to_string();
        self.pretokenize_string(url)
    }

    fn pretokenize_domain(&self) -> PreTokenizedString {
        let domain = self.url().root_domain().unwrap_or_default().to_string();

        self.pretokenize_string(domain)
    }

    fn pretokenize_site(&self) -> PreTokenizedString {
        let site = self.url().normalized_host().unwrap_or_default().to_string();

        self.pretokenize_string(site)
    }

    fn pretokenize_description(&self) -> PreTokenizedString {
        let text = self.description().unwrap_or_default();

        self.pretokenize_string(text)
    }

    fn pretokenize_microformats(&self) -> PreTokenizedString {
        let mut text = String::new();

        for microformat in self.microformats().iter() {
            text.push_str(microformat.as_str());
            text.push(' ');
        }

        self.pretokenize_string(text)
    }

    fn pretokenize_string(&self, text: String) -> PreTokenizedString {
        self.pretokenize_string_with(text, tokenizer::Tokenizer::default())
    }

    fn pretokenize_string_with(
        &self,
        text: String,
        tokenizer: tokenizer::Tokenizer,
    ) -> PreTokenizedString {
        let mut tokenizer = tokenizer;

        let mut tokens = Vec::new();

        {
            let mut stream = tokenizer.token_stream(&text);
            while let Some(token) = stream.next() {
                tokens.push(token.clone());
            }
        }

        PreTokenizedString { text, tokens }
    }
    pub fn into_tantivy(self, schema: &tantivy::schema::Schema) -> Result<TantivyDocument> {
        let mut doc = TantivyDocument::new();

        let title = self.pretokenize_title()?;
        let all_text = self.pretokenize_all_text()?;
        let clean_text = self.pretokenize_clean_text();
        let url = self.pretokenize_url();
        let domain = self.pretokenize_domain();
        let site = self.pretokenize_site();
        let description = self.pretokenize_description();
        let microformats = self.pretokenize_microformats();
        let url_for_site_operator = self.pretokenize_string_with(
            self.url().to_string(),
            tokenizer::Tokenizer::SiteOperator(tokenizer::SiteOperatorUrlTokenizer),
        );

        let domain_name = self
            .url()
            .root_domain()
            .unwrap_or_default()
            .find('.')
            .map(|index| {
                &domain.text[..ceil_char_boundary(&domain.text, index).min(domain.text.len())]
            })
            .unwrap_or_default()
            .to_string();

        let schemas: Vec<_> = self.schema_org();
        let first_ingredient_tag_id =
            find_recipe_first_ingredient_tag_id(&schemas, &self.root).unwrap_or_default();

        let schema_json = serde_json::to_string(&schemas).ok().unwrap_or_default();

        let pretokenized_schema_json = match schema_org::flattened_json(schemas) {
            Ok(mut f) => {
                let mut tokens = Vec::new();

                {
                    let mut stream = f.token_stream();

                    while let Some(token) = stream.next() {
                        tokens.push(token.clone());
                    }
                }

                PreTokenizedString {
                    text: f.text().to_string(),
                    tokens,
                }
            }
            Err(_) => PreTokenizedString {
                text: String::new(),
                tokens: Vec::new(),
            },
        };

        let site_hash = split_u128(hash(self.url().normalized_host().unwrap_or_default()).0);

        let mut url_without_query = self.url().clone();
        url_without_query.set_query(None);

        let url_without_query_hash = split_u128(hash(url_without_query.as_str()).0);
        let url_hash = split_u128(hash(self.url().as_str()).0);

        let tld = self.url().tld().unwrap_or_default();
        let url_without_tld = self
            .url()
            .host_str()
            .unwrap_or_default()
            .trim_end_matches(&tld)
            .to_string()
            + "/"
            + self.url().path()
            + "?"
            + self.url().query().unwrap_or_default();

        let url_without_tld_hash = split_u128(hash(url_without_tld).0);

        let domain_hash = split_u128(hash(self.url().root_domain().unwrap_or_default()).0);
        let title_hash = split_u128(hash(self.title().unwrap_or_default()).0);

        for field in schema
            .fields()
            .filter_map(|(field, _)| FieldMapping::get(field.field_id() as usize))
        {
            let tantivy_field = schema
                .get_field(field.name())
                .unwrap_or_else(|_| panic!("Unknown field: {}", field.name()));

            match field {
                Field::Text(TextField::Title) => {
                    doc.add_pre_tokenized_text(tantivy_field, title.clone())
                }
                Field::Text(TextField::StemmedTitle) => {
                    let mut tokens = title.tokens.clone();
                    stem_tokens(&mut tokens, self.lang.unwrap_or(Lang::Eng));

                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        PreTokenizedString {
                            text: title.text.clone(),
                            tokens,
                        },
                    );
                }
                Field::Text(TextField::CleanBody) => {
                    doc.add_pre_tokenized_text(tantivy_field, clean_text.clone())
                }
                Field::Text(TextField::StemmedCleanBody) => {
                    let mut tokens = clean_text.tokens.clone();
                    stem_tokens(&mut tokens, self.lang.unwrap_or(Lang::Eng));

                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        PreTokenizedString {
                            text: clean_text.text.clone(),
                            tokens,
                        },
                    );
                }
                Field::Text(TextField::CleanBodyBigrams) => {
                    doc.add_text(
                        tantivy_field,
                        self.clean_text().cloned().unwrap_or_default(),
                    );
                }
                Field::Text(TextField::CleanBodyTrigrams) => {
                    doc.add_text(
                        tantivy_field,
                        self.clean_text().cloned().unwrap_or_default(),
                    );
                }
                Field::Text(TextField::TitleBigrams) => {
                    doc.add_text(tantivy_field, title.text.clone());
                }
                Field::Text(TextField::TitleTrigrams) => {
                    doc.add_text(tantivy_field, title.text.clone());
                }
                Field::Text(TextField::Description) => {
                    doc.add_pre_tokenized_text(tantivy_field, description.clone());
                }
                Field::Text(TextField::Url) => {
                    doc.add_pre_tokenized_text(tantivy_field, url.clone())
                }
                Field::Text(TextField::UrlForSiteOperator) => {
                    doc.add_pre_tokenized_text(tantivy_field, url_for_site_operator.clone())
                }
                Field::Text(TextField::UrlNoTokenizer) => {
                    let url = self.url().to_string();

                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        PreTokenizedString {
                            text: url.clone(),
                            tokens: vec![tantivy::tokenizer::Token {
                                offset_from: 0,
                                offset_to: url.len(),
                                position: 0,
                                text: url,
                                position_length: 1,
                            }],
                        },
                    );
                }
                Field::Text(TextField::SiteWithout) => {
                    doc.add_pre_tokenized_text(tantivy_field, site.clone())
                }
                Field::Text(TextField::Domain) => {
                    doc.add_pre_tokenized_text(tantivy_field, domain.clone())
                }
                Field::Text(TextField::SiteNoTokenizer) => doc.add_pre_tokenized_text(
                    tantivy_field,
                    PreTokenizedString {
                        text: site.text.clone(),
                        tokens: vec![tantivy::tokenizer::Token {
                            offset_from: 0,
                            offset_to: site.text.len(),
                            position: 0,
                            text: site.text.clone(),
                            position_length: 1,
                        }],
                    },
                ),
                Field::Text(TextField::SiteIfHomepageNoTokenizer) => {
                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(
                            tantivy_field,
                            PreTokenizedString {
                                text: site.text.clone(),
                                tokens: vec![tantivy::tokenizer::Token {
                                    offset_from: 0,
                                    offset_to: site.text.len(),
                                    position: 0,
                                    text: site.text.clone(),
                                    position_length: 1,
                                }],
                            },
                        )
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainNoTokenizer) => doc.add_pre_tokenized_text(
                    tantivy_field,
                    PreTokenizedString {
                        text: domain.text.clone(),
                        tokens: vec![tantivy::tokenizer::Token {
                            offset_from: 0,
                            offset_to: domain.text.len(),
                            position: 0,
                            text: domain.text.clone(),
                            position_length: 1,
                        }],
                    },
                ),
                Field::Text(TextField::TitleIfHomepage) => {
                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(tantivy_field, title.clone());
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainIfHomepage) => {
                    if self.is_homepage() {
                        doc.add_text(tantivy_field, domain.text.clone());
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainNameNoTokenizer) => {
                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        PreTokenizedString {
                            text: domain_name.to_string(),
                            tokens: vec![tantivy::tokenizer::Token {
                                offset_from: 0,
                                offset_to: domain_name.len(),
                                position: 0,
                                text: domain_name.to_string(),
                                position_length: 1,
                            }],
                        },
                    );
                }
                Field::Text(TextField::DomainNameIfHomepageNoTokenizer) => {
                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(
                            tantivy_field,
                            PreTokenizedString {
                                text: domain_name.to_string(),
                                tokens: vec![tantivy::tokenizer::Token {
                                    offset_from: 0,
                                    offset_to: domain_name.len(),
                                    position: 0,
                                    text: domain_name.to_string(),
                                    position_length: 1,
                                }],
                            },
                        );
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainIfHomepageNoTokenizer) => {
                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(tantivy_field, domain.clone());
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::AllBody) => {
                    doc.add_pre_tokenized_text(tantivy_field, all_text.clone())
                }
                Field::Text(TextField::RecipeFirstIngredientTagId) => {
                    doc.add_text(tantivy_field, first_ingredient_tag_id.clone());
                }
                Field::Text(TextField::SchemaOrgJson) => {
                    doc.add_text(tantivy_field, schema_json.clone());
                }
                Field::Text(TextField::FlattenedSchemaOrgJson) => {
                    doc.add_pre_tokenized_text(tantivy_field, pretokenized_schema_json.clone());
                }
                Field::Text(TextField::MicroformatTags) => {
                    doc.add_pre_tokenized_text(tantivy_field, microformats.clone());
                }
                Field::Fast(FastField::IsHomepage) => {
                    doc.add_u64(tantivy_field, (self.is_homepage()).into());
                }
                Field::Fast(FastField::LastUpdated) => doc.add_u64(
                    tantivy_field,
                    self.updated_time()
                        .map_or(0, |time| time.timestamp().max(0) as u64),
                ),
                Field::Fast(FastField::TrackerScore) => {
                    doc.add_u64(tantivy_field, self.trackers().len() as u64)
                }
                Field::Fast(FastField::NumUrlTokens) => {
                    doc.add_u64(tantivy_field, url.tokens.len() as u64)
                }
                Field::Fast(FastField::NumMicroformatTagsTokens) => {
                    doc.add_u64(tantivy_field, microformats.tokens.len() as u64)
                }
                Field::Fast(FastField::NumTitleTokens) => {
                    doc.add_u64(tantivy_field, title.tokens.len() as u64)
                }
                Field::Fast(FastField::NumCleanBodyTokens) => {
                    doc.add_u64(tantivy_field, clean_text.tokens.len() as u64)
                }
                Field::Fast(FastField::NumDescriptionTokens) => {
                    doc.add_u64(tantivy_field, description.tokens.len() as u64)
                }
                Field::Fast(FastField::NumUrlForSiteOperatorTokens) => {
                    doc.add_u64(tantivy_field, url_for_site_operator.tokens.len() as u64)
                }
                Field::Fast(FastField::NumDomainTokens) => {
                    doc.add_u64(tantivy_field, domain.tokens.len() as u64)
                }
                Field::Fast(FastField::NumFlattenedSchemaTokens) => {
                    doc.add_u64(tantivy_field, pretokenized_schema_json.tokens.len() as u64)
                }
                Field::Fast(FastField::SiteHash1) => {
                    doc.add_u64(tantivy_field, site_hash[0]);
                }
                Field::Fast(FastField::SiteHash2) => {
                    doc.add_u64(tantivy_field, site_hash[1]);
                }
                Field::Fast(FastField::UrlWithoutQueryHash1) => {
                    doc.add_u64(tantivy_field, url_without_query_hash[0]);
                }
                Field::Fast(FastField::UrlWithoutQueryHash2) => {
                    doc.add_u64(tantivy_field, url_without_query_hash[1]);
                }
                Field::Fast(FastField::UrlHash1) => {
                    doc.add_u64(tantivy_field, url_hash[0]);
                }
                Field::Fast(FastField::UrlHash2) => {
                    doc.add_u64(tantivy_field, url_hash[1]);
                }
                Field::Fast(FastField::UrlWithoutTldHash1) => {
                    doc.add_u64(tantivy_field, url_without_tld_hash[0]);
                }
                Field::Fast(FastField::UrlWithoutTldHash2) => {
                    doc.add_u64(tantivy_field, url_without_tld_hash[1]);
                }
                Field::Fast(FastField::DomainHash1) => {
                    doc.add_u64(tantivy_field, domain_hash[0]);
                }
                Field::Fast(FastField::DomainHash2) => {
                    doc.add_u64(tantivy_field, domain_hash[1]);
                }
                Field::Fast(FastField::TitleHash1) => {
                    doc.add_u64(tantivy_field, title_hash[0]);
                }
                Field::Fast(FastField::TitleHash2) => {
                    doc.add_u64(tantivy_field, title_hash[1]);
                }
                Field::Fast(FastField::SimHash) => {
                    let hash = if !clean_text.text.is_empty() {
                        simhash::hash(&clean_text.text)
                    } else {
                        0
                    };
                    doc.add_u64(tantivy_field, hash);
                }
                Field::Fast(FastField::NumPathAndQuerySlashes) => {
                    let num_slashes = self
                        .url()
                        .path_segments()
                        .map(|segments| segments.count())
                        .unwrap_or(0);

                    doc.add_u64(tantivy_field, num_slashes as u64);
                }
                Field::Fast(FastField::NumPathAndQueryDigits) => {
                    let num_digits = self
                        .url()
                        .path()
                        .chars()
                        .filter(|c| c.is_ascii_digit())
                        .count()
                        + self
                            .url()
                            .query()
                            .unwrap_or_default()
                            .chars()
                            .filter(|c| c.is_ascii_digit())
                            .count();

                    doc.add_u64(tantivy_field, num_digits as u64);
                }
                Field::Fast(FastField::LikelyHasAds) => {
                    doc.add_u64(tantivy_field, self.likely_has_ads() as u64);
                }
                Field::Fast(FastField::LikelyHasPaywall) => {
                    doc.add_u64(tantivy_field, self.likely_has_paywall() as u64);
                }
                Field::Fast(FastField::LinkDensity) => {
                    doc.add_u64(
                        tantivy_field,
                        (self.link_density() * FLOAT_SCALING as f64) as u64,
                    );
                }
                Field::Text(TextField::BacklinkText)
                | Field::Text(TextField::SafetyClassification)
                | Field::Text(TextField::InsertionTimestamp)
                | Field::Fast(FastField::HostCentrality)
                | Field::Fast(FastField::PageCentrality)
                | Field::Fast(FastField::FetchTimeMs)
                | Field::Fast(FastField::PreComputedScore)
                | Field::Fast(FastField::Region)
                | Field::Fast(FastField::HostNodeID1)
                | Field::Fast(FastField::HostNodeID2)
                | Field::Text(TextField::DmozDescription) => {}
            }
        }

        Ok(doc)
    }
}

fn stemmer_from_lang(lang: &Lang) -> rust_stemmers::Stemmer {
    match lang {
        Lang::Ara => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Arabic),
        Lang::Dan => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Danish),
        Lang::Nld => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Dutch),
        Lang::Fin => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Finnish),
        Lang::Fra => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::French),
        Lang::Deu => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::German),
        Lang::Ell => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Greek),
        Lang::Hun => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Hungarian),
        Lang::Ita => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Italian),
        Lang::Por => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Portuguese),
        Lang::Ron => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Romanian),
        Lang::Rus => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Russian),
        Lang::Spa => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Spanish),
        Lang::Swe => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Swedish),
        Lang::Tam => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Tamil),
        Lang::Tur => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Turkish),
        _ => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English),
    }
}

fn stem_tokens(tokens: &mut [tantivy::tokenizer::Token], lang: Lang) {
    let stemmer = stemmer_from_lang(&lang);
    for token in tokens {
        // TODO remove allocation
        if let Ok(stemmed_str) = std::panic::catch_unwind(|| stemmer.stem(&token.text).into_owned())
        {
            token.text.clear();
            token.text.push_str(&stemmed_str);
        }
    }
}
