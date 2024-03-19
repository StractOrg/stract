// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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
    rake::RakeModel,
    schema::{FastField, TextField},
    simhash, split_u128, tokenizer,
    webpage::url_ext::UrlExt,
    Error, Result,
};
use tantivy::{
    tokenizer::{PreTokenizedString, Tokenizer},
    TantivyDocument,
};
use whatlang::Lang;

use super::{fn_cache::FnCache, Html};

use crate::schema::{Field, FLOAT_SCALING};

impl Html {
    pub fn pretokenize_title(&self) -> Result<PreTokenizedString> {
        let title = self.title();

        if title.is_none() {
            return Err(Error::EmptyField("title").into());
        }
        let title = title.unwrap();

        Ok(self.pretokenize_string(title, TextField::Title))
    }

    pub fn pretokenize_all_text(&self) -> Result<PreTokenizedString> {
        let all_text = self.all_text();

        if all_text.is_none() {
            return Err(Error::EmptyField("all body").into());
        }
        let all_text = all_text.unwrap();

        Ok(self.pretokenize_string(all_text, TextField::AllBody))
    }

    pub fn pretokenize_clean_text(&self) -> PreTokenizedString {
        let clean_text = self.clean_text().cloned().unwrap_or_default();
        self.pretokenize_string(clean_text, TextField::CleanBody)
    }

    pub fn pretokenize_url(&self) -> PreTokenizedString {
        let url = self.url().to_string();
        self.pretokenize_string(url, TextField::Url)
    }

    pub fn pretokenize_url_for_site_operator(&self) -> PreTokenizedString {
        self.pretokenize_string_with(
            self.url().to_string(),
            tokenizer::Tokenizer::SiteOperator(tokenizer::SiteOperatorUrlTokenizer),
        )
    }

    pub fn pretokenize_domain(&self) -> PreTokenizedString {
        let domain = self.url().root_domain().unwrap_or_default().to_string();

        self.pretokenize_string(domain, TextField::Domain)
    }

    pub fn pretokenize_site(&self) -> PreTokenizedString {
        let site = self.url().normalized_host().unwrap_or_default().to_string();

        self.pretokenize_string(site, TextField::SiteWithout)
    }

    pub fn pretokenize_description(&self) -> PreTokenizedString {
        let text = self.description().unwrap_or_default();

        self.pretokenize_string(text, TextField::Description)
    }

    pub fn pretokenize_microformats(&self) -> PreTokenizedString {
        let mut text = String::new();

        for microformat in self.microformats().iter() {
            text.push_str(microformat.as_str());
            text.push(' ');
        }

        self.pretokenize_string(text, TextField::MicroformatTags)
    }

    fn pretokenize_string(&self, text: String, field: TextField) -> PreTokenizedString {
        self.pretokenize_string_with(text, field.indexing_tokenizer())
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

    pub fn domain_name(&self) -> String {
        let domain = self.url().domain().unwrap_or_default();
        self.url()
            .root_domain()
            .unwrap_or_default()
            .find('.')
            .map(|index| &domain[..ceil_char_boundary(&domain, index).min(domain.len())])
            .unwrap_or_default()
            .to_string()
    }

    pub fn keywords(&self, rake: &RakeModel) -> Vec<String> {
        self.clean_text()
            .map(|text| {
                rake.keywords(text, self.lang.unwrap_or(Lang::Eng))
                    .into_iter()
                    .map(|k| k.text)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn site_hash(&self) -> [u64; 2] {
        split_u128(hash(self.url().normalized_host().unwrap_or_default()).0)
    }

    pub fn url_without_query_hash(&self) -> [u64; 2] {
        let mut url_without_query = self.url().clone();
        url_without_query.set_query(None);

        split_u128(hash(url_without_query.as_str()).0)
    }

    pub fn url_without_tld_hash(&self) -> [u64; 2] {
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

        split_u128(hash(url_without_tld).0)
    }

    pub fn url_hash(&self) -> [u64; 2] {
        split_u128(hash(self.url().as_str()).0)
    }

    pub fn domain_hash(&self) -> [u64; 2] {
        split_u128(hash(self.url().root_domain().unwrap_or_default()).0)
    }

    pub fn title_hash(&self) -> [u64; 2] {
        split_u128(hash(self.title().unwrap_or_default()).0)
    }

    pub fn as_tantivy(&self, schema: &tantivy::schema::Schema) -> Result<TantivyDocument> {
        let mut doc = TantivyDocument::new();
        let mut cache = FnCache::new(self);

        for field in schema
            .fields()
            .filter_map(|(field, _)| Field::get(field.field_id() as usize))
        {
            let tantivy_field = schema
                .get_field(field.name())
                .unwrap_or_else(|_| panic!("Unknown field: {}", field.name()));

            match field {
                Field::Text(TextField::Title) => doc.add_pre_tokenized_text(
                    tantivy_field,
                    cache
                        .pretokenize_title()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?,
                ),
                Field::Text(TextField::StemmedTitle) => {
                    let title = cache
                        .pretokenize_title()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
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
                Field::Text(TextField::CleanBody) => doc
                    .add_pre_tokenized_text(tantivy_field, cache.pretokenize_clean_text().clone()),
                Field::Text(TextField::StemmedCleanBody) => {
                    let clean_text = cache.pretokenize_clean_text();
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
                    let title = cache
                        .pretokenize_title()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    doc.add_text(tantivy_field, title.text.clone());
                }
                Field::Text(TextField::TitleTrigrams) => {
                    let title = cache
                        .pretokenize_title()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    doc.add_text(tantivy_field, title.text.clone());
                }
                Field::Text(TextField::Description) => {
                    let description = cache.pretokenize_description();
                    doc.add_pre_tokenized_text(tantivy_field, description.clone());
                }
                Field::Text(TextField::Url) => {
                    let url = cache.pretokenize_url();
                    doc.add_pre_tokenized_text(tantivy_field, url.clone())
                }
                Field::Text(TextField::UrlForSiteOperator) => doc.add_pre_tokenized_text(
                    tantivy_field,
                    cache.pretokenize_url_for_site_operator().clone(),
                ),
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
                    doc.add_pre_tokenized_text(tantivy_field, cache.pretokenize_site().clone())
                }
                Field::Text(TextField::Domain) => {
                    doc.add_pre_tokenized_text(tantivy_field, cache.pretokenize_domain().clone())
                }
                Field::Text(TextField::SiteNoTokenizer) => {
                    let site = cache.pretokenize_site();

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
                }
                Field::Text(TextField::SiteIfHomepageNoTokenizer) => {
                    let site = cache.pretokenize_site();

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
                Field::Text(TextField::DomainNoTokenizer) => {
                    let domain = cache.pretokenize_domain();

                    doc.add_pre_tokenized_text(
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
                    )
                }
                Field::Text(TextField::TitleIfHomepage) => {
                    let title = cache
                        .pretokenize_title()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;

                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(tantivy_field, title);
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainIfHomepage) => {
                    let domain = cache.pretokenize_domain();
                    if self.is_homepage() {
                        doc.add_text(tantivy_field, domain.text.clone());
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainNameNoTokenizer) => {
                    let domain_name = cache.domain_name();

                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        PreTokenizedString {
                            text: domain_name.clone(),
                            tokens: vec![tantivy::tokenizer::Token {
                                offset_from: 0,
                                offset_to: domain_name.len(),
                                position: 0,
                                text: domain_name.clone(),
                                position_length: 1,
                            }],
                        },
                    );
                }
                Field::Text(TextField::DomainNameIfHomepageNoTokenizer) => {
                    let domain_name = cache.domain_name();

                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(
                            tantivy_field,
                            PreTokenizedString {
                                text: domain_name.clone(),
                                tokens: vec![tantivy::tokenizer::Token {
                                    offset_from: 0,
                                    offset_to: domain_name.len(),
                                    position: 0,
                                    text: domain_name.clone(),
                                    position_length: 1,
                                }],
                            },
                        );
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainIfHomepageNoTokenizer) => {
                    let domain = cache.pretokenize_domain();

                    if self.url().is_homepage() {
                        doc.add_pre_tokenized_text(
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
                        );
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::AllBody) => {
                    let all_text = cache
                        .pretokenize_all_text()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;

                    doc.add_pre_tokenized_text(tantivy_field, all_text.clone())
                }
                Field::Text(TextField::RecipeFirstIngredientTagId) => {
                    doc.add_text(
                        tantivy_field,
                        cache.first_ingredient_tag_id().cloned().unwrap_or_default(),
                    );
                }
                Field::Text(TextField::SchemaOrgJson) => {
                    doc.add_text(tantivy_field, cache.schema_json());
                }
                Field::Text(TextField::FlattenedSchemaOrgJson) => {
                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        cache.pretokenized_schema_json().clone(),
                    );
                }
                Field::Text(TextField::MicroformatTags) => {
                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        cache.pretokenize_microformats().clone(),
                    );
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
                    doc.add_u64(tantivy_field, cache.pretokenize_url().tokens.len() as u64)
                }
                Field::Fast(FastField::NumMicroformatTagsTokens) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_microformats().tokens.len() as u64,
                ),
                Field::Fast(FastField::NumTitleTokens) => doc.add_u64(
                    tantivy_field,
                    cache
                        .pretokenize_title()
                        .as_ref()
                        .map(|n| n.tokens.len() as u64)
                        .map_err(|e| anyhow::anyhow!("{}", e))?,
                ),
                Field::Fast(FastField::NumCleanBodyTokens) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_clean_text().tokens.len() as u64,
                ),
                Field::Fast(FastField::NumDescriptionTokens) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_description().tokens.len() as u64,
                ),
                Field::Fast(FastField::NumUrlForSiteOperatorTokens) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_url_for_site_operator().tokens.len() as u64,
                ),
                Field::Fast(FastField::NumDomainTokens) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_domain().tokens.len() as u64,
                ),
                Field::Fast(FastField::NumFlattenedSchemaTokens) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenized_schema_json().tokens.len() as u64,
                ),
                Field::Fast(FastField::SiteHash1) => {
                    doc.add_u64(tantivy_field, cache.site_hash()[0]);
                }
                Field::Fast(FastField::SiteHash2) => {
                    doc.add_u64(tantivy_field, cache.site_hash()[1]);
                }
                Field::Fast(FastField::UrlWithoutQueryHash1) => {
                    doc.add_u64(tantivy_field, cache.url_without_query_hash()[0]);
                }
                Field::Fast(FastField::UrlWithoutQueryHash2) => {
                    doc.add_u64(tantivy_field, cache.url_without_query_hash()[1]);
                }
                Field::Fast(FastField::UrlHash1) => {
                    doc.add_u64(tantivy_field, cache.url_hash()[0]);
                }
                Field::Fast(FastField::UrlHash2) => {
                    doc.add_u64(tantivy_field, cache.url_hash()[1]);
                }
                Field::Fast(FastField::UrlWithoutTldHash1) => {
                    doc.add_u64(tantivy_field, cache.url_without_tld_hash()[0]);
                }
                Field::Fast(FastField::UrlWithoutTldHash2) => {
                    doc.add_u64(tantivy_field, cache.url_without_tld_hash()[1]);
                }
                Field::Fast(FastField::DomainHash1) => {
                    doc.add_u64(tantivy_field, cache.domain_hash()[0]);
                }
                Field::Fast(FastField::DomainHash2) => {
                    doc.add_u64(tantivy_field, cache.domain_hash()[1]);
                }
                Field::Fast(FastField::TitleHash1) => {
                    doc.add_u64(tantivy_field, cache.title_hash()[0]);
                }
                Field::Fast(FastField::TitleHash2) => {
                    doc.add_u64(tantivy_field, cache.title_hash()[1]);
                }
                Field::Fast(FastField::SimHash) => {
                    let clean_text = cache.pretokenize_clean_text();

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
                | Field::Fast(FastField::HostCentralityRank)
                | Field::Fast(FastField::PageCentrality)
                | Field::Fast(FastField::PageCentralityRank)
                | Field::Fast(FastField::FetchTimeMs)
                | Field::Fast(FastField::PreComputedScore)
                | Field::Fast(FastField::Region)
                | Field::Fast(FastField::HostNodeID)
                | Field::Fast(FastField::TitleEmbeddings)
                | Field::Fast(FastField::KeywordEmbeddings)
                | Field::Text(TextField::Keywords)
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
