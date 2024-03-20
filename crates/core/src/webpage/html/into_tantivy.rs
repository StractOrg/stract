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
    schema::{text_field, FastFieldEnum, TextFieldEnum},
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

        Ok(self.pretokenize_string(title, text_field::Title.into()))
    }

    pub fn pretokenize_all_text(&self) -> Result<PreTokenizedString> {
        let all_text = self.all_text();

        if all_text.is_none() {
            return Err(Error::EmptyField("all body").into());
        }
        let all_text = all_text.unwrap();

        Ok(self.pretokenize_string(all_text, text_field::AllBody.into()))
    }

    pub fn pretokenize_clean_text(&self) -> PreTokenizedString {
        let clean_text = self.clean_text().cloned().unwrap_or_default();
        self.pretokenize_string(clean_text, text_field::CleanBody.into())
    }

    pub fn pretokenize_url(&self) -> PreTokenizedString {
        let url = self.url().to_string();
        self.pretokenize_string(url, text_field::Url.into())
    }

    pub fn pretokenize_url_for_site_operator(&self) -> PreTokenizedString {
        self.pretokenize_string_with(
            self.url().to_string(),
            tokenizer::Tokenizer::SiteOperator(tokenizer::SiteOperatorUrlTokenizer),
        )
    }

    pub fn pretokenize_domain(&self) -> PreTokenizedString {
        let domain = self.url().root_domain().unwrap_or_default().to_string();

        self.pretokenize_string(domain, text_field::Domain.into())
    }

    pub fn pretokenize_site(&self) -> PreTokenizedString {
        let site = self.url().normalized_host().unwrap_or_default().to_string();

        self.pretokenize_string(site, text_field::SiteWithout.into())
    }

    pub fn pretokenize_description(&self) -> PreTokenizedString {
        let text = self.description().unwrap_or_default();

        self.pretokenize_string(text, text_field::Description.into())
    }

    pub fn pretokenize_microformats(&self) -> PreTokenizedString {
        let mut text = String::new();

        for microformat in self.microformats().iter() {
            text.push_str(microformat.as_str());
            text.push(' ');
        }

        self.pretokenize_string(text, text_field::MicroformatTags.into())
    }

    fn pretokenize_string(&self, text: String, field: TextFieldEnum) -> PreTokenizedString {
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
                Field::Text(TextFieldEnum::Title(_)) => doc.add_pre_tokenized_text(
                    tantivy_field,
                    cache
                        .pretokenize_title()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?,
                ),
                Field::Text(TextFieldEnum::StemmedTitle(_)) => {
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
                Field::Text(TextFieldEnum::CleanBody(_)) => doc
                    .add_pre_tokenized_text(tantivy_field, cache.pretokenize_clean_text().clone()),
                Field::Text(TextFieldEnum::StemmedCleanBody(_)) => {
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
                Field::Text(TextFieldEnum::CleanBodyBigrams(_)) => {
                    doc.add_text(
                        tantivy_field,
                        self.clean_text().cloned().unwrap_or_default(),
                    );
                }
                Field::Text(TextFieldEnum::CleanBodyTrigrams(_)) => {
                    doc.add_text(
                        tantivy_field,
                        self.clean_text().cloned().unwrap_or_default(),
                    );
                }
                Field::Text(TextFieldEnum::TitleBigrams(_)) => {
                    let title = cache
                        .pretokenize_title()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    doc.add_text(tantivy_field, title.text.clone());
                }
                Field::Text(TextFieldEnum::TitleTrigrams(_)) => {
                    let title = cache
                        .pretokenize_title()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    doc.add_text(tantivy_field, title.text.clone());
                }
                Field::Text(TextFieldEnum::Description(_)) => {
                    let description = cache.pretokenize_description();
                    doc.add_pre_tokenized_text(tantivy_field, description.clone());
                }
                Field::Text(TextFieldEnum::Url(_)) => {
                    let url = cache.pretokenize_url();
                    doc.add_pre_tokenized_text(tantivy_field, url.clone())
                }
                Field::Text(TextFieldEnum::UrlForSiteOperator(_)) => doc.add_pre_tokenized_text(
                    tantivy_field,
                    cache.pretokenize_url_for_site_operator().clone(),
                ),
                Field::Text(TextFieldEnum::UrlNoTokenizer(_)) => {
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
                Field::Text(TextFieldEnum::SiteWithout(_)) => {
                    doc.add_pre_tokenized_text(tantivy_field, cache.pretokenize_site().clone())
                }
                Field::Text(TextFieldEnum::Domain(_)) => {
                    doc.add_pre_tokenized_text(tantivy_field, cache.pretokenize_domain().clone())
                }
                Field::Text(TextFieldEnum::SiteNoTokenizer(_)) => {
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
                Field::Text(TextFieldEnum::SiteIfHomepageNoTokenizer(_)) => {
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
                Field::Text(TextFieldEnum::DomainNoTokenizer(_)) => {
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
                Field::Text(TextFieldEnum::TitleIfHomepage(_)) => {
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
                Field::Text(TextFieldEnum::DomainIfHomepage(_)) => {
                    let domain = cache.pretokenize_domain();
                    if self.is_homepage() {
                        doc.add_text(tantivy_field, domain.text.clone());
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextFieldEnum::DomainNameNoTokenizer(_)) => {
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
                Field::Text(TextFieldEnum::DomainNameIfHomepageNoTokenizer(_)) => {
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
                Field::Text(TextFieldEnum::DomainIfHomepageNoTokenizer(_)) => {
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
                Field::Text(TextFieldEnum::AllBody(_)) => {
                    let all_text = cache
                        .pretokenize_all_text()
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;

                    doc.add_pre_tokenized_text(tantivy_field, all_text.clone())
                }
                Field::Text(TextFieldEnum::RecipeFirstIngredientTagId(_)) => {
                    doc.add_text(
                        tantivy_field,
                        cache.first_ingredient_tag_id().cloned().unwrap_or_default(),
                    );
                }
                Field::Text(TextFieldEnum::SchemaOrgJson(_)) => {
                    doc.add_text(tantivy_field, cache.schema_json());
                }
                Field::Text(TextFieldEnum::FlattenedSchemaOrgJson(_)) => {
                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        cache.pretokenized_schema_json().clone(),
                    );
                }
                Field::Text(TextFieldEnum::MicroformatTags(_)) => {
                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        cache.pretokenize_microformats().clone(),
                    );
                }
                Field::Fast(FastFieldEnum::IsHomepage(_)) => {
                    doc.add_u64(tantivy_field, (self.is_homepage()).into());
                }
                Field::Fast(FastFieldEnum::LastUpdated(_)) => doc.add_u64(
                    tantivy_field,
                    self.updated_time()
                        .map_or(0, |time| time.timestamp().max(0) as u64),
                ),
                Field::Fast(FastFieldEnum::TrackerScore(_)) => {
                    doc.add_u64(tantivy_field, self.trackers().len() as u64)
                }
                Field::Fast(FastFieldEnum::NumUrlTokens(_)) => {
                    doc.add_u64(tantivy_field, cache.pretokenize_url().tokens.len() as u64)
                }
                Field::Fast(FastFieldEnum::NumMicroformatTagsTokens(_)) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_microformats().tokens.len() as u64,
                ),
                Field::Fast(FastFieldEnum::NumTitleTokens(_)) => doc.add_u64(
                    tantivy_field,
                    cache
                        .pretokenize_title()
                        .as_ref()
                        .map(|n| n.tokens.len() as u64)
                        .map_err(|e| anyhow::anyhow!("{}", e))?,
                ),
                Field::Fast(FastFieldEnum::NumCleanBodyTokens(_)) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_clean_text().tokens.len() as u64,
                ),
                Field::Fast(FastFieldEnum::NumDescriptionTokens(_)) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_description().tokens.len() as u64,
                ),
                Field::Fast(FastFieldEnum::NumUrlForSiteOperatorTokens(_)) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_url_for_site_operator().tokens.len() as u64,
                ),
                Field::Fast(FastFieldEnum::NumDomainTokens(_)) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenize_domain().tokens.len() as u64,
                ),
                Field::Fast(FastFieldEnum::NumFlattenedSchemaTokens(_)) => doc.add_u64(
                    tantivy_field,
                    cache.pretokenized_schema_json().tokens.len() as u64,
                ),
                Field::Fast(FastFieldEnum::SiteHash1(_)) => {
                    doc.add_u64(tantivy_field, cache.site_hash()[0]);
                }
                Field::Fast(FastFieldEnum::SiteHash2(_)) => {
                    doc.add_u64(tantivy_field, cache.site_hash()[1]);
                }
                Field::Fast(FastFieldEnum::UrlWithoutQueryHash1(_)) => {
                    doc.add_u64(tantivy_field, cache.url_without_query_hash()[0]);
                }
                Field::Fast(FastFieldEnum::UrlWithoutQueryHash2(_)) => {
                    doc.add_u64(tantivy_field, cache.url_without_query_hash()[1]);
                }
                Field::Fast(FastFieldEnum::UrlHash1(_)) => {
                    doc.add_u64(tantivy_field, cache.url_hash()[0]);
                }
                Field::Fast(FastFieldEnum::UrlHash2(_)) => {
                    doc.add_u64(tantivy_field, cache.url_hash()[1]);
                }
                Field::Fast(FastFieldEnum::UrlWithoutTldHash1(_)) => {
                    doc.add_u64(tantivy_field, cache.url_without_tld_hash()[0]);
                }
                Field::Fast(FastFieldEnum::UrlWithoutTldHash2(_)) => {
                    doc.add_u64(tantivy_field, cache.url_without_tld_hash()[1]);
                }
                Field::Fast(FastFieldEnum::DomainHash1(_)) => {
                    doc.add_u64(tantivy_field, cache.domain_hash()[0]);
                }
                Field::Fast(FastFieldEnum::DomainHash2(_)) => {
                    doc.add_u64(tantivy_field, cache.domain_hash()[1]);
                }
                Field::Fast(FastFieldEnum::TitleHash1(_)) => {
                    doc.add_u64(tantivy_field, cache.title_hash()[0]);
                }
                Field::Fast(FastFieldEnum::TitleHash2(_)) => {
                    doc.add_u64(tantivy_field, cache.title_hash()[1]);
                }
                Field::Fast(FastFieldEnum::SimHash(_)) => {
                    let clean_text = cache.pretokenize_clean_text();

                    let hash = if !clean_text.text.is_empty() {
                        simhash::hash(&clean_text.text)
                    } else {
                        0
                    };
                    doc.add_u64(tantivy_field, hash);
                }
                Field::Fast(FastFieldEnum::NumPathAndQuerySlashes(_)) => {
                    let num_slashes = self
                        .url()
                        .path_segments()
                        .map(|segments| segments.count())
                        .unwrap_or(0);

                    doc.add_u64(tantivy_field, num_slashes as u64);
                }
                Field::Fast(FastFieldEnum::NumPathAndQueryDigits(_)) => {
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
                Field::Fast(FastFieldEnum::LikelyHasAds(_)) => {
                    doc.add_u64(tantivy_field, self.likely_has_ads() as u64);
                }
                Field::Fast(FastFieldEnum::LikelyHasPaywall(_)) => {
                    doc.add_u64(tantivy_field, self.likely_has_paywall() as u64);
                }
                Field::Fast(FastFieldEnum::LinkDensity(_)) => {
                    doc.add_u64(
                        tantivy_field,
                        (self.link_density() * FLOAT_SCALING as f64) as u64,
                    );
                }
                Field::Text(TextFieldEnum::BacklinkText(_))
                | Field::Text(TextFieldEnum::SafetyClassification(_))
                | Field::Text(TextFieldEnum::InsertionTimestamp(_))
                | Field::Fast(FastFieldEnum::HostCentrality(_))
                | Field::Fast(FastFieldEnum::HostCentralityRank(_))
                | Field::Fast(FastFieldEnum::PageCentrality(_))
                | Field::Fast(FastFieldEnum::PageCentralityRank(_))
                | Field::Fast(FastFieldEnum::FetchTimeMs(_))
                | Field::Fast(FastFieldEnum::PreComputedScore(_))
                | Field::Fast(FastFieldEnum::Region(_))
                | Field::Fast(FastFieldEnum::HostNodeID(_))
                | Field::Fast(FastFieldEnum::TitleEmbeddings(_))
                | Field::Fast(FastFieldEnum::KeywordEmbeddings(_))
                | Field::Text(TextFieldEnum::Keywords(_))
                | Field::Text(TextFieldEnum::DmozDescription(_)) => {}
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
