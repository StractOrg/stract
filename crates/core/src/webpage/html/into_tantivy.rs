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
    schema::{
        text_field::{self, TextField},
        FastFieldEnum, TextFieldEnum,
    },
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
                .unwrap_or_else(|_| unreachable!("Unknown field: {}", field.name()));

            match field {
                Field::Text(f) => f.add_html_tantivy(self, &mut cache, &mut doc, schema)?,
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
                Field::Fast(FastFieldEnum::HostCentrality(_))
                | Field::Fast(FastFieldEnum::HostCentralityRank(_))
                | Field::Fast(FastFieldEnum::PageCentrality(_))
                | Field::Fast(FastFieldEnum::PageCentralityRank(_))
                | Field::Fast(FastFieldEnum::FetchTimeMs(_))
                | Field::Fast(FastFieldEnum::PreComputedScore(_))
                | Field::Fast(FastFieldEnum::Region(_))
                | Field::Fast(FastFieldEnum::HostNodeID(_))
                | Field::Fast(FastFieldEnum::TitleEmbeddings(_))
                | Field::Fast(FastFieldEnum::KeywordEmbeddings(_)) => {}
            }
        }

        Ok(doc)
    }
}
