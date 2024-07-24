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

use chrono::{DateTime, NaiveDateTime};
use tantivy::{schema::Value, TantivyDocument};

use crate::{
    schema::{
        text_field::{self, TextField},
        Field, NumericalFieldEnum, TextFieldEnum,
    },
    snippet::TextSnippet,
    webpage::{schema_org, Region},
};

#[derive(
    Default,
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
)]
pub struct RetrievedWebpage {
    pub title: String,
    pub url: String,
    pub body: String,
    pub snippet: TextSnippet,
    pub dirty_body: String,
    pub description: Option<String>,
    pub dmoz_description: Option<String>,
    #[bincode(with_serde)]
    pub updated_time: Option<NaiveDateTime>,
    pub schema_org: Vec<schema_org::Item>,
    pub region: Region,
    pub likely_has_ads: bool,
    pub likely_has_paywall: bool,
    pub recipe_first_ingredient_tag_id: Option<String>,
    pub keywords: Vec<String>,
}
impl RetrievedWebpage {
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref().or(self.dmoz_description.as_ref())
    }
}

fn str_value(name: &str, value: &tantivy::schema::document::CompactDocValue) -> String {
    value
        .as_str()
        .unwrap_or_else(|| panic!("{} field should be text", name))
        .to_string()
}

impl From<TantivyDocument> for RetrievedWebpage {
    fn from(doc: TantivyDocument) -> Self {
        let mut webpage = RetrievedWebpage::default();

        for (field, value) in doc.field_values() {
            match Field::get(field.field_id() as usize) {
                Some(Field::Text(TextFieldEnum::Title(_))) => {
                    webpage.title = str_value(text_field::Title.name(), &value);
                }
                Some(Field::Text(TextFieldEnum::StemmedCleanBody(_))) => {
                    webpage.body = str_value(text_field::StemmedCleanBody.name(), &value);
                }
                Some(Field::Text(TextFieldEnum::Description(_))) => {
                    let desc = str_value(text_field::Description.name(), &value);
                    webpage.description = if desc.is_empty() { None } else { Some(desc) }
                }
                Some(Field::Text(TextFieldEnum::Url(_))) => {
                    webpage.url = str_value(text_field::Url.name(), &value);
                }
                Some(Field::Numerical(NumericalFieldEnum::LastUpdated(_))) => {
                    webpage.updated_time = {
                        let timestamp = value.as_u64().unwrap() as i64;
                        if timestamp == 0 {
                            None
                        } else {
                            DateTime::from_timestamp(timestamp, 0).map(|dt| dt.naive_utc())
                        }
                    }
                }
                Some(Field::Text(TextFieldEnum::AllBody(_))) => {
                    webpage.dirty_body = str_value(text_field::AllBody.name(), &value);
                }
                Some(Field::Numerical(NumericalFieldEnum::Region(_))) => {
                    webpage.region = {
                        let id = value.as_u64().unwrap();
                        Region::from_id(id)
                    }
                }
                Some(Field::Text(TextFieldEnum::DmozDescription(_))) => {
                    let desc = str_value(text_field::DmozDescription.name(), &value);
                    webpage.dmoz_description = if desc.is_empty() { None } else { Some(desc) }
                }
                Some(Field::Text(TextFieldEnum::SchemaOrgJson(_))) => {
                    let json = str_value(text_field::SchemaOrgJson.name(), &value);
                    webpage.schema_org = serde_json::from_str(&json).unwrap_or_default();
                }
                Some(Field::Numerical(NumericalFieldEnum::LikelyHasAds(_))) => {
                    webpage.likely_has_ads = value.as_bool().unwrap_or_default();
                }
                Some(Field::Numerical(NumericalFieldEnum::LikelyHasPaywall(_))) => {
                    webpage.likely_has_paywall = value.as_bool().unwrap_or_default();
                }
                Some(Field::Text(TextFieldEnum::RecipeFirstIngredientTagId(_))) => {
                    let tag_id = str_value(text_field::RecipeFirstIngredientTagId.name(), &value);
                    if !tag_id.is_empty() {
                        webpage.recipe_first_ingredient_tag_id = Some(tag_id);
                    }
                }
                Some(Field::Text(TextFieldEnum::Keywords(_))) => {
                    let keywords = str_value(text_field::Keywords.name(), &value);
                    webpage.keywords = keywords.split('\n').map(|s| s.to_string()).collect();
                }
                _ => {}
            }
        }

        webpage
    }
}
