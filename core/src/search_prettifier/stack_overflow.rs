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

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    inverted_index::RetrievedWebpage,
    webpage::schema_org::{self, Item, OneOrMany, Property},
    Error,
};

use super::{Sidebar, Snippet};
use crate::Result;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StackOverflowAnswer {
    pub body: Vec<CodeOrText>,
    pub date: String,
    pub url: String,
    pub upvotes: u32,
    pub accepted: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StackOverflowQuestion {
    pub body: Vec<CodeOrText>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CodeOrText {
    Code(String),
    Text(String),
}

fn parse_so_answer(
    text: OneOrMany<Property>,
    date: OneOrMany<Property>,
    upvotes: OneOrMany<Property>,
    url: OneOrMany<Property>,
    webpage_url: Url,
    accepted: bool,
) -> Option<StackOverflowAnswer> {
    let text: Vec<_> = text
        .many()
        .into_iter()
        .map(|prop| match prop {
            Property::String(s) => CodeOrText::Text(s),
            Property::Item(item) => CodeOrText::Code(
                item.properties
                    .get("text")
                    .and_then(|p| p.clone().one())
                    .and_then(|prop| prop.try_into_string())
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
            ),
        })
        .collect();

    let date = chrono::NaiveDateTime::parse_from_str(
        date.one()
            .and_then(|prop| prop.try_into_string())
            .unwrap_or_default()
            .as_str(),
        "%Y-%m-%dT%H:%M:%S",
    )
    .ok()?;

    let upvotes = upvotes
        .one()
        .and_then(|prop| prop.try_into_string())
        .and_then(|s| s.parse().ok())?;

    let url = url
        .one()
        .and_then(|prop| prop.try_into_string())
        .and_then(|s| Url::parse(&s).or_else(|_| webpage_url.join(&s)).ok())?;

    Some(StackOverflowAnswer {
        body: text,
        date: format!("{}", date.date().format("%b %d, %Y")),
        upvotes,
        url: url.to_string(),
        accepted,
    })
}

fn schema_item_to_stackoverflow_answer(
    item: schema_org::Item,
    url: Url,
    accepted: bool,
) -> Option<StackOverflowAnswer> {
    match (
        item.properties.get("text"),
        item.properties.get("dateCreated"),
        item.properties.get("upvoteCount"),
        item.properties.get("url"),
    ) {
        (Some(text), Some(date), Some(upvotes), Some(answer_url)) => parse_so_answer(
            text.clone(),
            date.clone(),
            upvotes.clone(),
            answer_url.clone(),
            url,
            accepted,
        ),
        _ => None,
    }
}

pub fn stackoverflow_snippet(webpage: &RetrievedWebpage) -> Result<Snippet> {
    match webpage
        .schema_org
        .iter()
        .find(|item| item.types_contains("QAPage"))
        .and_then(|item| item.properties.get("mainEntity"))
        .and_then(|properties| properties.clone().one())
        .and_then(|property| property.try_into_item())
    {
        Some(item) => {
            let question: Vec<CodeOrText> = item
                .properties
                .get("text")
                .map(|item| item.clone().many())
                .unwrap_or_default()
                .into_iter()
                .map(|prop| match prop {
                    Property::String(s) => CodeOrText::Text(s),
                    Property::Item(item) => CodeOrText::Code(
                        item.properties
                            .get("text")
                            .and_then(|p| p.clone().one())
                            .and_then(|prop| prop.try_into_string())
                            .unwrap_or_default(),
                    ),
                })
                .collect();

            let mut answers = Vec::new();

            if let Some(ans) = item
                .properties
                .get("acceptedAnswer")
                .cloned()
                .and_then(|ans| ans.one())
                .and_then(|prop| prop.try_into_item())
                .and_then(|item| {
                    schema_item_to_stackoverflow_answer(
                        item,
                        Url::parse(&webpage.url).unwrap(),
                        true,
                    )
                })
            {
                answers.push(ans);
            }

            for answer in item
                .properties
                .get("suggestedAnswer")
                .cloned()
                .map(|answers| answers.many())
                .unwrap_or_default()
                .into_iter()
                .filter_map(|prop| prop.try_into_item())
                .filter_map(|item| {
                    schema_item_to_stackoverflow_answer(
                        item,
                        Url::parse(&webpage.url).unwrap(),
                        false,
                    )
                })
            {
                answers.push(answer);
            }

            Ok(Snippet::StackOverflowQA {
                question: StackOverflowQuestion { body: question },
                answers: answers.into_iter().take(3).collect(),
            })
        }
        None => Err(Error::InvalidStackoverflowSchema.into()),
    }
}

pub fn create_stackoverflow_sidebar(schema_org: Vec<Item>, url: Url) -> Result<Sidebar> {
    if let Some(item) = schema_org
        .into_iter()
        .find(|item| item.types_contains("QAPage"))
        .and_then(|item| item.properties.get("mainEntity").cloned())
        .and_then(|properties| properties.one())
        .and_then(|property| property.try_into_item())
    {
        let title = item
            .properties
            .get("name")
            .cloned()
            .and_then(|prop| prop.one())
            .and_then(|prop| prop.try_into_string())
            .ok_or(Error::InvalidStackoverflowSchema)?;

        item.properties
            .get("acceptedAnswer")
            .cloned()
            .and_then(|ans| ans.one())
            .and_then(|prop| prop.try_into_item())
            .and_then(|item| schema_item_to_stackoverflow_answer(item, url, true))
            .map(|answer| Sidebar::StackOverflow { title, answer })
            .ok_or(Error::InvalidStackoverflowSchema.into())
    } else {
        Err(Error::InvalidStackoverflowSchema.into())
    }
}
