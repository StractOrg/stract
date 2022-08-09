// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

#[non_exhaustive]
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(tag = "@type")]
pub enum SchemaOrg {
    ImageObject(ImageObject),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ImageObject {
    pub name: Option<String>,
    pub description: Option<String>,
    pub author: Option<String>,
    #[serde(rename = "contentUrl")]
    pub content_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_object_example() {
        // example taken from https://schema.org/ImageObject
        let json = r#"
        {
            "@context": "https://schema.org",
            "@type": "ImageObject",
            "author": "Jane Doe",
            "contentLocation": "Puerto Vallarta, Mexico",
            "contentUrl": "mexico-beach.jpg",
            "datePublished": "2008-01-25",
            "description": "I took this picture while on vacation last year.",
            "name": "Beach in Mexico"
          }
        "#;

        let parsed: SchemaOrg = serde_json::from_str(json).unwrap();
        assert_eq!(
            parsed,
            SchemaOrg::ImageObject(ImageObject {
                name: Some("Beach in Mexico".to_string()),
                description: Some("I took this picture while on vacation last year.".to_string()),
                author: Some("Jane Doe".to_string()),
                content_url: Some("mexico-beach.jpg".to_string()),
            }),
        )
    }
}
