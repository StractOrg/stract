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

use kuchiki::NodeRef;

use super::SchemaOrg;

pub fn parse(root: NodeRef) -> Vec<SchemaOrg> {
    let mut res = Vec::new();

    for node in root.select("script").unwrap().filter(|node| {
        matches!(
            node.attributes.borrow().get("type"),
            Some("application/ld+json")
        )
    }) {
        let text_contens = node.text_contents();
        let content = text_contens.trim();

        if let Ok(schema) = serde_json::from_str(content) {
            res.push(schema);
        }
    }

    res
}

#[cfg(test)]
mod tests {
    use kuchiki::traits::TendrilSink;

    use crate::webpage::schema_org::{
        CreativeWork, ImageObject, MediaObject, OneOrMany, PersonOrOrganization, Thing,
    };

    use super::*;

    #[test]
    fn schema_dot_org_json_ld() {
        let root = kuchiki::parse_html().one(
            r#"
    <html>
        <head>
            <script type="application/ld+json">
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
            </script>
        </head>
        <body>
        </body>
    </html>
        "#,
        );

        let res = parse(root);

        assert_eq!(res.len(), 1);

        assert_eq!(
            res,
            vec![SchemaOrg::ImageObject(ImageObject {
                media_object: MediaObject {
                    creative_work: CreativeWork {
                        thing: Thing {
                            name: Some(OneOrMany::One(Box::new("Beach in Mexico".to_string()))),
                            description: Some(OneOrMany::One(Box::new(
                                "I took this picture while on vacation last year.".to_string()
                            ))),
                            ..Default::default()
                        },
                        author: Some(OneOrMany::One(Box::new(PersonOrOrganization::Name(
                            "Jane Doe".to_string()
                        )))),
                    },
                    content_url: Some(OneOrMany::One(Box::new("mexico-beach.jpg".to_string()))),
                }
            })],
        );
    }

    #[test]
    fn no_schema_dot_org_json_ld() {
        let html = r#"
    <html>
        <head>
            <script>
                {
                "invalid": "schema"
                }
            </script>
        </head>
        <body>
        </body>
    </html>
        "#;

        let root = kuchiki::parse_html().one(html);
        let res = parse(root);
        assert!(res.is_empty());
    }
}
