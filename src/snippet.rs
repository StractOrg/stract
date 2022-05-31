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

use tantivy::Searcher as TantivySearcher;
use tantivy::SnippetGenerator;

use crate::{query::Query, schema::Field, Result};

/// For now we use the snippet generator from tantivy.
/// In the future we want to implement something closer to the method described in https://cs.pomona.edu/~dkauchak/ir_project/whitepapers/Snippet-IL.pdf.
/// This will require us to store each paragraph of the webpage separately to get adequate performance.
/// Implementing SnippetIL will also allow us to correctly add "..." to the snippet.
pub fn generate(query: &Query, text: &str, searcher: &TantivySearcher) -> Result<String> {
    let generator = SnippetGenerator::create(
        searcher,
        &query.tantivy(searcher.schema(), searcher.index().tokenizers()),
        searcher
            .schema()
            .get_field(Field::Body.as_str())
            .expect("Failed to get body field"),
    )?;

    let snippet = generator.snippet(text);
    let highlighted = snippet.to_html() + "...";

    Ok(highlighted)
}

#[cfg(test)]
mod tests {
    use crate::{index::Index, searcher::Searcher, webpage::Webpage};

    use super::*;

    const TEST_TEXT: &str = r#"Rust is a systems programming language sponsored by
Mozilla which describes it as a "safe, concurrent, practical language", supporting functional and
imperative-procedural paradigms. Rust is syntactically similar to C++[according to whom?],
but its designers intend it to provide better memory safety while still maintaining
performance.
Rust is free and open-source software, released under an MIT License, or Apache License
2.0. Its designers have refined the language through the experiences of writing the Servo
web browser layout engine[14] and the Rust compiler. A large proportion of current commits
to the project are from community members.[15]
Rust won first place for "most loved programming language" in the Stack Overflow Developer
Survey in 2016, 2017, and 2018."#;

    #[test]
    fn snippet_during_search() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                &format!(
                    r#"
                        <html>
                            <head>
                                <title>Website for runners</title>
                            </head>
                            {}
                        </html>
                    "#,
                    TEST_TEXT
                ),
                "https://www.example.com",
                vec![],
                1.0,
            ))
            .expect("failed to parse webpage");
        index.commit().expect("failed to commit index");

        let searcher = Searcher::from(index);

        let result = searcher.search("rust language").expect("Search failed");

        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(
            result.documents[0].snippet,
                "<b>Rust</b> is a systems programming <b>language</b> sponsored by\nMozilla which describes it as a &quot;safe, concurrent, practical <b>language</b>&quot;, supporting functional and..."
                .to_string()
        );
    }
}
