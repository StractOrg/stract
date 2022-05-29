use crate::schema::Field;
use tantivy::fastfield::FastFieldReader;
use tantivy::{
    collector::{Collector, TopDocs},
    DocId, Score, SegmentReader,
};

pub(crate) fn initial_collector() -> impl Collector<Fruit = Vec<(f64, tantivy::DocAddress)>> {
    TopDocs::with_limit(20).tweak_score(move |segment_reader: &SegmentReader| {
        let centrality_field = segment_reader
            .schema()
            .get_field(Field::Centrality.as_str())
            .expect("Faild to load centrality field");
        let centrality_reader = segment_reader
            .fast_fields()
            .f64(centrality_field)
            .expect("Failed to get centrality fast-field reader");

        move |doc: DocId, original_score: Score| {
            let centrality = centrality_reader.get(doc);
            original_score as f64 + 100.0 * centrality
        }
    })
}

#[cfg(test)]
mod tests {
    
    use crate::{
        query::Query,
        search_index::Index,
        webpage::{Link, Webpage},
    };

    #[test]
    fn harmonic_ranking() {
        let query = Query::parse("great site").expect("Failed to parse query");

        for _ in 0..10 {
            let mut index = Index::temporary().expect("Unable to open index");

            index
                .insert(Webpage::new(
                    r#"
                        <html>
                            <head>
                                <title>Website A</title>
                            </head>
                            <a href="https://www.b.com">B site is great</a>
                        </html>
                    "#,
                    "https://www.a.com",
                    vec![],
                    0.0,
                ))
                .expect("failed to parse webpage");
            index
                .insert(Webpage::new(
                    r#"
                        <html>
                            <head>
                                <title>Website B</title>
                            </head>
                        </html>
                    "#,
                    "https://www.b.com",
                    vec![Link {
                        source: "https://www.a.com".to_string(),
                        destination: "https://www.b.com".to_string(),
                        text: "B site is great".to_string(),
                    }],
                    5.0,
                ))
                .expect("failed to parse webpage");

            index.commit().expect("failed to commit index");
            let result = index.search(&query).expect("Search failed");
            assert_eq!(result.documents.len(), 2);
            assert_eq!(result.documents[0].url, "https://www.b.com");
            assert_eq!(result.documents[1].url, "https://www.a.com");
        }
    }
}
