# Search Index
Imagine you are a librarian at a large library. You have a vast collection of books, but they are all unorganized. Let's say a visitor comes by and asks you about a cooking book that has recipes for pasta. How would you find the books? You could start by looking at the table-of-content for each book, but that would take a long time. What if you had a list of all the words in each book and the pages on which they appear? That would make it much easier to find the books the visitor is looking for. This is essentially what a search index is: a list of words and the documents in which they appear. This is called an inverted index, since it inverts the relationship between words and documents.

We use [tantivy](https://github.com/quickwit-oss/tantivy/) as our search index. Tantivy is a full-text search engine library written in Rust and inspired by [Lucene](https://lucene.apache.org/). An excellent overview of how tantivy works can be found [here](https://github.com/quickwit-oss/tantivy/blob/main/ARCHITECTURE.md).

## Indexing
- Can be done distributed, since a tantivy index consists of multiple segments.

- Tokenization

- Ordered by all ranking signals that are computable before the query is known.

## Searching
- Distributed searcher
    - Lookup a local searcher at each search server (one per shard)

- intersection query

- Search operators

## Ranking
- We don't use tantivy ranking

- Bm25 for text combined with all other signals

- Multiple stages
    - "Initial guess" based on index sorting
    - Linear regression
    - LambdaMART
    - Top 20 gets scored with a cross encoder and ranked again
