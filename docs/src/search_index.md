# Search Index
Imagine you are a librarian at a large library. You have a vast collection of books, but they are all unorganized. Let's say a visitor comes by and asks you about a cooking book that has recipes for pasta. How would you find the relevant books? You could start by looking at the table-of-content for each book, but that would take a long time. What if you had a list of all the words in each book and the pages on which they appear? That would make it much easier to find the books the visitor is looking for. This is essentially what a search index is: a list of words and the documents in which they appear. This is called an inverted index, since it inverts the relationship between words and documents.

We use [tantivy](https://github.com/quickwit-oss/tantivy/) as our inverted index. Tantivy is a full-text search engine library written in Rust and inspired by [Lucene](https://lucene.apache.org/). An excellent overview of how tantivy works can be found [here](https://github.com/quickwit-oss/tantivy/blob/main/ARCHITECTURE.md).

## Indexing
To built the index an indexer downloads one of the warc files that the crawler has saved in S3, and iterates over all the saved webpages in that file. This continues until all warc files have been processed.

When processing a webpage the indexer extracts various information from the page (the title, clean body text, language detection, update timestamps etc.). The text information is then passed through a tokenizer that's responsible for converting the text to a list of tokens:
$$\texttt{"This is a-test"} \rightarrow \texttt{["this", "is", "a", "-", "test"]}$$

The exact tokenization depends on the language of the page, but the idea is to split the text into "tokens" and normalize each token. The webpage is then appended to the posting list for each of the terms.

As discussed under the [webgraph](/webgraph/#harmonic-centrality) section, some ranking signals are calculated based on how the webpages link to eachoter. As these pages are independent from the queries the users execute, they can be calculated during indexing. The signals are then used to sort the posting lists for all the terms so that the best ranked webpages are placed in the front of the posting lists.
This provides a huge speedup when searching since we don't have to consider all the potential webpages in each posting list.

Since a tantivy index consists of a number of independent segments, the indexing parallelized across many indexers. The resulting indexes are then merged in the end by simply creating a new index with all the resulting segments.

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
