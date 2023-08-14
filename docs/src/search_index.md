# Search Index
Imagine you are a librarian at a large library. You have a vast collection of books, but they are all unorganized. Let's say a visitor comes by and asks you about a cooking book that has recipes for pasta. How would you find the relevant books? You could start by looking at the table-of-content for each book, but that would take a long time. What if you had a list of all the words in each book and the pages on which they appear? That would make it much easier to find the books the visitor is looking for. This is essentially what a search index is: a list of words and the documents in which they appear. Such a list is called an "inverted index" since it reverses the relationship between words and documents.

We use [tantivy](https://github.com/quickwit-oss/tantivy/) as our inverted index. Tantivy is a full-text search engine library written in Rust and inspired by [Lucene](https://lucene.apache.org/). An excellent overview of how tantivy works can be found [here](https://github.com/quickwit-oss/tantivy/blob/main/ARCHITECTURE.md).

## Indexing
To build the index, an indexer downloads one of the warc files that the crawler has saved in S3, and iterates over all the saved webpages in that file. This continues until all warc files have been processed.

When processing a webpage the indexer extracts various information from the page (the title, clean body text, language detection, update timestamps etc.). The extracted text is then passed through a tokenizer, which converts it into a list of tokens:
$$\texttt{"This is a-test"} \rightarrow \texttt{["this", "is", "a", "-", "test"]}$$

The exact tokenization depends on the language of the page, but the idea is to split the text into "tokens" and normalize each token. The webpage is then appended to the posting list for each of the terms.

As discussed under the [webgraph](/webgraph/#harmonic-centrality) section, some ranking signals are calculated based on how the webpages link to eachother. As these signals are independent from the queries the users execute, they can be calculated during indexing. The signals are then used to sort the posting lists for all the terms so that the best ranked webpages are placed in the front of the posting lists.
This provides a huge speedup when searching since we don't have to consider all the potential webpages in each posting list.

Since a tantivy index consists of multiple independent segments, indexing is parallelized across multiple indexers. The resulting indexes are then merged in the end by simply creating a new index with all the resulting segments.

## Searching
The index is split into a number of shards where each shard may contain multiple replicas. During search, one node gets the query from the user and is responsible for sending the query to a replica from all the other shards and combine the results.

At each shard there is a local searcher that is responsible for actually performing the search in the index. Let's say the user has searched for the query "pasta recipes". We want to find all webpages that has "pasta" in any of the indexed fields AND "recipe". Note it's fine if "recipe" is in the title and "pasta" is in the body.

The local searcher first retrieves the posting lists for "pasta" from all the indexed fields and then merges (or unions) them. It then does the same for "recipe" and performs an intersection between the resulting lists. This results in the list of webpages that contains both the word "pasta" and "recipe" in any of the fields.

Sometimes we know we only want results where the word "pasta" appears in the title. In this case, the query would be "intitle:pasta recipe". The local searchers then knows that the term "pasta" must be present in the posting list for the title field, in which case we don't need to union with the other fields.

Each shard ranks their local results and sends the best results back to the node that is responsible for combining the results for the final ranking.

## Ranking
The ranking happens in multiple stages. Some of these stages occur at the shard server, while others happen at the coordinator.

- At each shard server:
    1. The `K` first websites are retrieved from the posting lists based on the pre-calculated scores. Recall that this ranking has nothing to do with the query
    2. The results are ranked based on a linear combination of the signals. This linear regression model can be trained based on click data.
    3. If a lambdamart model has been defined, the best results from the linear regression stage gets passed into the lambdamart model.
- Combining results from all shards
    1. Results from each shard are re-ranked using both the linear regression and lambdamart models. This ensures the scores can be properly compared and ordered.
    2. The best 20 results, corresponding to the first page, gets scored with a cross encoder and again ranked using the linear regression followed by the lambdamart model.
