# Overview
Stract (and most other web search engines) is composed of three main components: the crawler, the web graph and the search index.

## Crawler
The crawler, often also referred to as a spider or bot, is the component responsible for collecting and scanning websites across the internet. It begins with a seed list of URLs, which it visits to fetch web pages. The crawler then parses these pages to extract additional URLs, which are then added to the list of URLs to be crawled in the future. This process repeats in a cycle, allowing the crawler to discover new web pages or updates to existing pages continuously. The content fetched by the crawler is passed on to the next components of the search engine: the web graph and the search index.

## Web graph
The web graph is a data structure that represents the relationships between different web pages. Each node in the web graph represents a unique web page, and each edge represents a hyperlink from one page to another. The web graph helps the search engine understand the structure of the web and the authority of different web pages. Stract uses the [harmonic centrality](webgraph.md#harmonic-centrality) to determine the authority of a webpage.

## Search Index
The search index is the component that facilitates fast and accurate search results. It is akin to the index at the back of a book, providing a direct mapping from words or phrases to the web pages in which they appear. This data structure is often referred to as an "inverted index". The search index is designed to handle complex search queries and return relevant results in a fraction of a second. The index uses the information gathered by the crawler and the structure of the web graph to rank search results according to their relevance.
