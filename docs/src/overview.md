# Overview
Stract (and most other web search engines) is composed of three main components: the crawler, the webgraph and the search index.

## Crawler
The crawler, often also referred to as a spider or bot, is the component responsible for collecting and scanning websites across the internet. It begins with a seed list of URLs, which it visits to fetch web pages. The crawler then parses these pages to extract additional URLs, which are then added to the list of URLs to be crawled in the future. This process repeats in a cycle, allowing the crawler to discover new web pages or updates to existing pages continuously. The content fetched by the crawler is passed on to the next components of the search engine: the webgraph and the search index.

## Webgraph
The webgraph is a data structure that represents the relationships between different web pages. Each node in the webgraph represents a unique web page, and each edge represents a hyperlink from one page to another. The webgraph helps the search engine understand the structure of the web and the authority of different web pages. Authority is determined by factors such as the number of other pages linking to a given page (also known as "backlinks"), which is an important factor in ranking search results. This concept is often referred to as "link analysis."

## Search Index
The search index is the component that facilitates fast and accurate search results. It is akin to the index at the back of a book, providing a direct mapping from words or phrases to the web pages in which they appear. This data structure is often referred to as an "inverted index". The search index is designed to handle complex search queries and return relevant results in a fraction of a second. The index uses the information gathered by the crawler and the structure of the webgraph to rank search results according to their relevance.
