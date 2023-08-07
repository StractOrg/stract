# Webgraph
The webgraph, often conceptualized as the "internet's map," provides a structured view of the interconnectedness of pages across the World Wide Web. With billions of pages linked together, the webgraph is a crucial tool for understanding the structure, pattern, and dynamics of the internet.

There are two primary ways of constructing the webgraph:

- **Page-Level Webgraph**: This method involves constructing the graph by analyzing individual pages and their outbound links. The nodes in this graph represent individual web pages, while the edges represent hyperlinks between them. This detailed view is especially helpful for understanding specific page connections.

- **Host-Level Webgraph**: Instead of examining individual pages, this approach consolidates all the links associated with a particular host, effectively simplifying the webgraph. In this representation, nodes represent entire websites or hosts, and edges represent connections between them. This broader perspective is suitable for understanding the authority and influence of entire websites.

## Segments
Given the extreme size of the internet, managing the webgraph as a single monolithic structure in memory is neither efficient nor practical. Thus, it's segmented into smaller parts called segments. Each segment is essentially a portion of the overall webgraph stored in a [RocksDB](https://rocksdb.org/) database on disk. This allows us to create webgraphs that are much larger than what we would be able to fit in memory.

## Webgraph Uses
### Harmonic Centrality
### Inbound Similarity

