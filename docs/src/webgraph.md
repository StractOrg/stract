# Webgraph
The webgraph, often conceptualized as the "internet's map," provides a structured view of the interconnectedness of pages across the World Wide Web. With billions of pages linked together, the webgraph is a crucial tool for understanding the structure, pattern, and dynamics of the internet.

There are two primary ways of constructing the webgraph:

- **Page-Level Webgraph**: This method involves constructing the graph by analyzing individual pages and their outbound links. The nodes in this graph represent individual web pages, while the edges represent hyperlinks between them. This detailed view is especially helpful for understanding specific page connections.

- **Host-Level Webgraph**: Instead of examining individual pages, this approach consolidates all the links associated with a particular host, effectively simplifying the webgraph. In this representation, nodes represent entire websites or hosts, and edges represent connections between them. This broader perspective is suitable for understanding the authority and influence of entire websites.

## Segments
Given the extreme size of the internet, managing the webgraph as a single monolithic structure in memory is neither efficient nor practical. Thus, it's segmented into smaller parts called segments. Each segment is essentially a portion of the overall webgraph stored in a [RocksDB](https://rocksdb.org/) database on disk. This allows us to create webgraphs that are much larger than what we would otherwise be able to fit in memory.

## Webgraph Uses
The structure of the web can provide highly valuable information when detemining the relevance of a page to a user's search query. PageRank, which is a centrality meassure developed by Larry Page and Sergey Brin, was one of the primary reasons why Google provided much better search results than their competitors in the early days.

Stract uses a similar centrality meassure called Harmonic Centrality which has been shown to satisfy some useful axioms for centrality ([paper](https://arxiv.org/abs/1308.2140)).

### Harmonic Centrality
Harmonic centrality is a measure used to identify the importance of a node within a network. In the context of a webgraph, nodes (whether they be individual pages or entire hosts) that have a high harmonic centrality are ones that are, on average, closer to all other nodes in the network. The closeness of a node in this context refers to its average distance from all other nodes.

In practical terms, a web page with high harmonic centrality might be seen as an influential page in the World Wide Web, indicating that it can be reached with fewer clicks, on average, from any other page on the internet. A page with high harmonic centrality therefore has higher likelihood for being relevant to a user's search query. 

In formulaic terms, the harmonic centrality $C_{H}(u)$ of a node $u$ is calculated as the sum of the reciprocals of the shortest paths from all nodes to $u$:

$$C_{H}(u) = \frac{1}{n-1} \sum_{v \neq u} \frac{1}{d(v,u)}$$

Where $d(v,u)$ is the shortest path from node $v$ to node $u$. We normalize the harmonic centrality by dividing by the number of nodes in the network minus one.

### Inbound Similarity
Inbound similarity plays a crucial role in enhancing personalized search results. Based on whether a user likes or dislikes results from a certain site, we can adjust the ranking of results from similar sites based on their preferences. The idea is that the similarity between two sites can be estimated by which sites that links to those sites. Two sites that has a lot of incoming links in common are likely to be topically similar.

Let's denote the set of inbound links for site $u$ as $I_{u}$ and $v$ for $I_{v}$. The similarity between two sites is calculated as the cosine similarity between their inbound link vectors:
$$S(u, v) = \frac{I_{u} \cdot I_{v}}{\|\|I_{u}\|\|\|\|I_{v}\|\|}$$

It's also this inbound similarity metric that is used to find similar sites in the *explore* feature.