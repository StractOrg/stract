# Optics
Optics is a domain specific language that is intended to give the user full control over which search results gets returned.

- If the user has specified an optic url, we grab the file from the url and parses it.
- Compiles down to tantivy queries that are matched against the inverted index.
