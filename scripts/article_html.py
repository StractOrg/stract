import argparse
import sys

try:
    from libzim.reader import Archive
    from libzim.search import Query, Searcher
except ImportError:
    sys.exit("Please install libzim (pip install libzim)")

parser = argparse.ArgumentParser(description="Dump a specific article from a ZIM file")

parser.add_argument("--zim-file", help="Path to the ZIM file", type=str, default="data/test.zim")
parser.add_argument("--url", help="If the query should be treated as a direct URL", action="store_true", default=False)
parser.add_argument("query", help="Query to search for", type=str)

args = parser.parse_args()

zim = Archive(args.zim_file)

if args.url:
    path = f"A/{args.query}"
else:
    query = Query().set_query(args.query)
    searcher = Searcher(zim)
    search = searcher.search(query)
    search_count = search.getEstimatedMatches()
    res = list(search.getResults(0, 1))
    path = res[0]

article = zim.get_entry_by_path(path)
print(bytes(article.get_item().content).decode("UTF-8"))
