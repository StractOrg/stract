import argparse
import sys

try:
    from libzim.reader import Archive
    from libzim.search import Query, Searcher
except ImportError:
    sys.exit("Please install libzim (pip install libzim)")

parser = argparse.ArgumentParser(description="Dump a specific article from a ZIM file")

parser.add_argument("--zim_file", help="Path to the ZIM file", type=str, default="data/test.zim")
parser.add_argument("query", help="Query to search for", type=str)

args = parser.parse_args()

zim = Archive(args.zim_file)

query = Query().set_query(args.query)
searcher = Searcher(zim)
search = searcher.search(query)
search_count = search.getEstimatedMatches()
res = list(search.getResults(0, 1))

article = zim.get_entry_by_path(res[0])
print(bytes(article.get_item().content).decode("UTF-8"))
