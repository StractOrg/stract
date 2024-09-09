# really hacky way to import from parent dir
import sys
sys.path.append("ltr")

import argparse
import random
import stract
import sqlite3
from tqdm import tqdm
import leechy

parser = argparse.ArgumentParser()

parser.add_argument('--queries-path', default="data/queries_us.csv", help='Path to queries')
parser.add_argument('--db-path', default="data/eval.sqlite", help='Path to resulting eval database')
parser.add_argument('--limit', type=int, default=None, help='Limit number of queries')

args = parser.parse_args()

coeffs = {
    'bm25_title': 0.04,
    'bm25_stemmed_title': 0.06,
    'bm25_title_bigrams': 0.04,
    'bm25_title_trigrams': 0.04,
    'title_coverage': 0.2,
    'idf_sum_title_if_homepage': 0.05,

    'bm25_stemmed_clean_body': 0.002,
    'bm25_keywords': 0.005,

    'idf_sum_url': 0.02,
    'idf_sum_domain': 0.001,
    'idf_sum_domain_name_no_tokenizer': 0.01,

    'bm25_f': 0.05,

    'bm25_backlink_text': 0.007,

    'tracker_score': 0.1,
    'host_centrality_rank': 0.04,

    # 'has_ads': 0.04,
}

queries = []
with open(args.queries_path) as f:
    for query in f.readlines():
        if len(query.strip()) > 1:
            queries.append(query.strip())

# random.shuffle(queries)
queries = list(reversed(queries))

if args.limit:
    queries = queries[:args.limit]

with sqlite3.connect(args.db_path) as conn:
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS queries (
            qid INTEGER PRIMARY KEY,
            query TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS results (
            rid INTEGER PRIMARY KEY,
            qid INTEGER,
            rank INTEGER,
            url TEXT,
            signals JSON
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS golden (
            gid INTEGER PRIMARY KEY,
            qid INTEGER,
            url TEXT,
            signals JSON,
            rank INTEGER
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_golden_qid ON golden (qid)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_qid ON results (qid)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_queries_query ON queries (query)")


    eng = leechy.Engine()
    for query in tqdm(queries):
        cursor.execute("SELECT query FROM queries WHERE query = ?", (query,))
        query_exists = cursor.fetchone() is not None

        if not query_exists:
            cursor.execute("INSERT INTO queries (query) VALUES (?)", (query,))
            conn.commit()

        cursor.execute("SELECT qid FROM queries WHERE query = ?", (query,))
        qid = cursor.fetchone()[0]

        if not query_exists:
            leechy_results = eng.search(query)
            if len(leechy_results) == 0:
                tqdm.write(f"No results found for {query}")
                sys.exit(-1)

            for (rank, url) in enumerate(leechy_results):
                res = stract.search(f'{query} exacturl:{url}', num_results=1)

                if len(res) > 0:
                    cursor.execute("INSERT INTO golden (qid, url, rank, signals) VALUES (?, ?, ?, ?)", (qid, url, rank, str(res[0]['rankingSignals'])))

        if query_exists:
            cursor.execute("DELETE FROM results WHERE qid = ?", (qid,))
            conn.commit()

        for page in range(0, 3):
            results = stract.search(query, num_results=100, page=page, signal_coefficients=coeffs)

            for (rank, result) in enumerate(results):
                rank = rank + 100*page
                cursor.execute("INSERT INTO results (qid, rank, url, signals) VALUES (?, ?, ?, ?)", (qid, rank, result['url'], str(result['rankingSignals'])))
            conn.commit()
