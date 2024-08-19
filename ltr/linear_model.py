import numpy as np
import json
import sqlite3
import stract
from scipy.optimize import differential_evolution
import random
from pprint import pprint


MAX_WEIGHT = 10
NUM_QUERIES_FOR_EVAL = 80

con = sqlite3.connect("data/auto-ranking-annotation.sqlite")
cur = con.cursor()

res = cur.execute(
    """
        SELECT qid, query
		FROM queries
        WHERE EXISTS (
			SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.annotation > 0
		)
"""
)

accepted_queries = set()
with open("data/queries_us.csv") as f:
    for query in f.readlines():
        if len(query.strip()) > 1:
            accepted_queries.add(query.strip().lower())

queries = {
    qid: {"query": query}
    for qid, query in res.fetchall()
    if query.lower() in accepted_queries
}

for qid in queries:
    res = cur.execute(
        """
            SELECT qid, url, annotation, orig_rank, webpage_json
            FROM search_results
            WHERE qid = ?
    """,
        (qid,),
    )
    urls = {
        url: {
            "label": label,
            "orig_rank": orig_rank,
            "signals": json.loads(page)["rankingSignals"],
        }
        for _, url, label, orig_rank, page in res.fetchall()
    }
    urls = [
        (url, w["label"], w["orig_rank"], w["signals"])
        for url, w in urls.items()
        if w["label"] is not None
    ]

    sorting_key = lambda x: (
        -x[1],
        x[2],
    )

    urls = sorted(urls, key=sorting_key)
    queries[qid]["urls"] = urls

feature2id = {}
id2feature = {}

for qid, data in queries.items():
    for url, score, _, signals in data["urls"]:
        for feature, value in signals.items():
            if feature not in feature2id:
                id = len(feature2id)
                feature2id[feature] = id
                id2feature[id] = feature

queries = [
    (q["query"], [u[0] for u in q["urls"] if u[1] > 0]) for q in queries.values()
]
queries = random.sample(queries, NUM_QUERIES_FOR_EVAL)
pprint([q for q, _ in queries])

bounds = [(0, MAX_WEIGHT) for _ in range(len(feature2id))]


def eval_query(query, expected_urls, weights):
    coeffs = None

    if len(weights) != 0:
        coeffs = {id2feature[i]: w for i, w in enumerate(weights)}
        coeffs["lambda_mart"] = 10.0

    res = stract.search(query, signal_coefficients=coeffs)
    return sum([1 for r in res if r["url"] in expected_urls])


cache = {}


def eval_weights(weights):
    if tuple(weights) in cache:
        return cache[tuple(weights)]

    _queries = queries
    total = sum([len(urls) for _, urls in _queries])

    res = sum([eval_query(q, urls, weights) for q, urls in _queries]) / total

    cache[tuple(weights)] = res

    return res


def optim(weights):
    res = -eval_weights(weights)
    print("Score:", res)
    return res


def callback(intermediate_result):
    pprint({id2feature[i]: w for i, w in enumerate(intermediate_result.x)})
    print("Score:", -intermediate_result.fun)


print("baseline", eval_weights([]))

result = differential_evolution(
    optim,
    bounds,
    maxiter=100,
    popsize=2,
    disp=False,
    polish=False,
    callback=callback,
)

weights = {id2feature[i]: w for i, w in enumerate(result.x)}

print("Best weights")
pprint(weights)
