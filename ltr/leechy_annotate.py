import leechy
from db import Db
import stract
import numpy as np
from tqdm import tqdm
import time
import sys

NUM_LABELS = 4

with open("data/queries_us.csv") as f:
    all_queries = []
    for query in [line.strip() for line in f.readlines()]:
        if len(query) < 3:
            continue

        # check if query has large percentage of non-alphanumeric characters
        if sum([c.isalnum() for c in query]) / len(query) < 0.5:
            continue

        if len(query) > 100:
            continue

        if len(query.split()) <= 1:
            continue

        all_queries.append(query)

# shuffle queries

np.random.shuffle(all_queries)

db = Db("data/auto-ranking-annotation.sqlite")

for query in all_queries:

    db.add_query(query)


unannotated_queries = db.get_unannotated_queries()

eng = leechy.Engine()
for qid, query in tqdm(unannotated_queries.items()):
    if query not in all_queries:
        continue

    tqdm.write(query)
    leechy_results = []

    for i in range(3):
        leechy_results = eng.search(query)
        if len(leechy_results) > 0:
            break

    if len(leechy_results) == 0:
        tqdm.write(f"No results found for {query}")
        sys.exit(-1)

    for i, result in enumerate(leechy_results):
        label = NUM_LABELS - int(np.log2(i + 1))

        try:
            data = next(
                res
                for res in stract.search(f"{query} exacturl:{result}")
                if res["url"] == result
            )
        except StopIteration:
            continue

        if not data:
            continue

        tqdm.write(f"{result}: {label}")
        db.insert_result(qid, i, data)
        db.annotate(qid, result, label)

    for page in range(2, 2 + 1):
        bad_results = stract.search(query, page)

        for i, result in enumerate(bad_results):
            if result["url"] in leechy_results:
                continue

            label = 0
            tqdm.write(f'{result["url"]}: {label}')
            db.insert_result(qid, (i + 1) * page, result)
            db.annotate(qid, result["url"], label)

    time.sleep(np.random.normal(60, 20))
    tqdm.write("")
