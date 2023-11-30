import numpy as np
import re
from llama_cpp import Llama
import sqlite3
from tqdm import tqdm
import json
import time
import requests

NUM_RESULTS_PER_QUERY = 20
NUM_ENSEMBLE_PREDS = 1
PROMPT = """<|im_start|>system
Perform the task to the best of your ability.<|im_end|>
<|im_start|>user
Think about this step-by-step. You task is to evaluate a single search result on an integer scale of 0-4, where a higher score means the result is more relevant. A relevant result should come from a trust-worthy source and answer the users query. The final relevancy should be on a new line on the form "Relevancy: {{}}". It is very important that you think about the steps before giving the final "Relevancy: {{}}" score.

Query: "{}"
Url: "{}"
Title: "{}"
Snippet: "{}"<|im_end|>
<|im_start|>assistant
"""
llm = Llama(
    n_gpu_layers=-1,
    n_ctx=8000,
    model_path="data/neuralhermes-2.5-mistral-7b.Q5_K_M.gguf",
)


with open("data/queries_us.csv") as f:
    all_queries = [line.strip() for line in f.readlines()]


def setup_db():
    db = sqlite3.connect("data/auto-ranking-annotation.sqlite")

    cur = db.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS queries (
            qid UUID PRIMARY KEY DEFAULT (HEX(RANDOMBLOB(16))),
            query TEXT NOT NULL UNIQUE
        );"""
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_results (
            qid UUID NOT NULL,
            url TEXT NOT NULL,
            orig_rank INTEGER NOT NULL,
            webpage_json TEXT NOT NULL,
            annotation INTEGER,
            PRIMARY KEY (qid, url)
        );"""
    )

    db.commit()

    return db


db = setup_db()

cur = db.cursor()
for query in all_queries:
    if len(query) < 3:
        continue

    # check if query has large percentage of non-alphanumeric characters
    if sum([c.isalnum() for c in query]) / len(query) < 0.5:
        continue

    if len(query) > 100:
        continue

    cur.execute(
        """
        INSERT OR IGNORE INTO queries (query)
        VALUES (?)
        """,
        (query,),
    )

db.commit()

unannotated_queries = cur.execute(
    """
    SELECT qid, query
    FROM queries
    WHERE NOT EXISTS (
        SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.annotation IS NOT NULL
    )
    ORDER BY qid
    """,
).fetchall()

unannotated_queries = {qid: query for qid, query in unannotated_queries}


def simplify_snippet(snippet):
    if "text" not in snippet:
        return ""

    return "".join([f["text"] for f in snippet["text"]["fragments"]])


def get_search_results(query):
    url = "https://trystract.com/beta/api/search"

    payload = {
        "query": query,
        "numResults": NUM_RESULTS_PER_QUERY,
        "returnRankingSignals": True,
    }

    return [
        {
            "url": w["url"],
            "title": w["title"],
            "rankingSignals": {s: v["value"] for (s, v) in w["rankingSignals"].items()},
            "snippet": simplify_snippet(w["snippet"]),
        }
        for w in requests.post(url, json=payload).json()["webpages"][
            :NUM_RESULTS_PER_QUERY
        ]
    ]


# add search results to db
print("Adding search results to db")
for qid, query in tqdm(unannotated_queries.items()):
    has_results = (
        cur.execute("SELECT 1 FROM search_results WHERE qid = ?", (qid,)).fetchone()
        is not None
    )
    if has_results:
        continue

    results = get_search_results(query)
    time.sleep(1)

    for i, result in enumerate(results):
        cur.execute(
            """
            INSERT INTO search_results (qid, url, orig_rank, webpage_json)
            VALUES (?, ?, ?, ?)
            """,
            (qid, result["url"], i, json.dumps(result)),
        )

    db.commit()


def get_prompt(query, url, title, snippet):
    return PROMPT.format(
        query,
        url,
        title,
        snippet,
    )


def get_relevancy(res):
    regex = r"Relevancy: (\d)"
    matches = re.findall(regex, res)
    if len(matches) == 0:
        return None
    return int(matches[0])


for qid, query in tqdm(unannotated_queries.items()):
    unnanotated_results = cur.execute(
        """
        SELECT url, orig_rank, webpage_json
        FROM search_results
        WHERE qid = ? AND annotation IS NULL
        ORDER BY orig_rank
        """,
        (qid,),
    ).fetchall()

    for url, orig_rank, webpage_json in tqdm(unnanotated_results):
        webpage = json.loads(webpage_json)
        prompt = get_prompt(query, url, webpage["title"], webpage["snippet"])
        res = 0
        n = 0
        for _ in range(NUM_ENSEMBLE_PREDS):
            output = llm.create_completion(
                prompt,
                max_tokens=-1,
                echo=False,
                temperature=0.4,
                stop=["<|im_start|>", "<|im_end|>"],
            )
            output = output["choices"][0]["text"]

            relevancy = get_relevancy(output)
            if relevancy is None:
                continue

            n += 1
            res += relevancy

        if n == 0:
            print("No relevancy annotation for", query, url)
            continue
        relevancy = np.round(res / n).astype(int)
        relevancy = max(0, min(4, int(relevancy)))

        cur.execute(
            """
            UPDATE search_results
            SET annotation = ?
            WHERE qid = ? AND url = ?
            """,
            (relevancy, qid, url),
        )

        db.commit()
