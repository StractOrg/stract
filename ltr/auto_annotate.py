import numpy as np
from pprint import pprint
from llama_cpp import Llama
import sqlite3
from tqdm import tqdm
import json
import time
import requests
import random

# API = "https://stract.com/beta/api/search"
API = "http://localhost:3000/beta/api/search"
NUM_RESULTS_PER_QUERY = 20

ELO_K = 32
ELO_SCALE = 400
ELO_ROUNDS_MULT = 5
NUM_LABELS = 4

# PROMPT = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>
# 
# You are a helpful, smart, kind, and efficient AI assistant. You always fulfill the user's requests to the best of your ability.<|eot_id|><|start_header_id|>user<|end_header_id|>
# 
# Think about this step-by-step. You are a search engine evaluator and your task is to evaluate search results based on how well the result matches the query
# You will be shown two results for each query and most choose which result is best for the users query. A good result most answer the users query and come from an authoritative source.
# To choose the best result, write "Best: RESULT_A" or "Best: RESULT_B". Before choosing the best result, you should first evaluate the relevance of each result to the query.
# 
# Query: "{}"
# 
# RESULT_A:
# Url: "{}"
# Title: "{}"
# Snippet: "{}"
# 
# RESULT_B:
# Url: "{}"
# Title: "{}"
# Snippet: "{}"
# 
# Evaluation:<|eot_id|><|start_header_id|>assistant<|end_header_id|>
# 
# """

PROMPT = """<start_of_turn>user
Think about this step-by-step. You are a search engine evaluator and your task is to evaluate search results based on how well the result matches the query
You will be shown two results for each query and most choose which result is best for the users query. A good result most answer the users query and come from an authoritative source.
To choose the best result, write "Best: RESULT_A" or "Best: RESULT_B". Before choosing the best result, you should first evaluate the relevance of each result to the query.

Query: "{}"

RESULT_A:
Url: "{}"
Title: "{}"
Snippet: "{}"

RESULT_B:
Url: "{}"
Title: "{}"
Snippet: "{}"<end_of_turn>
<start_of_turn>model
"""
llm = Llama(
    n_gpu_layers=-1,
    n_ctx=8000,
    # model_path="data/Meta-Llama-3.1-8B-Instruct-Q8_0.gguf",
    model_path="data/gemma-2-2b-it-abliterated-Q6_K_L.gguf",
    repeat_penalty=False,
    no_penalize_nl=True,
    verbose=False,
)


with open("data/queries_us.csv") as f:
    all_queries = [line.strip() for line in f.readlines()]

# shuffle queries
np.random.shuffle(all_queries)


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
            annotation INTEGER DEFAULT NULL,
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

    # only consider queries with at least two words
    if len(query.split()) < 2:
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
    payload = {
        "query": query,
        "numResults": NUM_RESULTS_PER_QUERY,
        "returnRankingSignals": True,
        'signalCoefficients': {
            'host_centrality_rank': 0.022,
            'page_centrality_rank': 0.022,
            }
    }

    return [
        {
            "url": w["url"],
            "title": w["title"],
            "rankingSignals": {s: v["value"] for (s, v) in w["rankingSignals"].items()},
            "snippet": simplify_snippet(w["snippet"]),
        }
        for w in requests.post(API, json=payload).json()["webpages"][
            :NUM_RESULTS_PER_QUERY
        ]
    ]


def add_results(query):
    has_results = (
        cur.execute("SELECT 1 FROM search_results WHERE qid = ?", (qid,)).fetchone()
        is not None
    )
    if has_results:
        return

    results = get_search_results(query)
    time.sleep(1)

    for i, result in enumerate(results):
        cur.execute(
            """
            INSERT OR IGNORE INTO search_results (qid, url, orig_rank, webpage_json)
            VALUES (?, ?, ?, ?)
            """,
            (qid, result["url"], i, json.dumps(result)),
        )

    db.commit()


def get_prompt(query, url_a, title_a, snippet_a, url_b, title_b, snippet_b):
    return PROMPT.format(
        query,
        url_a,
        title_a,
        snippet_a,
        url_b,
        title_b,
        snippet_b,
    )


def get_best(res):
    if "Best: RESULT_A" in res:
        return 1
    if "Best: RESULT_B" in res:
        return 0
    return None

def elo_update(winner, loser, elo):
    p_winner = 1 / (1 + 10 ** ((elo[loser] - elo[winner]) / ELO_SCALE))
    p_loser = 1 - p_winner

    elo[winner] += ELO_K * (1 - p_winner)
    elo[loser] += ELO_K * (0 - p_loser)

    return elo


for qid, query in tqdm(unannotated_queries.items()):
    add_results(query)
    unnanotated_results = cur.execute(
        """
        SELECT url, orig_rank, webpage_json
        FROM search_results
        WHERE qid = ? AND annotation IS NULL
        ORDER BY orig_rank
        """,
        (qid,),
    ).fetchall()

    elo = {url: ELO_SCALE // 2 for url, _, _ in unnanotated_results}

    for _ in tqdm(range(0, ELO_ROUNDS_MULT * len(unnanotated_results))):
        (url_a, _, json_a), (url_b, _, json_b)= random.sample(unnanotated_results, 2)

        webpage_a = json.loads(json_a)
        webpage_b = json.loads(json_b)
        prompt = get_prompt(query, url_a, webpage_a["title"], webpage_a["snippet"], url_b, webpage_b["title"], webpage_b["snippet"])
        output = llm.create_completion(
            prompt,
            max_tokens=1024,
            echo=False,
            temperature=0.4,
            stop=["<|start_header_id|>", "<|eot_id|>"],
        )
        output = output["choices"][0]["text"]

        relevancy = get_best(output)
        if relevancy is not None:
            if relevancy == 1:
                elo = elo_update(url_a, url_b, elo)
            else:
                elo = elo_update(url_b, url_a, elo)

    # sort resutls by elo and assign labels based on their rank
    elo = {url: elo[url] for url, _, _ in unnanotated_results}
    elo = sorted(elo.items(), key=lambda x: x[1], reverse=True)
    elo = [{"url": url} for url, _ in elo]

    for i in range(len(elo)):
        # elo[i]['label'] = (NUM_LABELS - i // (len(elo) // NUM_LABELS))
        elo[i]['label'] = NUM_LABELS - int(np.log2(i + 1))

    print(query)
    pprint(elo)
    for website in elo:
        url = website['url']
        relevancy = website['label']
        cur.execute(
            """
            UPDATE search_results
            SET annotation = ?
            WHERE qid = ? AND url = ?
            """,
            (relevancy, qid, url),
        )
        db.commit()
