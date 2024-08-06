import sklearn
import numpy as np
import json
import random
from sklearn.linear_model import LinearRegression, LogisticRegression
from pprint import pprint
import jax
import jax.numpy as jnp
import rax

TRAIN_PERCENT = 0.8

import sqlite3

con = sqlite3.connect("data/auto-ranking-annotation.sqlite")
cur = con.cursor()

res = cur.execute(
    """
        SELECT qid, query
		FROM queries
        WHERE EXISTS (
			SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.annotation IS NOT NULL
		)
"""
)
queries = {qid: {"query": query} for qid, query in res.fetchall()}

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

# convert to [{ranking_signals, score}]
scores = []

random.shuffle(scores)

sorted_features = sorted(feature2id.items(), key=lambda x: x[1])

for qid, data in queries.items():
    sum_scores = sum([score for _, score, _, _ in data["urls"]])
    for i, (url, score, _, signals) in enumerate(data["urls"]):
        signals = {feature2id[k]: v for k, v in signals.items()}

        signals = [signals.get(k, 0) for _, k in sorted_features]

        scores.append({"ranking_signals": signals, "score": score})

train = scores[: int(len(scores) * TRAIN_PERCENT)]
test = scores[int(len(scores) * TRAIN_PERCENT) :]

X_train = []
y_train = []
for score in train:
    X_train.append(score["ranking_signals"])
    y_train.append(score["score"])

X_test = []
y_test = []
for score in test:
    X_test.append(score["ranking_signals"])
    y_test.append(score["score"])

X_train = jnp.array(X_train)
y_train = jnp.array(y_train)
X_test = jnp.array(X_test)
y_test = jnp.array(y_test)

# jax model
w = jnp.zeros(X_train.shape[1])

def model(w, X):
    return jnp.dot(X, w)

def loss(w, batch):
    features, labels, mask = batch
    scores = model(w, features)

    return rax.approx_t12n(rax.mrr_metric)(scores, labels) 

grad_fn = jax.jit(jax.grad(loss))

# train
for i in range(128):
    batch_size = 20
    for j in range(0, len(X_train), batch_size):
        batch = (X_train[j : j + batch_size], y_train[j : j + batch_size], jnp.ones(batch_size))
        w = w - grad_fn(w, batch) * 3e-4
        w = w.clip(0, 10)



# model = LinearRegression(fit_intercept=False, positive=True)
# model = SVR(kernel="linear")
# model = DecisionTreeRegressor()
# model.fit(X_train, y_train)

print("TRAIN")
for k in [1, 2, 5, 10]:
    print(f"NDCG@{k}: ", sklearn.metrics.ndcg_score([y_train], [model(w, X_train)], k=k))

print()
print("TEST")
for k in [1, 2, 5, 10]:
    print(f"NDCG@{k}: ", sklearn.metrics.ndcg_score([y_test], [model(w, X_test)], k=k))



weights = {id2feature[i]: float(v) for (i, v) in enumerate(w)}
print()
print("Weights:")
for k, v in sorted(weights.items(), key=lambda x: -x[1]):
    print(k, v)
linear_model = {"weights": weights}
with open("data/linear_model.json", "w") as f:
    json.dump(linear_model, f, indent=2)
