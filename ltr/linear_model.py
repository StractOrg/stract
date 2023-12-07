import sklearn
import numpy as np
import json
import random
from sklearn.linear_model import LinearRegression
from pprint import pprint

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
    for i, (url, score, _, signals) in enumerate(data["urls"]):
        signals = {feature2id[k]: v for k, v in signals.items()}

        signals = [signals[k] for _, k in sorted_features]

        scores.append({"ranking_signals": signals, "score": 20 / (i + 1)})

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

X_train = np.array(X_train)
y_train = np.array(y_train)
X_test = np.array(X_test)
y_test = np.array(y_test)

model = LinearRegression(fit_intercept=False, positive=True)
# model = SVR(kernel="linear")
# model = DecisionTreeRegressor()
model.fit(X_train, y_train)

print("TRAIN")
print("score: ", model.score(X_train, y_train))
print(
    "Mean squared error: ",
    sklearn.metrics.mean_squared_error(y_train, model.predict(X_train)),
)

print()
print("TEST")
print("score: ", model.score(X_test, y_test))
print(
    "Mean squared error: ",
    sklearn.metrics.mean_squared_error(y_test, model.predict(X_test)),
)

weights = {id2feature[i]: v for (i, v) in enumerate(model.coef_)}
linear_model = {"weights": weights}
pprint(linear_model)
with open("data/linear_model.json", "w") as f:
    json.dump(linear_model, f, indent=2)
