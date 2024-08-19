import lightgbm as lgb
import numpy as np
import json
from pprint import pprint
import sqlite3
from sklearn import metrics
import itertools

CATEGORICAL_FEATURES = ["is_homepage"]

param_grid = {
    "objective": ["lambdarank"],
    "verbosity": [-1],
    "metric": ["ndcg"],
    "ndcg_at": [[1, 2, 3, 5, 10]],
    # "learning_rate": [0.003],
    # "num_iterations": [100],
    # "max_depth": [-1, 2, 4, 8],
    "max_depth": [-1],
    # "num_leaves": [7, 15, 31],
    "num_leaves": [63],
    "lambda_l2": [2.5],
    "linear_tree": [False],
}


accepted_queries = set()
with open("data/queries_us.csv") as f:
    for query in f.readlines():
        if len(query.strip()) > 1:
            accepted_queries.add(query.strip().lower())

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
queries = {qid: {"query": query} for qid, query in res.fetchall()}
qids = list(queries.keys())

for qid in qids:
    if not queries[qid]["query"].lower() in accepted_queries:
        del queries[qid]

for qid in queries:
    res = cur.execute(
        """
            SELECT qid, url, annotation, webpage_json
            FROM search_results
            WHERE qid = ?
    """,
        (qid,),
    )
    urls = {
        url: {"label": label, "signals": json.loads(page)["rankingSignals"]}
        for _, url, label, page in res.fetchall()
    }
    urls = [
        (url, w["label"], w["signals"])
        for url, w in urls.items()
        if w["label"] is not None
    ]

    urls = sorted(urls, key=lambda x: int(x[1]), reverse=True)
    queries[qid]["urls"] = urls


feature2id = {}
id2feature = {}

for qid, data in queries.items():
    for url, score, signals in data["urls"]:
        for feature, value in signals.items():
            if feature not in feature2id:
                id = len(feature2id)
                feature2id[feature] = id
                id2feature[id] = feature

# convert to qid -> {url: {score, features}
new_queries = {}

for qid, data in queries.items():
    new_queries[qid] = {}
    for url, score, signals in data["urls"]:
        new_queries[qid][url] = {
            "score": score,
            "features": {feature2id[k]: v for k, v in signals.items()},
        }

queries = new_queries

# Create dataset
items = list(queries.items())
# shuffle items
np.random.shuffle(items)

train_size = int(len(items) * 0.8)
X_train = []
y_train = []
q_train = []
for query, urls in items[:train_size]:
    q_train.append(query)
    for url, data in urls.items():
        x = [data["features"].get(k, 0) for k in id2feature]
        X_train.append(x)
        y_train.append(data["score"])

X_test = []
y_test = []
q_test = []
for query, urls in items[train_size:]:
    q_test.append(query)
    for url, data in urls.items():
        X_test.append([data["features"].get(k, 0) for k in id2feature])
        y_test.append(data["score"])

X_train = np.array(X_train)
y_train = np.array(y_train)

X_test = np.array(X_test)
y_test = np.array(y_test)

# Create group
q_train = np.array([len(queries[qid]) for qid in q_train])
q_test = np.array([len(queries[qid]) for qid in q_test])

print("Train size:", len(X_train))
print("Test size:", len(X_test))


params = [
    dict(zip(param_grid.keys(), values))
    for values in itertools.product(*param_grid.values())
]


best_param = None
best_score = 0

for param in params:
    dataset = lgb.Dataset(
        X_train,
        y_train,
        group=q_train,
        feature_name=[k for k in feature2id],
        categorical_feature=[feature2id[k] for k in CATEGORICAL_FEATURES],
    )

    res = lgb.cv(
        train_set=dataset,
        params=param,
        nfold=5,
        return_cvbooster=True,
    )

    scores = []

    for metric, vals in res.items():
        if metric in ["cvbooster"]:
            continue

        if "stdv" in metric:
            continue
        scores.append(vals[0])

    score = np.mean(scores)

    if score > best_score:
        best_score = score
        best_param = param

print("Best param:")
pprint(best_param)


# Train model
dataset = lgb.Dataset(
    X_train,
    y_train,
    group=q_train,
    feature_name=[k for k in feature2id],
    categorical_feature=[feature2id[k] for k in CATEGORICAL_FEATURES],
)
booster = lgb.train(
    best_param,
    dataset,
)

# dump model
booster.save_model(
    "data/lambdamart.txt",
)
# print feature importance
print()
print("Feature importance:")
pprint(
    sorted(
        [
            (id2feature[i], v)
            for i, v in enumerate(booster.feature_importance())
            if v > 0
        ],
        key=lambda x: x[1],
        reverse=True,
    )
)
print()
print("Test set:")
for k in [1, 2, 3, 5, 10]:
    print(f"NDCG@{k}: {metrics.ndcg_score([y_test], [booster.predict(X_test)], k=k)}")

# verify that the saved model outputs the same scores
# for the same input
saved_model = lgb.Booster(model_file="data/lambdamart.txt")

for i in range(len(X_test)):
    assert booster.predict(X_test[i : i + 1]) == saved_model.predict(X_test[i : i + 1])


# print an example
# print()
# print("Example:")
# t = X_test[0]
# print("Features:")
# pprint({id2feature[i]: v for i, v in enumerate(t)})
# print("Score:", booster.predict(t.reshape(1, -1))[0])
