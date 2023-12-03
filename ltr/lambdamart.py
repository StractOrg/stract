import lightgbm as lgb
import numpy as np
import json
from pprint import pprint
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
        X_train.append([data["features"].get(k, 0) for k in id2feature])
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

# Train model
n_estimators = 50
model = lgb.LGBMRanker(
    objective="lambdarank",
    metric="ndcg",
    importance_type="gain",
    num_leaves=50,
    n_estimators=n_estimators,
    max_depth=10,
    learning_rate=0.01,
    label_gain=[i for i in range(max(y_train.max(), y_test.max()) + 1)],
)
model.fit(
    X_train,
    y_train,
    group=q_train,
    feature_name=[k for k in feature2id],
    eval_set=[(X_test, y_test)],
    eval_group=[q_test],
    eval_at=[1, 2, 3, 5, 10],
    eval_metric="ndcg",
)

# dump model
model.booster_.save_model(
    "data/lambdamart.txt",
)

# print feature importance
pprint(
    sorted(
        [(id2feature[i], v) for i, v in enumerate(model.feature_importances_) if v > 0],
        key=lambda x: x[1],
        reverse=True,
    )
)

# verify that the saved model outputs the same scores
# for the same input
saved_model = lgb.Booster(model_file="data/lambdamart.txt")

for i in range(len(X_test)):
    assert model.predict(X_test[i : i + 1]) == saved_model.predict(X_test[i : i + 1])

# print an example
# print("Example:")
# t = X_test[0]
# print("Features:")
# pprint({id2feature[i]: v for i, v in enumerate(t)})
# print("Score:", model.predict(t.reshape(1, -1))[0])
