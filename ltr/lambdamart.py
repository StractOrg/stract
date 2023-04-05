import lightgbm as lgb
import numpy as np
import json
from pprint import pprint

with open("ltr_scores.json") as f:
    scores = json.load(f)

feature2id = {k: i for i, (k, _) in enumerate(
    scores[0]['ranking_signals'].items())}
id2feature = {v: k for k, v in feature2id.items()}

queries = {}  # query -> {url: {score, features}
for item in scores:
    query = item["query"]
    if query not in queries:
        queries[query] = {}
    masked_features = {k: v for k, v in item["ranking_signals"].items() if k not in set([
        "CrossEncoder"])}
    queries[query][item["url"]] = {
        "score": item["score"], "features": item["ranking_signals"], "masked_features": masked_features}


# Create dataset
items = list(queries.items())
train_size = int(len(items) * 0.8)
X_train = []
y_train = []
q_train = []
for query, urls in items[:train_size]:
    # we have 2 instances per query
    # one normal, one masked
    q_train.append(query)
    q_train.append(query)
    for url, data in urls.items():
        X_train.append([data["features"][k] for k in feature2id])
        y_train.append(data["score"])
        X_train.append([data["masked_features"].get(k, 0.0)
                       for k in feature2id])
        y_train.append(data["score"])

X_test = []
y_test = []
q_test = []
for query, urls in items[train_size:]:
    # we have 2 instances per query
    # one normal, one masked
    q_test.append(query)
    q_test.append(query)
    for url, data in urls.items():
        X_test.append([data["features"][k] for k in feature2id])
        y_test.append(data["score"])
        X_test.append([data["masked_features"].get(k, None)
                      for k in feature2id])
        y_test.append(data["score"])

X_train = np.array(X_train)
y_train = np.array(y_train)

X_test = np.array(X_test)
y_test = np.array(y_test)

# Convert score from 0.0-1.0 to 0-3
y_train = (y_train * 3).astype(int)
y_test = (y_test * 3).astype(int)

# Create group
q_train = np.array([len(queries[qid]) for qid in q_train])
q_test = np.array([len(queries[qid]) for qid in q_test])

# Train model
model = lgb.LGBMRanker(
    objective="lambdarank",
    metric="ndcg",
    num_leaves=50,
    n_estimators=100,
    min_data_in_leaf=10,
    max_depth=None,
    early_stopping_rounds=5,
    learning_rate=0.005,
)
model.fit(X_train, y_train, group=q_train, feature_name=[k for k in feature2id], eval_set=[(X_test, y_test)], eval_group=[
          q_test], eval_at=[1, 3, 5, 10], eval_metric="ndcg")

# dump model
model.booster_.save_model("data/lambdamart.txt",
                          num_iteration=model.best_iteration_)
