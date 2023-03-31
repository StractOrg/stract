import sklearn
import numpy as np
import json
import random
from sklearn.linear_model import LinearRegression

TRAIN_PERCENT = 0.8

with open("ltr_scores.json", "r") as f:
    scores = json.load(f)

random.shuffle(scores)

feature2id = {k: i for i, (k, _) in enumerate(
    scores[0]['ranking_signals'].items())}
id2feature = {v: k for k, v in feature2id.items()}

train = scores[:int(len(scores) * TRAIN_PERCENT)]
test = scores[int(len(scores) * TRAIN_PERCENT):]

X_train = []
y_train = []
for score in train:
    X_train.append([score['ranking_signals'][k] for k in feature2id])
    y_train.append(score['score'])

X_test = []
y_test = []
for score in test:
    X_test.append([score['ranking_signals'][k] for k in feature2id])
    y_test.append(score['score'])

X_train = np.array(X_train)
y_train = np.array(y_train)
X_test = np.array(X_test)
y_test = np.array(y_test)

model = LinearRegression(
    fit_intercept=False, positive=True)
model.fit(X_train, y_train)

weights = {id2feature[i]: v for (i, v) in enumerate(model.coef_)}
linear_model = {'weights': weights}
with open("data/linear_model.json", "w") as f:
    json.dump(linear_model, f, indent=2)

print("TRAIN")
print("score: ", model.score(X_train, y_train))
print("Mean squared error: ", sklearn.metrics.mean_squared_error(
    y_train, model.predict(X_train)))

print()
print("TEST")
print("score: ", model.score(X_test, y_test))
print("Mean squared error: ", sklearn.metrics.mean_squared_error(
    y_test, model.predict(X_test)))
