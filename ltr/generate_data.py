import openai
import os
import requests
import random
import json
import pandas as pd
from tqdm import tqdm

STRACT_ENDPOINT = "localhost:3000"
NUM_SHUFFLES = 5
NUM_QUERIES = 100
TOP_N_RESULTS = 20

openai.api_key = os.getenv("OPENAI_API_KEY")


def search(q):
    url = f"http://{STRACT_ENDPOINT}/beta/api/search"
    r = requests.post(
        url, json={"query": q, "offset": 0, "num_results": TOP_N_RESULTS})
    return r.json()['webpages']


def prompt(query, results):
    results = "\n".join(
        [json.dumps({k: v for (k, v) in r.items() if k != 'url'}) for r in results])
    return f"""You are a search engine evaluator. You will be presented with some results in json format from a search engine based on a query. Your task is to score the results from 0 to 1 for the query such that the best result gets a score of 1.0 and the worst gets a score of 0.0. A good search result answers the users query and comes from a trustworthy domain. You should not take the current ordering into account when you score the results. You should only output each score on the same line separated by ',' nothing else.

query: {query}

results:
{results}

scored results:"""


def score(query, results):
    p = prompt(query, results)
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{'role': 'user', 'content': p}],
        temperature=0,
    )
    return response.choices[0].message.content


queries = requests.get(
    "https://s3.trystract.com/public/queries_us_big.csv").text.splitlines()
random.shuffle(queries)
queries = queries[:NUM_QUERIES]

scores = {'query': [], 'url': [], 'score': []}

for query in tqdm(queries):
    search_result = search(query)

    res = []

    for r in search_result:
        if 'Normal' not in r['snippet']:
            continue

        snip = r['snippet']['Normal']
        res.append({"domain": r['domain'], "title": r['title'], "url": r['url'],
                    "snippet": snip['text']})

    if len(res) == 0:
        continue

    for _ in range(NUM_SHUFFLES):
        random.shuffle(res)
        try:
            s = score(query, res)
            for i, s in enumerate(s.split(',')):
                s = float(s)
                url = res[i]['url']

                scores['query'].append(query)
                scores['url'].append(url)
                scores['score'].append(s)
        except:
            pass

df = pd.DataFrame(scores)
df = df.groupby(['query', 'url'])
df = df.mean().sort_values(by='score', ascending=False).reset_index()
df['rank'] = df.groupby('query')['score'].rank(ascending=False)

print(df.head())
df.to_csv('ltr_scores.csv', index=False)
