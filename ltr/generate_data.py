import openai
import os
import requests
import random
import json
import pandas as pd
from tqdm import tqdm

STRACT_ENDPOINT = "localhost:3000"
NUM_QUERIES = 100
TOP_N_RESULTS = 10

CENTRALITY_BOOST_OPTIC = 'Ranking(Signal("host_centrality"), 100000000);'

openai.api_key = os.getenv("OPENAI_API_KEY")


def search(q, top_n, optic=None):
    url = f"http://{STRACT_ENDPOINT}/beta/api/search"
    r = requests.post(
        url, json={"query": q, "page": 0, "num_results": top_n, "return_ranking_signals": True, "optic_program": optic})
    search_result = r.json()['webpages']
    res = []

    for r in search_result:
        if 'Normal' not in r['snippet']:
            continue

        snip = r['snippet']['Normal']
        res.append({"domain": r['domain'], "title": r['title'], "url": r['url'], "ranking_signals": r['ranking_signals'],
                    "snippet": snip['text'], "body": r["body"]})

    return res


def score_prompt(query: str, res):
    r = {k: v for (k, v) in res.items()
         if k in ['domain', 'title', 'snippet']}
    r['domain_score'] = '{:f}'.format(res['ranking_signals']['HostCentrality'])
    encoded = json.dumps(r)
    return f"""You are a search engine evaluator. Evaluate the following result based on the query. A good search result is relevant to the query and comes from a domain with a high domain_score. The domain_score is between 0.0 and 1.0. The score for the search result should be between 0.0 and 1.0.

query: england
result: {{domain: "theguardian.com", title: "Hungary fans fight with police inside Wembley at start of England match | World Cup 2022 qualifiers | The Guardian",
    snippet: "Tyrone Mings said that England's players were not fazed by Hungary's fans booing the knee. “We've faced criticism for taking the knee and we have collectively stood passionately together,” the <b>England</b> defender said.",
    domain_score: 0.028118234}}
score: 0.8

query: vim plugins
result: {{domain: "revelry.co", title: "My VIM Setup - Coding Creativity At Revelry",
    snippet: "Here are the plugins in my Vim setup that I really like: sessionman. im is a Vim session manager, meaning it will save your open buffers to a file for easy re-opening. It's useful in cases when you've been working on a ticket and you need to have one set of files open and then need to switch to another ticket that requires",
    domain_score: 0.0}}
score: 0.2

query: q learning algorithm
result: {{domain: "datascience.stackexchange.com", title: "Q-learning why do we subtract the Q(s, a) term during update? - Data Science Stack Exchange",
    snippet: "The q-learning algorithm is an off-policy algorithm , unlike SARSA . The Bellman equation describes q-learning as follows: "The q value for action $a$ taken in state $s$ at time $t$ becomes equal to: that same q-value plus small amount of: currently",
    domain_score: 0.017928008}}
score: 1.0

query: mutton curry variations
result: {{domain: "earthspice.in", title: "NADAN KOZHI CHICKEN CURRY - EarthSpice",
    snippet: "VARIATIONS AND SUBSTITUTES You can make this dish without coconut milk for a thinner gravy and different taste. Roasted chicken can also be used Chicken stock can be added along with the coconut milk Earthspice Organic Spice Provide Company in all India, Likes, Garam Masala, Tea Masala, Batthi Masala and Etc.",
    domain_score: 0.0}}
score: 0.0

query: {query}
result: {encoded}
score: """


def score(query, results):
    res = [None for _ in range(len(results))]
    for i, r in enumerate(results):
        p = score_prompt(query, r)
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{'role': 'user', 'content': p}],
            temperature=0,
        )
        res[i] = float(response.choices[0].message.content)

    return res


def query_prompt(res):
    r = {k: v for (k, v) in res.items() if k in ['domain', 'title', 'body']}
    r['body'] = ' '.join(r['body'].split(' ')[:200])[:1000]

    return f"""Generate a keyword based search query such that the following result would be considered a good result. The query should be at most 4 keywords long. You should not output anything besides the query.
result: {json.dumps(r)}
query: """


def good_query(res):
    p = query_prompt(res)
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{'role': 'user', 'content': p}],
        temperature=0,
    )
    return response.choices[0].message.content


queries = requests.get(
    "https://s3.trystract.com/public/queries_us_big.csv").text.splitlines()
random.shuffle(queries)

ranking_signals = {}
scores = {'query': [], 'url': [], 'score': []}

queries_taken = 0

with tqdm(total=NUM_QUERIES) as pbar:
    for query in queries:
        if queries_taken >= NUM_QUERIES:
            break

        res = search(query, 1, optic=CENTRALITY_BOOST_OPTIC)

        if len(res) < 1:
            continue

        query = good_query(res[0])

        res = search(query, TOP_N_RESULTS)
        if len(res) < TOP_N_RESULTS:
            continue

        try:
            for i, s in enumerate(score(query, res)):
                if s is None:
                    continue

                url = res[i]['url']
                signals = res[i]['ranking_signals']

                scores['query'].append(query)
                scores['url'].append(url)
                scores['score'].append(s)
                ranking_signals[url] = signals
        except Exception as e:
            print('Error', e)
            continue

        queries_taken += 1
        pbar.update(1)

df = pd.DataFrame(scores)
df = df.groupby(['query', 'url'])
df = df.mean().sort_values(by='score', ascending=False).reset_index()
df['rank'] = df.groupby('query')['score'].rank(ascending=False)
df['ranking_signals'] = df['url'].map(ranking_signals)

# convert to object
res = json.loads(df.to_json(orient='records'))


# save res in ltr_scores.json prettified
with open('ltr_scores.json', 'w') as f:
    json.dump(res, f, indent=2)
