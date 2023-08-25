import requests
import random
import json
import pandas as pd
from tqdm import tqdm
import torch
import urllib.request
from rwkv.model import RWKV
from rwkv.utils import PIPELINE, PIPELINE_ARGS
import os


STRACT_ENDPOINT = "localhost:3000"
NUM_QUERIES = 100
TOP_N_RESULTS = 10

CENTRALITY_BOOST_OPTIC = 'Ranking(Signal("host_centrality"), 100000000);'

rwkv_model = "https://huggingface.co/BlinkDL/rwkv-4-raven/resolve/main/RWKV-4-Raven-7B-v9-Eng99%25-Other1%25-20230412-ctx8192.pth"
# download model
if not os.path.exists("data/rwkv_model.pth"):
    print("Downloading model")
    urllib.request.urlretrieve(rwkv_model, "data/rwkv_model.pth")

if not os.path.exists("data/rwkv_tokenizer.json"):
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/BlinkDL/ChatRWKV/main/v2/20B_tokenizer.json",
        "data/rwkv_tokenizer.json",
    )

torch.set_num_threads(12)
model = RWKV(
    model="data/rwkv_model.pth",
    strategy="cpu fp32",
)
pipeline = PIPELINE(model, "data/rwkv_tokenizer.json")
print("model ready")


def search(q: str, top_n, optic=None):
    if q == "":
        return []

    url = f"http://{STRACT_ENDPOINT}/beta/api/search"
    r = requests.post(
        url,
        json={
            "query": q,
            "page": 0,
            "numResults": top_n,
            "returnRankingSignals": True,
            "optic": optic,
        },
    )
    search_result = r.json()["webpages"]
    res = []

    for r in search_result:
        if "Normal" not in r["snippet"]:
            continue

        snip = r["snippet"]["Normal"]["text"]
        snip = snip.replace("<b>", "").replace("</b>", "")
        res.append(
            {
                "domain": r["domain"],
                "title": r["title"],
                "url": r["url"],
                "rankingSignals": r["rankingSignals"],
                "snippet": snip,
                "body": r["body"],
            }
        )

    return res


def score_prompt(query: str, res):
    r = {k: v for (k, v) in res.items() if k in ["domain", "title", "snippet"]}
    # r['domain_score'] = '{:f}'.format(
    #     res['rankingSignals']['host_centrality'])
    encoded = json.dumps(r)

    inst = "You are a search engine evaluator. Evaluate the following result based on the query. A good search result is relevant to the query and comes from a trustworthy domain. The score for the search result should be between 0.0 and 1.0."
    return f"""Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.

# Instruction:
{inst}

query: england
result: {{domain: "theguardian.com", title: "Hungary fans fight with police inside Wembley at start of England match | World Cup 2022 qualifiers | The Guardian",
    snippet: "Tyrone Mings said that England's players were not fazed by Hungary's fans booing the knee. “We've faced criticism for taking the knee and we have collectively stood passionately together,” the <b>England</b> defender said."}}

# Response:
score: 0.8<|endoftext|>

# Instruction:
{inst}

query: vim plugins
result: {{domain: "revelry.co", title: "My VIM Setup - Coding Creativity At Revelry",
    snippet: "Here are the plugins in my Vim setup that I really like: sessionman. im is a Vim session manager, meaning it will save your open buffers to a file for easy re-opening. It's useful in cases when you've been working on a ticket and you need to have one set of files open and then need to switch to another ticket that requires"}}

# Response:
score: 0.2<|endoftext|>

# Instruction:
{inst}

query: q learning algorithm
result: {{domain: "datascience.stackexchange.com", title: "Q-learning why do we subtract the Q(s, a) term during update? - Data Science Stack Exchange",
    snippet: "The q-learning algorithm is an off-policy algorithm , unlike SARSA . The Bellman equation describes q-learning as follows: "The q value for action $a$ taken in state $s$ at time $t$ becomes equal to: that same q-value plus small amount of: currently"}}

# Response:
score: 1.0<|endoftext|>

# Instruction:
{inst}
query: mutton curry variations
result: {{domain: "earthspice.in", title: "NADAN KOZHI CHICKEN CURRY - EarthSpice",
    snippet: "VARIATIONS AND SUBSTITUTES You can make this dish without coconut milk for a thinner gravy and different taste. Roasted chicken can also be used Chicken stock can be added along with the coconut milk Earthspice Organic Spice Provide Company in all India, Likes, Garam Masala, Tea Masala, Batthi Masala and Etc."}}

# Response:
score: 0.0<|endoftext|>

# Instruction:
{inst}

query: {query}
result: {encoded}

# Response:
score:"""


def run_model(prompt: str) -> str:
    print("running model")
    print(prompt)

    all_tokens = []
    occurrence = {}
    state = None
    max_token_count = 10
    args = PIPELINE_ARGS(
        temperature=0.2,
        top_p=0.0,
        alpha_frequency=0.4,
        alpha_presence=0.4,
        token_ban=[],  # ban the generation of some tokens
        token_stop=[0],
    )  # stop generation whenever you see any token here

    for i in range(max_token_count):
        out, state = model.forward(
            pipeline.encode(prompt) if i == 0 else [token], state
        )
        for n in occurrence:
            out[n] -= args.alpha_presence + occurrence[n] * args.alpha_frequency

        token = pipeline.sample_logits(
            out, temperature=args.temperature, top_p=args.top_p
        )
        if token in args.token_stop:
            break
        all_tokens += [token]
        if token not in occurrence:
            occurrence[token] = 1
        else:
            occurrence[token] += 1

    output = pipeline.decode(all_tokens).split("\n")[0].strip()
    print("START OF OUTPUT")
    print(output)
    print("END OF OUTPUT")
    return output


def score(query, results):
    res = [None for _ in range(len(results))]
    for i, r in enumerate(results):
        p = score_prompt(query, r)
        res[i] = float(run_model(p))

    return res


def query_prompt(res):
    r = {k: v for (k, v) in res.items() if k in ["domain", "title", "body"]}
    r["body"] = " ".join(r["body"].split(" ")[:200])[:1000]

    inst = "Generate a keyword based search query such that the following result would be considered a good result. The query should be at most 4 keywords long. You should only output a single query and nothing else"

    return f"""Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.

# Instruction:
{inst}

domain: wikipedia.org
body: On this Wikipedia the language links are at the top of the page across from the article title. Go to top . Map of Colts Neck Township in Monmouth County. Inset: Location of Monmouth County highlighted in the State of New Jersey. Census Bureau map of Colts Neck Township, New Jersey The township has been ranked as one of the state's highest-income communities. Based on data from the American Community Survey for 2013–2017, Colts Neck residents had a median household income of $167,480, ranked fifth in the state among municipalities with more than 10,000 residents, more than double the statewide median of $76,475.[24] [25] The township has a Farmland Preservation Committee which to date has preserved nearly 1,000 acres (400 ha) of land, providing one way in which Colts Neck has been able to prevent large-scale development. The township has strict zoning regulations, and because there is no public water or sewage service, most homes must be built on lots covering a minimum of 2, 5 and 10 a

# Response:
query:"colts neck wikipedia"<|endoftext|>

# Instruction:
{inst}

domain: slideshare.net
body: 1. - C herkasy region is not only the geographical cent- er of Ukraine; it is its spiritual center: rich nature, generous fertile black soils, unique historical and cultural heritage. In Cherkasy region, there were formed the first Cossack republic and Ukrainian statehood. Its first hetman capital was here, in Chyhyryn, during the times of our great countryman Bohdan Khmelnytsky. Many European countries’ ambassadors were given a reception here. The Ukrainian Cossack phenomenon be- gan and the Cossack rebellions and XVII century national liberation revolution started on these lands. Cherkasy region is a place of a great deed of the na- tion during the Second World War. Hundreds of monu- ments and memorial steles are on the paces of Korsun- Shevchenkivsky battle, Uman-Batoshanska military operation, and the battle for the Dnipro River. DuringtheKyivRustimesitsSouthernborderswerepro- tected by city-fortresses of poroska, posulska, and posupi- yska defense lines: Korsun, Zheln, Rymiv, Piso

# Response:
query:"cherkasy ukraine"<|endoftext|>

# Instruction:
{inst}

domain: {r['domain']}
body: {r['body']}

# Response:
query:"""


def good_query(res):
    p = query_prompt(res)
    return run_model(p).replace('"', "").lower().split(",")[0]


queries = requests.get(
    "http://s3.trystract.com/public/queries_us_big.csv"
).text.splitlines()
random.shuffle(queries)

ranking_signals = {}
scores = {"query": [], "url": [], "score": []}

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

                url = res[i]["url"]
                signals = res[i]["rankingSignals"]

                scores["query"].append(query)
                scores["url"].append(url)
                scores["score"].append(s)
                ranking_signals[url] = signals
        except Exception as e:
            print("Error", e)
            continue

        queries_taken += 1
        pbar.update(1)

df = pd.DataFrame(scores)
df = df.groupby(["query", "url"])
df = df.mean().sort_values(by="score", ascending=False).reset_index()
df["rank"] = df.groupby("query")["score"].rank(ascending=False)
df["rankingSignals"] = df["url"].map(ranking_signals)

# convert to object
res = json.loads(df.to_json(orient="records"))


# save res in ltr_scores.json prettified
with open("ltr_scores.json", "w") as f:
    json.dump(res, f, indent=2)
