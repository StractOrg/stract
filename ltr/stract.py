import requests

API = "https://stract.com/beta/api/search"
# API = "http://localhost:3000/beta/api/search"
NUM_RESULTS_PER_QUERY = 20


def simplify_snippet(snippet):
    if "text" not in snippet:
        return ""

    return "".join([f["text"] for f in snippet["text"]["fragments"]])


def search(query, num_results=NUM_RESULTS_PER_QUERY, page=0, signal_coefficients=None):
    payload = {
        "query": query,
        "numResults": num_results,
        "page": page,
        "returnRankingSignals": True,
        "signalCoefficients": signal_coefficients,
    }

    return [
        {
            "url": w["url"],
            "title": w["title"],
            "rankingSignals": {s: v["value"] for (s, v) in w["rankingSignals"].items()},
            "snippet": simplify_snippet(w["snippet"]),
        }
        for w in requests.post(API, json=payload).json()["webpages"][
            :num_results
        ]
    ]
