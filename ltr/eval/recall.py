import argparse
import sqlite3
import json
from pprint import pprint

parser = argparse.ArgumentParser()

parser.add_argument('--db-path', default="data/eval.sqlite", help='Path to resulting eval database')
parser.add_argument('--limit', type=int, default=None, help='Limit number of queries')

args = parser.parse_args()

def print_signals_sorted(signals):
    for signal, value in sorted(signals.items(), key=lambda item: item[1], reverse=True):
        print(signal, ':', value)

def compute_recall(cursor):
    relevant = 0
    all = 0

    cursor.execute("SELECT qid FROM queries")
    for row in cursor.fetchall():
        qid = row[0]

        cursor.execute("SELECT url FROM golden WHERE qid = ?", (qid,))
        golden_urls = set(row[0] for row in cursor.fetchall())
        cursor.execute("SELECT url FROM results WHERE qid = ?", (qid,))
        result_urls = set(row[0] for row in cursor.fetchall())

        intersection = golden_urls.intersection(result_urls)
        relevant += len(intersection)
        all += len(golden_urls)

    return relevant / all

def missing(cursor):
    cursor.execute("SELECT qid, query FROM queries")
    total_diff = {}
    num_missing = 0

    for row in cursor.fetchall():
        qid = row[0]
        query = row[1]

        cursor.execute("SELECT url, signals FROM golden WHERE qid = ?", (qid,))
        golden = {row[0]: json.loads(row[1].replace("'", '"')) for row in cursor.fetchall()}

        cursor.execute("SELECT url, signals FROM results WHERE qid = ?", (qid,))
        result = {row[0]: json.loads(row[1].replace("'", '"')) for row in cursor.fetchall()}
        all_signals = set()



        avg_signal = {signal: sum([r.get(signal, 0)  for r in result.values()]) / len(result) for signal in all_signals}

        for url, signals in golden.items():
            if url in result:
                continue
            num_missing += 1

            print(f'missing {url} from "{query}"')
            diff_signal = {signal: value - avg_signal.get(signal, 0) for (signal, value) in signals.items()}
            print('diff signal')
            print_signals_sorted(diff_signal)
            print()
            print()

            for (signal, value) in diff_signal.items():
                total_diff[signal] = total_diff.get(signal, 0) + value

    print("average diff across all missing")
    avg_diff = {signal: value / num_missing for (signal, value) in total_diff.items()}
    print_signals_sorted(avg_diff)

    print()
    print()

with sqlite3.connect(args.db_path) as conn:
    cursor = conn.cursor()
    recall = compute_recall(cursor)
    missing(cursor)

    print('recall', recall)
