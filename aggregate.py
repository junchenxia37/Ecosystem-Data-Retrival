from datetime import datetime
import json
import os
from typing import Iterable, Tuple
from collections import Counter
from dateutil import parser

# Monthly aggregation
def aggregate_monthly(data: Iterable[str], return_str=False)->list[Tuple[datetime, int]]:
    if not data:
        return None
    if type(data) is not list:
        data = list(data)
    if len(data) == 0:
        return None
    data = [parser.parse(date) for date in data if date is not None]
    c: Counter[datetime] = Counter(
        (date.replace(day=1, hour=0, minute=0, second=0, microsecond=0) for date in data if date is not None)
    )

    result = sorted(dict(c).items())
    if return_str:
        result = [(date.strftime('%Y-%m'), count) for date, count in result]
    return result

INPUT_DIR = './results'
INPUT_FLIES = [f'{n}.json' for n in range(0, 9)]+ ["a.json","n.json","o.json","p.json"]
OUTPUT_PATH = './aggregate.json'

"""
results = {
    org: {
        repo: {
            metric: [
                (datetime, int)
            ]
        }
    }
}
"""
if not os.path.exists(OUTPUT_PATH):
    results = {}
else:
    with open(OUTPUT_PATH, 'r') as f:
        results = json.load(f)

for file in INPUT_FLIES:
    full_path = os.path.join(INPUT_DIR, file)
    if not os.path.exists(full_path):
        continue
    with open(full_path, 'r') as f:
        data = json.load(f)
    if not data:
        continue
    for org, org_data in data.items():
        if org not in results:
            results[org] = {}
        for repo, repo_data in org_data.items():
            if type(repo_data) is not dict: # skip invalid data (e.g. 'owner' key)
                continue
            if repo not in results[org]:
                results[org][repo] = {}

            if not 'commits' in repo_data or repo_data['commits'] is None:
                results[org][repo]["commits"] = None
            else:
                results[org][repo]["commits"] = {'date': aggregate_monthly(map(lambda x: x['date'], repo_data['commits']), return_str=True)}

            if not 'forks' in repo_data or repo_data['forks'] is None:
                results[org][repo]["forks"] = None
            else:
                results[org][repo]["forks"] = {'created_at': aggregate_monthly(map(lambda x: x['created_at'], repo_data['forks']), return_str=True)}

            if not 'pulls' in repo_data or repo_data['pulls'] is None:
                results[org][repo]["pulls"] = None
            else:
                results[org][repo]["pulls"] = {
                    'created_at': aggregate_monthly(map(lambda x: x['created_at'], repo_data['pulls']), return_str=True), 
                    'closed_at': aggregate_monthly(map(lambda x: x['closed_at'], repo_data['pulls']), return_str=True)
                }

            # if not 'watches' in repo_data or repo_data['watches'] is None:
            #     results[org][repo]["watches"] = None
            # else:
            #     results[org][repo]["watches"] = {'created_at': aggregate_monthly(map(lambda x: x['created_at'], repo_data['watches']), return_str=True)}


            if not 'issues' in repo_data or repo_data['issues'] is None:
                results[org][repo]["issues"] = None
            else:
                results[org][repo]["issues"] = {
                    'created_at': aggregate_monthly(map(lambda x: x['created_at'], repo_data['issues']), return_str=True), 
                    'closed_at': aggregate_monthly(map(lambda x: x['closed_at'], repo_data['issues']), return_str=True)
                }
            
            if not 'releases' in repo_data or repo_data['releases'] is None:
                results[org][repo]["releases"] = None
            else:
                results[org][repo]["releases"] = {
                    'created_at': aggregate_monthly(map(lambda x: x['created_at'], repo_data['releases']), return_str=True), 
                    'published_at': aggregate_monthly(map(lambda x: x['published_at'], repo_data['releases']), return_str=True)
                }


        with open(OUTPUT_PATH, 'w') as f:
            json.dump(results, f)
        print(f'Finish {file}.')
