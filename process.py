from typing import Any, Callable, Optional
import requests
import tomllib
import os
import json
from typing import Dict

TOKEN = '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'


def log_error(e):
    with open('error.log', 'a') as f:
        f.write(str(e))
        f.write('\n')

class Metric:
    def __init__(self, metric_name: str, url_resource: str, extractor: Callable[[dict], Any], params: Optional[Dict[str, str]]=None) -> None:
        self.metric_name = metric_name
        self.url_resource = url_resource
        self.extractor = extractor
        self.params = params

date_fetchers = {
    Metric('commits', 'commits', lambda commit: {'date': commit['commit']['author']['date']}, {'since': 'YYYY-MM-DDTHH:MM:SSZ'}),
    Metric('forks', 'forks', lambda fork: {'created_at': fork['created_at']}),
    Metric('pulls', 'pulls', lambda pull: {'created_at': pull['created_at'], 'closed_at': pull['closed_at']}, {'state': 'all'}),
    # Metric('watches', 'events', lambda event: None if event['type'] != 'WatchEvent' else {'created_at': event['created_at']}),
    Metric('issues', 'issues', lambda issue: {'created_at': issue['created_at'], 'closed_at': issue['closed_at']}, {'state': 'all'}),
    Metric('releases', 'releases', lambda release: {'created_at': release['created_at'], 'published_at': release['published_at']}),
}

def fetch_all_pages(base_url, headers, params=None):
    """
    Fetch all pages from a paginated GitHub API endpoint.

    Args:
    base_url (str): The URL of the GitHub API endpoint.
    headers (dict): The headers to include in the requests, typically for authentication and content type.

    Returns:
    list: A list containing all items fetched from all pages.
    """
    items = []
    url = base_url

    while url:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            items.extend(data)  # Assuming the response is a list of items
            url = None  # Reset the URL to end the loop if no Link header is found

            # Process the Link header to find the next page URL
            if 'Link' in response.headers:
                links = response.headers['Link']
                next_url = None
                # Parse the Link header to find the URL for the next page
                for link in links.split(', '):
                    part, rel = link.split('; ')
                    if 'rel="next"' in rel:
                        next_url = part.strip('<>')
                        break
                url = next_url  # Set the URL to the next page's URL
        else:
            print("Failed to fetch data:", response.status_code)
            break  # Stop the loop on failure

    return items

def get_metrics(owner, repo, metric: Metric, access_token=None):
    # URL for the GitHub API to get commits
    url = f"https://api.github.com/repos/{owner}/{repo}/{metric.url_resource}"
    
    # Header to use the personal access token for authentication
    headers = {}
    if access_token:
        headers['Authorization'] = f'token {access_token}'

    try:
        collection = fetch_all_pages(url, headers, metric.params)
        if collection is None:
            raise Exception('collection is none')
        return filter(lambda x: x is not None, map(metric.extractor, collection))
    except requests.exceptions.RequestException as e:
        log_error(f"Error: {e}")
        return None

def get_repo_metrics(owner, repo):
    results = {}
    for metric in date_fetchers:
        try:
            metrics_result = get_metrics(owner, repo, metric, TOKEN)
            results[metric.metric_name] = list(metrics_result) if metrics_result is not None else None
        except Exception as e:
            log_error(f"Error: {e}")
            return None
    return results

def process_ecosystems(dir, checkpoint_file_path):
    # check if the org is already processed
    if os.path.exists(checkpoint_file_path):
        with open(checkpoint_file_path, 'r') as f:
            results = json.load(f)
    else:
        """
        results = {
            org: {
                repo: {
                    metric: [
                        {...}
                    ]
                }, 
                owner: str
            }
        }
        """
        results = {} # {org: {repo: metric: {...}, owner: str}}
    for root, dirs, files in os.walk(dir):
        for file in files:
            if not file.endswith(".toml"):
                continue
            full_path = os.path.join(root, file)
            with open(full_path, 'rb') as f:
                config = tomllib.load(f)
            if not 'repo' in config:
                continue
            org = file.removesuffix('.toml')
            if org in results:
                continue
            last_owner = None
            results[org] = {}
            for repo_link in map(lambda url: url['url'], config['repo']):
                try:
                    url_pieces = repo_link.split('/')
                    owner = url_pieces[-2]
                    repo = url_pieces[-1]
                    if repo in results[org]:
                        continue
                    if last_owner is None:
                        last_owner = owner
                    else:
                        if last_owner != owner:
                            log_error(f'Inconsistent owner name: {last_owner} vs {owner}')
                    metric = get_repo_metrics(owner, repo)
                    if metric is not None:
                        results[org][repo] = metric
                except Exception as e:
                    log_error(f'Error happened when handling {repo_link}: {e}')
                    continue
                with open(checkpoint_file_path, 'w') as f:
                    json.dump(results, f)
            results[org]['owner'] = last_owner
          
    return results

INPUT_BASE = './ecosystems'

subfolders = [chr(i) for i in range(ord('n'), ord('z') + 1)]

for subfolder in subfolders:
    print(f'Starting {subfolder}...')
    r = process_ecosystems(f'{INPUT_BASE}/{subfolder}', f'results/{subfolder}.json')
    print(f'Finish {subfolder}.')