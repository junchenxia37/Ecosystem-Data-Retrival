import requests
import base64
import time
import json
import argparse


readme_variants = ["README.md", "README", "README.txt", "readme.md", "readme"]

def get_readme(repo):
    for readme_filename in readme_variants:
        readme_url = repo['url'] + f"/contents/{readme_filename}"
        response = requests.get(readme_url, headers=headers)
        # time.sleep(0.1)
        if response.status_code == 200:
            readme_data = response.json()
            return base64.b64decode(readme_data['content']).decode()
    return None


def get_repositories(username):
    i = 0
    url = f"https://api.github.com/users/{username}/repos"
    repos = []
    while url:
        response = requests.get(url, headers=headers)
        # time.sleep(0.1)
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch repos: {response.content}")
        data = response.json()
        for repo in data:
          repo_name = repo['name']
          repo_description = repo['description']
          readme_content = get_readme(repo)
          repos.append({'name': repo['name'], 'desc': repo_description, 'readme': readme_content})
          print(f'{i:<4}{repo_name} done')
          i += 1
        url = response.links.get('next', {}).get('url', None)
    return repos

parser = argparse.ArgumentParser(
                    prog='crawler',
                    description='Crawl Github Repositories')

parser.add_argument('org', type=str, help='The Target Org You Want To Crawl')
parser.add_argument('-t', '--token', type=str, help='Your Github Token')
parser.add_argument('-o', '--output', type=str, help='Output File Name')

args = parser.parse_args()
TOKEN = args.token
org = args.org
output = args.output
headers = {
    'Authorization': f'token {TOKEN}'
}

repos = get_repositories(org)

with open(output, 'w') as f:
   json.dump(repos, f)