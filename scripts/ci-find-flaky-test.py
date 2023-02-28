#!/usr/bin/env python3
import sys
import http.client
import json
import re
from urllib.request import urlopen
from collections import Counter
from optparse import OptionParser
from datetime import datetime

def fail(resp):
    status = resp.status
    result = resp.read()
    print(f"{status} : {result}")
    #sys.exit(1)

def report(client, flaky_tcs, MDHead):
    now = datetime.now()
    MDtext = MDHead + f"""
Generated at {now}
| suite | case  | total failed times  |
|-------|-------|---------------------|
"""
    flaky_tcs = Counter(flaky_tcs)
    for (suite, tc), total in sorted(flaky_tcs.items(), key=lambda i: i[1], reverse=True):
        print(f'{suite} {tc} failed -> {total} times')
        MDtext = MDtext + f"| {suite} | {tc} | {total} |\n"
    # @todo configable gist
    client.request('POST', '/gists/797724b5f50aa9304708f8e04c24c02d',
                   headers = {'Authorization' : f'Bearer {gh_token}',
                              'X-GitHub-Api-Version': '2022-11-28',
                              'User-Agent': 'python3'
                              },
                   body = json.dumps({'description' : f'Lastest Updates for {branch}',
                                      'files' : {"EMQX_master_flaky_tcs.md":
                                                 {"content" : MDtext}
                                                 }
                                      }
                                     )
                   )
    resp = client.getresponse()


def github_api_get(client, gh_token, url, redirect = False):
    resp = client.request('GET', url,
                   headers = {'Authorization' : f'Bearer {gh_token}',
                              'X-GitHub-Api-Version': '2022-11-28',
                              'User-Agent': 'python3'
                              }
                   )
    resp = C1.getresponse()
    if resp.status == 410: # Gone.
        resp.read()
        return
    if resp.status == 200:
        return json.loads(resp.read())
    elif resp.status == 302 and redirect:
        resp.read()
        redirect_url = resp.getheader('location')
        f = urlopen(redirect_url)
        return f.read()
    else:
        fail(resp)

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-r", "--repo", dest="repo",
                      help="github repo", default="emqx/emqx")
    parser.add_option("-t", "--token", dest="gh_token",
                      help="github API token")
    parser.add_option("-b", "--branch", dest="branch", default = 'master',
                      help="Branch that workflow runs on")
    parser.add_option("-l", "--limit", dest="limit", default = 100,
                      type = 'int',
                      help="Limits the number of failed github jobs")
    parser.add_option("-d", "--days", dest="days", default = 7,
                      type = 'int',
                      help="Limits the number of days we look back.")
    (options, args) = parser.parse_args()

    repo = options.repo
    gh_token = options.gh_token
    branch = options.branch
    limit = options.limit
    days_limit = options.days

    flaky_tcs = list()
    page = 1
    num_failed_jobs = 0

    C1 = http.client.HTTPSConnection('api.github.com')

    while num_failed_jobs < limit:
      body = github_api_get(C1, gh_token, f'/repos/{repo}/actions/workflows/run_test_cases.yaml/runs?per_page={limit}&page={page}&branch={branch}')
      page = page + 1
      for run in body['workflow_runs']:
          time_diff = datetime.now() - datetime.strptime(run['created_at'], "%Y-%m-%dT%H:%M:%SZ")
          if time_diff.days > days_limit:
              print("Days limit reached")
              break;
          if run['conclusion'] != 'success':
              run_id = run['id']
              ## get jobs
              body = github_api_get(C1, gh_token, f'/repos/{repo}/actions/runs/{run_id}/jobs?per_page=100')
              for j in body['jobs']:
                  conclusion = j['conclusion']
                  if conclusion == 'failure':
                      job_id = j['id']
                      print("failed job: %s: %s" % (j['id'], j['conclusion']))
                      num_failed_jobs = num_failed_jobs+1
                      if (num_failed_jobs >= limit):
                          break
                      job_log = github_api_get(C1, gh_token, f'/repos/{repo}/actions/jobs/{job_id}/logs', redirect = True)
                      if job_log:
                        for line in job_log.decode('utf-8').split('\n'):
                            m = re.search('%%%(.*) ==> (.*): FAILED', line)
                            if m:
                                suite_name = m.group(1)
                                tc_name = m.group(2)
                                print(f'{suite_name} : {tc_name}')
                                flaky_tcs.append((suite_name, tc_name))
    report(C1, flaky_tcs, f"Branch: {branch}")
