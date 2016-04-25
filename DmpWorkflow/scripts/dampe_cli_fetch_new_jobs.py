"""
Created on Mar 15, 2016

@author: zimmer
"""
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL
import requests


def main():
    res = requests.get("%s/newjobs/" % DAMPE_WORKFLOW_URL)
    res.raise_for_status()
    jobs = res.json().get("jobs")
    print 'found %i new job instances to deploy' % len(jobs)
    for job in jobs:
        j = DmpJob.fromJSON(job)
        j.write_script()
        j.submit()

if __name__ == "__main__":
    main()
