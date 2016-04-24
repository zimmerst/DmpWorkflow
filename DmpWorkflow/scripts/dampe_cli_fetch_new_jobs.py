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
    for job in jobs:
        j = DmpJob.fromJSON(job)
        j.write_script()
        j.submit()
    #
    # for job in models.Job.objects:
    #     newJobs = [j for j in job.jobInstances if j.status == 'New']
    #     if len(newJobs):
    #         dJob = DmpJob(job)
    #         for j in newJobs:
    #             dInstance = copy.deepcopy(dJob)
    #             dInstance.setInstanceParameters(j)
    #             newJobInstances.append(dInstance)
                
    print 'found %i new job instances to deploy' % len(jobs)

if __name__ == "__main__":
    main()
