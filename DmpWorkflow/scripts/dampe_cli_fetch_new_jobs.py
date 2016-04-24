"""
Created on Mar 15, 2016

@author: zimmer
"""
from DmpWorkflow.config.defaults import ArgumentParser, os, sys, cfg, DAMPE_WORKFLOW_URL
import copy
from DmpWorkflow.core import db, models
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.utils.flask_helpers import parseJobXmlToDict


def main():
    newJobInstances = []
    db.connect()  # connect to DB
    for job in models.Job.objects:
        newJobs = [j for j in job.jobInstances if j.status == 'New']
        if len(newJobs):
            dJob = DmpJob(job)
            for j in newJobs:
                dInstance = copy.deepcopy(dJob)
                dInstance.setInstanceParameters(j)
                newJobInstances.append(dInstance)
                
    print 'found %i new job instances to deploy' % len(newJobInstances)

if __name__ == "__main__":
    main()
