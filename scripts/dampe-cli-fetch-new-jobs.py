'''
Created on Mar 15, 2016

@author: zimmer
'''
import copy
from core import db
import core.models as models
from core.DmpJob import DmpJob

if __name__ == "__main__":
    newJobInstances = []
    db.connect() # connect to DB
    for job in models.Job.objects:
        newJobs = [j for j in job.jobInstances if j.status == 'New']
        if len(newJobs):
            dJob = DmpJob(job.body)
            for j in newJobs:
                dInstance = copy.deepcopy(dJob)
                dInstance.setInstanceParameters(j.body)
                newJobInstances.append(dInstance)
    print 'found %i new job instances to deploy'%len(newJobInstances)
    ## okay - can do bulk submission or something like that
    