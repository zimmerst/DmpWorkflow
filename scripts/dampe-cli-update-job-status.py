'''
Created on Mar 15, 2016

@author: zimmer
'''
import copy
from core import db
import core.models as models
from core.DmpJob import DmpJob
# each job has an immutable document identifier.
if __name__ == "__main__":
    db.connect()
    
    
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
    