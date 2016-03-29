'''
Created on Mar 15, 2016

@author: zimmer
'''
import copy
from core import db
import core.models as models
from core.DmpJob import DmpJob

import random

hostnames = ['fell','bullet','iris']


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return random.randint(range_start, range_end)

if __name__ == "__main__":
    
    newJobInstances = []
    Db = db.connect() # connect to DB
    for job in models.Job.objects:
        newJobs = [j for j in job.jobInstances if j.status == 'New']
        if len(newJobs):
            dJob = DmpJob(job)
            for j in newJobs:
                dInstance = copy.deepcopy(dJob)
                dInstance.instanceId = j.uniqueId
                dInstance.setInstanceParameters(j.body)
                newJobInstances.append(dInstance)
                
    print 'found %i new job instances to deploy'%len(newJobInstances)
    #for inst in newJobInstances:
    #    Db.update({'jobInstances.uniqueId': '%s'%inst.uniqueId}, {'jobInstances.batchId': random_with_N_digits(6)})
        
    ## okay - can do bulk submission or something like that
    