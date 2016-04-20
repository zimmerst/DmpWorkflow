'''
Created on Mar 15, 2016

@author: zimmer
'''
from utils.scriptDefaults import cfg
import copy
from core import db
import core.models as models
from core.DmpJob import DmpJob
from utils.flask_helpers import parseJobXmlToDict

import random

def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return random.randint(range_start, range_end)

if __name__ == "__main__":
    
    newJobInstances = []
    db.connect() # connect to DB
    for job in models.Job.objects:
        newJobs = [j for j in job.jobInstances if j.status == 'New']
        if len(newJobs):
            dJob = DmpJob(job)
            for j in newJobs:
                dInstance = copy.deepcopy(dJob)
                dInstance.setInstanceParameters(j)
                newJobInstances.append(dInstance)
                
    print 'found %i new job instances to deploy'%len(newJobInstances)
    ## okay - can do bulk submission or something like that
    