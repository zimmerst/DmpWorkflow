'''
Created on Mar 15, 2016

@author: zimmer
'''
import copy
from core import db
import core.models as models
from core.DmpJob import DmpJob

import random, time

hostnames = ['fell','bullet','iris','hequ']


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return random.randint(range_start, range_end)

if __name__ == "__main__":
    counter=0
    maxCount = 100
    newJobInstances = []
    Db = db.connect() # connect to DB
    for job in models.Job.objects:
        if counter < maxCount:
            newJobs = [j for j in job.jobInstances if j.status == 'New']
            if len(newJobs):
                if len(newJobs)>maxCount:
                    newJobs = newJobs[:maxCount]
                dJob = DmpJob(job)
                for j in newJobs:
                    dInstance = copy.deepcopy(dJob)
                    dInstance.instanceId = j._id
                    dInstance.jobId = job.id
                    dInstance.setInstanceParameters(j.body)
                    j.status = random.choice(["Submitted","New","New","New"])
                    if j.status == "Submitted":
                        j.last_update = time.ctime()
                        j.batchId = random_with_N_digits(8)
                        newJobInstances.append(dInstance)
                        counter+=1
            job.save()
    print 'deployed %i new job instances.'%len(newJobInstances)
    #for inst in newJobInstances:
    #    Db.update({'jobInstances.uniqueId': '%s'%inst.uniqueId}, {'jobInstances.batchId': random_with_N_digits(6)})
        
    ## okay - can do bulk submission or something like that
    print 'updating submitted jobs'
    for job in models.Job.objects:
        newJobs = [j for j in job.jobInstances if j.status == 'Submitted']
        if len(newJobs):
            dJob = DmpJob(job)
            for j in newJobs:
                dInstance = copy.deepcopy(dJob)
                dInstance.instanceId = j._id
                dInstance.jobId = job.id
                dInstance.setInstanceParameters(j.body)
                j.batchId = random_with_N_digits(8)
                j.last_update = time.ctime()
                j.hostname = "%s-%i"%(random.choice(hostnames),random_with_N_digits(2))
                j.status = random.choice(["Submitted","Running","Failed","Done","Submitted","Submitted","Running"])
                if j.status == "Submitted": j.minor_status = "JobPending"
                elif j.status == "Running": j.minor_status = random.choice(["RunningExecutionWrapper","CopyingInputFiles","AssemblingOutputFiles"])
                elif j.status == "Done": j.minor_status = "ExecutionCompleted"
                else: j.minor_status = "FailedWithCode:%i"%random.choice([1,2,3,4,5,6])
        job.save()
    