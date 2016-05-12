"""
Created on Mar 15, 2016

@author: zimmer
"""
from DmpWorkflow.config.defaults import os, sys, DAMPE_WORKFLOW_URL
from DmpWorkflow.core.system import db
from DmpWorkflow.core.models import Job
from DmpWorkflow.core.DmpJob import DmpJob
from random import randint, choice
from copy import deepcopy
from time import ctime

hostnames = ['fell', 'bullet', 'iris', 'hequ']


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return randint(range_start, range_end)


if __name__ == "__main__":
    counter = 0
    maxCount = 100
    newJobInstances = []
    db.connect()  # connect to DB
    for job in Job.objects:
        os.environ["DWF_JOBNAME"] = job.title
        if counter < maxCount:
            newJobs = [j for j in job.jobInstances if j.status == 'New']
            if len(newJobs):
                if len(newJobs) > maxCount:
                    newJobs = newJobs[:maxCount]
                dJob = DmpJob(job)
                for j in newJobs:
                    dInstance = deepcopy(dJob)
                    dInstance.instanceId = j.instanceId
                    dInstance.jobId = job.id
                    dInstance.setInstanceParameters(j)
                    dInstance.write_script()
                    tstatus = choice(["Submitted", "New", "New", "New"])
                    if tstatus == "Submitted":
                        # print 'found status other than new'
                        j.batchId = random_with_N_digits(8)
                        j.setStatus(tstatus)
                        newJobInstances.append(dInstance)
                        counter += 1
            job.update()
    print 'deployed %i new job instances.' % len(newJobInstances)
    # for inst in newJobInstances:
    #    Db.update({'jobInstances.uniqueId': '%s'%inst.uniqueId}, {'jobInstances.batchId': random_with_N_digits(6)})
    # sys.exit()
    ## okay - can do bulk submission or something like that
    sys.exit()
    print 'updating submitted jobs'
    for job in Job.objects:
        newJobs = [j for j in job.jobInstances if j.status == 'Submitted']
        if len(newJobs):
            dJob = DmpJob(job)
            for j in newJobs:
                dInstance = deepcopy(dJob)
                dInstance.instanceId = j.instanceId
                dInstance.jobId = job.id
                dInstance.setInstanceParameters(j)
                j.batchId = random_with_N_digits(8)
                j.last_update = ctime()
                j.hostname = "%s-%i" % (choice(hostnames), random_with_N_digits(2))
                tstatus = choice(["Submitted", "Running", "Failed", "Done", "Submitted", "Submitted", "Running"])
                if tstatus == "Submitted":
                    j.minor_status = "JobPending"
                elif tstatus == "Running":
                    j.minor_status = choice(
                        ["RunningExecutionWrapper", "CopyingInputFiles", "AssemblingOutputFiles"])
                elif tstatus == "Done":
                    j.minor_status = "ExecutionCompleted"
                else:
                    j.minor_status = "FailedWithCode:%i" % choice([1, 2, 3, 4, 5, 6])
                j.setStatus(tstatus)
        job.update()
