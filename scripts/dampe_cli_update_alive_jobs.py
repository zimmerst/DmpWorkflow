"""
Created on Mar 15, 2016

@author: zimmer
"""
from core.models import Job
from utils.scriptDefaults import cfg
import copy, sys, time
from core import db
from utils.flask_helpers import update_status
from hpc.lsf import LSF 


# FIXME: add watchdog triggers
def check_status(jobId):
    return True

    
def main():
    db.connect()
    batchEngine = LSF()
    batchEngine.update()
    for batchId in batchEngine.allJobs:
        job_dict = batchEngine[batchId]
        JobId, InstanceId = job_dict['JOB_NAME'].split(".")
        my_job = Job.objects.filter(id=str(JobId))
        jInstance = my_job.getInstance(InstanceId)
        jInstance.set("hostname", job_dict["EXEC_HOST"])
        oldStatus = jInstance.status
        newStatus = batchEngine.status_map[job_dict['STAT']]
        if newStatus != oldStatus:
            jInstance.setStatus(newStatus)
        my_job.update()

if __name__ == '__main__':
    main()
