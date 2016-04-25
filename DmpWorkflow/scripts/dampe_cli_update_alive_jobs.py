"""
Created on Mar 15, 2016

@author: zimmer
"""
import requests

from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL
from DmpWorkflow.hpc.lsf import LSF


# FIXME: add watchdog triggers
def check_status(jobId):
    return True


def main():
    # db.connect()
    batchEngine = LSF()
    batchEngine.update()
    for batchId, job_dict in batchEngine.allJobs:
        JobId, InstanceId = job_dict['JOB_NAME'].split(".")
        hostname = job_dict["EXEC_HOST"]
        status = batchEngine.status_map[job_dict['STAT']]
        res = requests.post("%s/jobalive/" % DAMPE_WORKFLOW_URL, data={"taskid": JobId, "instanceid": InstanceId,
                                                                       "hostname": hostname, "status": status})
        res.raise_for_status()
        res = res.json()
        if not res.get("result", "nok") == "ok":
            print "error %s" % res.get("message")
            # my_job = Job.objects.filter(id=str(JobId))
            # jInstance = my_job.getInstance(InstanceId)
            # jInstance.set("hostname", job_dict["EXEC_HOST"])
            # oldStatus = jInstance.status
            # newStatus = batchEngine.status_map[job_dict['STAT']]
            # if newStatus != oldStatus:
            #     jInstance.setStatus(newStatus)
            # my_job.update()


if __name__ == '__main__':
    main()
