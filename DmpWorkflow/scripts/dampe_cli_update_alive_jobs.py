"""
Created on Mar 15, 2016

@author: zimmer
@todo: add watchdog triggers.
"""
import requests
import importlib
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, BATCH_DEFAULTS
HPC = importlib.import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])
#def check_status(jobId):
#    return True

def main():
    batchEngine = HPC.BatchEngine()
    batchEngine.update()
    for batchId, job_dict in batchEngine.allJobs.iteritems():
        JobId, InstanceId = job_dict['JOB_NAME'].split(".")
        hostname = job_dict["EXEC_HOST"]
        status = batchEngine.status_map[job_dict['STAT']]
        res = requests.post("%s/jobalive/" % DAMPE_WORKFLOW_URL, data={"taskid": JobId, "instanceid": InstanceId,
                                                                       "hostname": hostname, "status": status})
        res.raise_for_status()
        res = res.json()
        if not res.get("result", "nok") == "ok":
            print "error updating %i %s"%(int(batchId), res.get("error"))

if __name__ == '__main__':
    main()
