"""
Created on Mar 15, 2016

@author: zimmer
@todo: add watchdog triggers.
"""
import requests
import importlib
import json
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, BATCH_DEFAULTS, AppLogger
HPC = importlib.import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])

def main():
    log = AppLogger("dampe-cli-update-alive-jobs")
    batchEngine = HPC.BatchEngine()
    batchEngine.update()
    for batchId, job_dict in batchEngine.allJobs.iteritems():
        print batchId, job_dict
        JobId, InstanceId = job_dict['JOB_NAME'].split("-")
        hostname = job_dict["EXEC_HOST"]
        status = batchEngine.status_map[job_dict['STAT']]
        cpu = job_dict[batchEngine.parameter_map['cpu']]
        mem = job_dict[batchEngine.parameter_map['mem']]
        my_dict = {"taskid": JobId, "instanceid": InstanceId, "hostname": hostname, "status": status, "cpu":cpu, "memory":mem}
        res = requests.post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args":json.dumps(my_dict)})
        res.raise_for_status()
        res = res.json()
        if not res.get("result", "nok") == "ok":
            log.error("error updating %i %s",int(batchId), res.get("error"))
    log.info("completed cycle")
if __name__ == '__main__':
    main()
