"""
Created on Mar 15, 2016

@author: zimmer
@todo: add watchdog triggers.
"""
import requests
import importlib
import json
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, BATCH_DEFAULTS, FINAL_STATII, AppLogger
HPC = importlib.import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])

def main():
    log = AppLogger("dampe-cli-update-alive-jobs")
    batchEngine = HPC.BatchEngine()
    batchEngine.update()
    for batchId, job_dict in batchEngine.allJobs.iteritems():
        #print batchId, job_dict
        try:
            JobId, InstanceId = job_dict['JOB_NAME'].split("-")
        except Exception as err:
            log.exception(err)
            continue
        hostname = job_dict["EXEC_HOST"]
        status = batchEngine.status_map[job_dict['STAT']] 
        if status in FINAL_STATII: continue
        cpu = float(batchEngine.getCPUtime(batchId))
        mem = float(batchEngine.getMemory(batchId))
        my_dict = {"t_id": JobId, "inst_id": InstanceId, "hostname": hostname, "major_status": status, "cpu":cpu, "memory":mem}
        log.debug("%s : %s",batchId,my_dict)
        res = requests.post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args":json.dumps(my_dict)})
        res.raise_for_status()
        res = res.json()
        if not res.get("result", "nok") == "ok":
            log.error("error updating %i %s",int(batchId), res.get("error"))
    log.info("completed cycle")
if __name__ == '__main__':
    main()
