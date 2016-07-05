"""
Created on Mar 15, 2016

@author: zimmer
@todo: add watchdog triggers.
"""
import logging
from requests import post
from importlib import import_module
from json import dumps
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, BATCH_DEFAULTS, FINAL_STATII

HPC = import_module("DmpWorkflow.hpc.%s" % BATCH_DEFAULTS['system'])


def main():
    log = logging.getLogger("script")
    site = BATCH_DEFAULTS['name']
    batchEngine = HPC.BatchEngine()
    batchEngine.update()
    for batchId, job_dict in batchEngine.allJobs.iteritems():
        # print batchId, job_dict
        hostname = job_dict.get("EXEC_HOST", "None")
        JobId = "None"
        InstanceId = "None"
        try:
            JobId, InstanceId = job_dict['JOB_NAME'].split("-")
        except Exception as err:
            if hostname == "None":
                log.error("trapped exception for job %s (%s)", str(batchId), str(job_dict['JOB_NAME']))
                log.debug(err)
                continue
        status = "Unknown"
        if job_dict['STAT'] in batchEngine.status_map:
            status = batchEngine.status_map[job_dict['STAT']]
        if status in FINAL_STATII:
            continue
        cpu = float(batchEngine.getCPUtime(batchId))
        mem = float(batchEngine.getMemory(batchId, unit='MB'))
        my_dict = {"t_id": JobId, "inst_id": InstanceId, "hostname": hostname,
                   "major_status": status, "cpu": cpu, "memory": mem, "site": site}
        log.debug("%s : %s", batchId, my_dict)
        res = post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args": dumps(my_dict)})
        res.raise_for_status()
        res = res.json()
        if not res.get("result", "nok") == "ok":
            log.error("error updating %i %s", int(batchId), res.get("error"))
    log.info("completed cycle")


if __name__ == '__main__':
    main()
