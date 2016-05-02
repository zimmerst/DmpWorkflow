"""
Created on Mar 15, 2016

@author: zimmer
@brief: watchdog that kills the job if needed.
"""
import requests
import importlib
import json
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, BATCH_DEFAULTS, FINAL_STATII, AppLogger
HPC = importlib.import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])

def main():
    log = AppLogger("watchdog")
    batchEngine = HPC.BatchEngine()
    batchEngine.update()
    batchsite = BATCH_DEFAULTS['name']
    res = requests.get("%s/watchdog/" % DAMPE_WORKFLOW_URL, data = {"site":str(batchsite)})
    res.raise_for_status()
    res = res.json()
    if not res.get("result", "nok") == "ok":
        log.error(res.get("error"))
    jobs = res.get("jobs")
    print 'found %i jobs with requirements'
    log.info("completed cycle")
if __name__ == '__main__':
    main()
