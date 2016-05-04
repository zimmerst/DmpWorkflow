"""
Created on Mar 15, 2016

@author: zimmer
"""
import requests
import sys
from argparse import ArgumentParser
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, BATCH_DEFAULTS, AppLogger

def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s taskName xmlFile [options]", description="create new job in DB")
    parser.add_argument("-d", "--dry", dest="dry", action = 'store_true', default=False, help='if dry, do not try interacting with batch farm')
    parser.add_argument("-l", "--local", dest="local", action = 'store_true', default=False, help='run locally')
    parser.add_argument("-p", "--pythonbin", dest="python", default=None, type=str, help='the python executable if non standard is chosen')
    parser.add_argument("-c", "--chunk", dest="chunk", default=100, type=int, help='number of jobs to process per cycle')
    parser.add_argument("-m", "--maxJobs", dest="maxJobs", default=None, type=int, help='number of jobs that can be in the system')
    opts = parser.parse_args(args)
    log = AppLogger("dampe-cli-fetch-new-jobs")
    batchsite = BATCH_DEFAULTS['name']
    if not opts.maxJobs is None:
        res = requests.get("%s/watchdog/" % DAMPE_WORKFLOW_URL, data = {"site":str(batchsite)})
        res.raise_for_status()
        res = res.json()
        if not res.get("result", "nok") == "ok":
            log.error(res.get("error"))
        jobs = res.get("jobs")
        if len(jobs) >= opts.maxJobs:
            log.info("reached maximum number of jobs per site, not submitting anything, change this value by setting it to higher value")
            sys.exit();
    res = requests.get("%s/newjobs/" % DAMPE_WORKFLOW_URL, data = {"site":str(batchsite)})
    res.raise_for_status()
    res = res.json()
    if not res.get("result", "nok") == "ok":
        log.error(res.get("error"))
    jobs = res.get("jobs")
    log.info('found %i new job instances to deploy',len(jobs))
    njobs = 0
    for i,job in enumerate(jobs):
        if i < opts.chunk: 
            j = DmpJob.fromJSON(job)
            #j.__updateEnv__()
            j.write_script(pythonbin=opts.python,debug=opts.dry)
            try: 
                ret = j.submit(dry=opts.dry,local=opts.local)
                j.updateStatus("Submitted","WaitingForExecution",batchId=ret)
                njobs+=1
            except Exception, e:
                log.exception(e)
                
    log.info("cycle completed, submitted %i new jobs",njobs)

if __name__ == "__main__":
    main()
