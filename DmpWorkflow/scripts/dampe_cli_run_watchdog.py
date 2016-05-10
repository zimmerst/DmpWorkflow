"""
Created on Mar 15, 2016

@author: zimmer
@brief: watchdog that kills the job if needed.
"""
import requests
import importlib
import json
from logging import getLogger
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, BATCH_DEFAULTS, FINAL_STATII, cfg
from DmpWorkflow.utils.tools import getSixDigits, convertHHMMtoSec
HPC = importlib.import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])


def __getRunningJobs(batchsite):
    """ internal method to get running jobs """    
    log = getLogger("script")
    res = requests.get("%s/watchdog/" % DAMPE_WORKFLOW_URL, data = {"site":str(batchsite)})
    res.raise_for_status()
    res = res.json()
    if not res.get("result", "nok") == "ok":
        log.error(res.get("error"))
        return []
    jobs = res.get("jobs")
    return jobs

def __updateStatus(job, bj, mem, cpu, batchEngine = None, dry=True):
    """ internal method which reports jobStatus of running jobs to DB """
    log = getLogger("script")
    my_dict = {'t_id':job['t_id'],'inst_id':job['inst_id'],
               'major_status':'Terminated','minor_status':"KilledByBatch"}
    # check current status
    stat = batchEngine.status_map[batchEngine.allJobs[bj.batchId]['STAT']]
    if stat in FINAL_STATII:
        if job['major_status']!=stat:
            log.warning("found a job that should be in non-final state but batch reports it to be failed or done, updating db")
        else:
            del my_dict['minor_status']
            my_dict['memory']=mem
            my_dict['cpu']=cpu
        if not dry:
            res = requests.post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args": json.dumps(my_dict)})
            res.raise_for_status()
            res = res.json()
            if res.get("result", "nok") != "ok":
                log.exception(res.get("error"))
            else:
                log.debug("status updated")

def __reportKilledJob(j):
    """ internal method reports when a job was killed """
    log = getLogger("script")
    my_dict = {'t_id':j['t_id'],'inst_id':j['inst_id'],
                'major_status':'Terminated','minor_status':"KilledByWatchDog"}
    res = requests.post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args": json.dumps(my_dict)})
    res.raise_for_status()
    res = res.json()
    if res.get("result", "nok") != "ok":
        log.exception(res.get("error"))
    else:
        log.debug("status updated")
 
def main(args=None):
    usage = "Usage: %(prog)s [options]"
    description = "run watchdog"
    parser = ArgumentParser(usage=usage, description=description)
    parser.add_argument("--site", dest="site", type=str, default=None, help='name of site', required=False)
    parser.add_argument("--dry", dest="dry", action = 'store_true', default=False, help='test-run')
    opts = parser.parse_args(args)

    ratio_cpu_max = float(cfg.get("watchdog","ratio_cpu"))
    ratio_mem_max = float(cfg.get("watchdog","ratio_mem"))

    log = getLogger("script")
    batchEngine = HPC.BatchEngine()
    batchEngine.update()
    batchsite = BATCH_DEFAULTS['name'] if opts.site is None else opts.site
    # first, get running jobs.
    jobs = __getRunningJobs(batchsite)
    log.info("watchdog settings: max_cpu %1.2f max_mem %1.2f (ratio with respect to max. allocated)",ratio_cpu_max,ratio_mem_max)    
    for j in jobs:
        max_cpu = float(j['max_mem'])
        max_mem = float(j['max_cpu'])
        if max_cpu == -1: max_cpu = float(convertHHMMtoSec(BATCH_DEFAULTS['cputime']))
        if max_mem == -1: max_mem = float(BATCH_DEFAULTS['memory'])
        bj = HPC.BatchJob(name="%s-%s"%(j['t_id'],getSixDigits(j['inst_id'])),
                          batchId = j['batchId'], defaults=BATCH_DEFAULTS)
        if str(bj.batchId) in batchEngine.allJobs:
            current_cpu = batchEngine.getCPUtime(str(bj.batchId))
            current_mem = batchEngine.getMemory(str(bj.batchId),unit='MB')
            ratio_cpu = current_cpu/max_cpu
            ratio_mem = current_mem/max_mem
            __updateStatus(j, bj, current_mem, current_cpu, batchEngine=batchEngine, dry=opts.dry)                
            if (ratio_cpu >= ratio_cpu_max) or (ratio_mem >= ratio_mem_max):
                log.info('%s cpu %1.1f mem %1.1f',bj.batchId,ratio_cpu, ratio_mem)
                log.warning('Watchdog identified job %s to exceed its sending kill signal', bj.batchId)            
                if opts.dry: continue
                try:
                    bj.kill()
                except Exception as err:
                    log.exception("could not schedule job for removal, reason below\n%s",err)
                if bj.status == "Failed":
                    __reportKilledJob(j)
    log.info("completed cycle")
    
if __name__ == '__main__':
    main()
