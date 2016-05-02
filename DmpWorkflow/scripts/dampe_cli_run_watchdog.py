"""
Created on Mar 15, 2016

@author: zimmer
@brief: watchdog that kills the job if needed.
"""
import requests
import importlib
import json
import copy
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, BATCH_DEFAULTS, FINAL_STATII, AppLogger, cfg
from DmpWorkflow.utils.tools import getSixDigits
HPC = importlib.import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])

def main(args=None):
    usage = "Usage: %(prog)s [options]"
    description = "run watchdog"
    parser = ArgumentParser(usage=usage, description=description)
    parser.add_argument("--site", dest="site", type=str, default=None, help='name of site', required=False)
    opts = parser.parse_args(args)
    log = AppLogger("watchdog")
    batchEngine = HPC.BatchEngine()
    batchEngine.update()
    batchsite = BATCH_DEFAULTS['name'] if opts.site is None else opts.site
    res = requests.get("%s/watchdog/" % DAMPE_WORKFLOW_URL, data = {"site":str(batchsite)})
    res.raise_for_status()
    res = res.json()
    if not res.get("result", "nok") == "ok":
        log.error(res.get("error"))
    jobs = res.get("jobs")

    ratio_cpu_max = float(cfg.get("watchdog","ratio_cpu"))
    ratio_mem_max = float(cfg.get("watchdog","ratio_mem"))
    log.info("watchdog settings: max_cpu %1.2f max_mem %1.2f (ratio with respect to max. allocated)",ratio_cpu_max,ratio_mem_max)    
    site_defaults = BATCH_DEFAULTS    
    for j in jobs:
        job_defaults = copy.deepcopy(site_defaults)
        meta = j['meta']
        for key in ['cputime','memory']:
            for var in meta: 
                if var['name']=="BATCH_OVERRIDE_%s"%key.upper(): 
                    site_defaults[key]=var['value']
        bj = HPC.BatchJob(name="%s-%s"%(j['t_id'],getSixDigits(j['inst_id'])),
                          batchId = j['batchId'],defaults=job_defaults)
        current_cpu = float(j['cpu'])
        current_mem = float(j['memory'])
        max_cpu = bj.getCPU()
        max_mem = bj.getMemory(unit='MB')
        ratio_cpu = current_cpu/max_cpu
        ratio_mem = current_mem/max_mem
        if (ratio_cpu >= ratio_cpu_max) or (ratio_mem >= ratio_mem_max):
            log.info('%s cpu %1.1f mem %1.1f',bj.batchId,ratio_cpu, ratio_mem)
            log.warning('Watchdog identified job %s to exceed its sending kill signal', bj.batchId)            
            try:
                bj.kill()
            except Exception as err:
                log.exception("could not schedule job for removal, reason below\n%s",err)
            if bj.status == "Failed":
                my_dict = {'t_id':j['t_id'],'inst_id':j['inst_id'],
                           'major_status':'Terminated','minor_status':"KilledByWatchDog"}
                res = requests.post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args": json.dumps(my_dict)})
                res.raise_for_status()
                res = res.json()
                if res.get("result", "nok") != "ok":
                    log.exception(res.get("error"))
                else:
                    log.debug("status updated")
    print 'found %i jobs with requirements'%len(jobs)
    log.info("completed cycle")
if __name__ == '__main__':
    main()
