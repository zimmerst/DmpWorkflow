"""
Created on Apr 19, 2016

@author: zimmer
@brief: payload script
"""
import logging
from os.path import expandvars, abspath, dirname, isdir, isfile, join as oPjoin
from os import curdir, environ, listdir, chdir, getenv
from importlib import import_module
from socket import gethostname
from sys import exit, argv
from DmpWorkflow.config.defaults import EXEC_DIR_ROOT, BATCH_DEFAULTS
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.utils.tools import safe_copy, camelize, mkdir, rm, ResourceMonitor
from DmpWorkflow.utils.shell import run
HPC = import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])

def __prepare(job, log, resources=None):
    try:
        job.updateStatus("Running", "PreparingInputData", hostname=gethostname(), batchId=batchId, resources=resources)
    except Exception as err: log.exception(err)
    # first, set all variables
    for var in job.MetaData: environ[var['name']] = expandvars(var['value'])
    log.info("current environment settings")
    log.info("\n".join(["%s: %s"%(key,value) for key, value in sorted(environ.iteritems())]))    
    for fi in job.InputFiles:
        src = expandvars(fi['source'])
        tg = expandvars(fi['target'])
        try:
            safe_copy(src, tg, attempts=4, sleep='4s')
        except IOError, e:
            try:
                job.updateStatus("Running" if DEBUG_TEST else "Failed", camelize(e), resources=resources)
            except Exception as err: log.exception(err)
            if not DEBUG_TEST: return 4
    log.info("content of current working directory %s: %s",abspath(curdir),str(listdir(curdir)))
    log.info("successfully completed staging.")
    return 0

def __runPayload(job, log, resources=None):
    with open('payload', 'w') as foop: 
        foop.write(job.exec_wrapper)
        foop.close()
    log.info("about to run payload")
    CMD = "%s payload" % job.executable
    log.info("CMD: %s", CMD)
    job.updateStatus("Running", "ExecutingApplication", resources=resources)
    output, error, rc = run([CMD])
    for o in output.split("\n"): print o
    if rc:
        log.error("Payload returned exit code %i, see below for more details.", rc)
        for e in error.split("\n"):
            if len(e): log.error(e)
        try:
            job.updateStatus("Running" if DEBUG_TEST else "Failed", "ApplicationExitCode%i" % rc, resources=resources)
        except Exception as err: log.exception(err)
        if not DEBUG_TEST: return 5
    log.info("successfully completed running application")
    log.info("content of current working directory %s: %s",abspath(curdir),str(listdir(curdir)))
    return 0

def __postRun(job, log, resources=None):
    job.updateStatus("Running", "PreparingOutputData", resources= resources)
    for fi in job.OutputFiles:
        src = expandvars(fi['source'])
        tg = expandvars(fi['target'])
        _dir = dirname(tg)
        if not isdir(_dir): 
            log.info("creating output directory %s",_dir)
            mkdir(_dir)
        try:
            safe_copy(src, tg, attempts=4, sleep='4s')
        except IOError, e:
            try:
                job.updateStatus("Running" if DEBUG_TEST else "Failed", camelize(e), resources=resources)
            except Exception as err: log.exception(err)
            if not DEBUG_TEST: exit(6)
    log.info("successfully completed staging.")


if __name__ == '__main__':
    RM = ResourceMonitor()
    pwd = curdir
    DEBUG_TEST = False
    log = logging.getLogger("script")
    fii = argv[1]
    if isfile(fii):
        fii = open(fii, 'rb').read()
    log.info("reading json input")
    job = DmpJob.fromJSON(fii)
    environ["DWF_SIXDIGIT"] = job.getSixDigits()
    batchId = getenv(HPC.BATCH_ID_ENV, "-1")
    my_exec_dir = oPjoin(EXEC_DIR_ROOT,job.getSixDigits(),"local" if batchId == "-1" else batchId)
    mkdir(my_exec_dir)
    chdir(my_exec_dir)
    log.info("execution directory %s",my_exec_dir)
    #DMPSWSYS = getenv("DMPSWSYS")
    #DAMPE_SW_DIR = getenv("DAMPE_SW_DIR",None)
    #if DAMPE_SW_DIR is None:
    #    raise Exception("must define $DAMPE_SW_DIR")
    # next, run the executable
    #if not DAMPE_SW_DIR in DMPSWSYS:
    #    log.info("trying to re-source setup script.")
    #    job.sourceSetupScript()
    rc = 0
    rc += __prepare(job, log, resources=RM)
    if rc: exit(rc)
    rc += __runPayload(job, log, resources=RM)
    if rc: exit(rc)
    rc += __postRun(job,log, resources=RM)
    if rc: exit(rc)
    # finally, compile output file.
    log.info("job complete, cleaning up working directory")
    chdir(pwd)
    rm(my_exec_dir)
    try:
        job.updateStatus("Done", "ApplicationComplete", resources=RM)
    except Exception as err:
        log.exception(err)
        

