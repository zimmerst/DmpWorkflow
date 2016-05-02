"""
Created on Apr 19, 2016

@author: zimmer
@brief: payload script
"""
import os
import sys
import importlib
import socket
from DmpWorkflow.config.defaults import EXEC_DIR_ROOT, BATCH_DEFAULTS, AppLogger
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.utils.tools import safe_copy, camelize, mkdir, rm
from DmpWorkflow.utils.shell import run
HPC = importlib.import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])

if __name__ == '__main__':
    pwd = os.curdir
    DEBUG_TEST = False
    log = AppLogger("dampe-payload-executor")
    fii = sys.argv[1]
    if os.path.isfile(fii):
        fii = open(fii, 'rb').read()
    log.info("reading json input")
    job = DmpJob.fromJSON(fii)
    os.environ["DWF_SIXDIGIT"] = job.getSixDigits()
    batchId = os.getenv(HPC.BATCH_ID_ENV, "-1")
    my_exec_dir = os.path.join(EXEC_DIR_ROOT,job.getSixDigits(),"local" if batchId == "-1" else batchId)
    mkdir(my_exec_dir)
    os.chdir(my_exec_dir)
    log.info("execution directory %s",my_exec_dir)
    try:
        job.updateStatus("Running", "PreparingInputData", hostname=socket.gethostname(), batchId=batchId)
    except Exception as err: log.exception(err)
    # first, set all variables
    for var in job.MetaData: os.environ[var['name']] = os.path.expandvars(var['value'])
    log.info("current environment settings")
    log.info("\n".join(["%s: %s"%(key,value) for key, value in sorted(os.environ.iteritems())]))    
    for fi in job.InputFiles:
        src = os.path.expandvars(fi['source'])
        tg = os.path.expandvars(fi['target'])
        try:
            safe_copy(src, tg, attempts=4, sleep='4s')
        except IOError, e:
            try:
                job.updateStatus("Running" if DEBUG_TEST else "Failed", camelize(e))
            except Exception as err: log.exception(err)
            if not DEBUG_TEST: exit(4)
    log.info("content of current working directory %s: %s",os.path.abspath(os.curdir),str(os.listdir(os.curdir)))
    
    log.info("successfully completed staging.")
    # next, run the executable
    with open('payload', 'w') as foo: 
        foo.write(job.exec_wrapper)
        foo.close()
    log.info("about to run payload")
    CMD = "%s payload" % job.executable
    log.info("CMD: %s", CMD)
    job.updateStatus("Running", "ExecutingApplication")
    output, error, rc = run([CMD])
    for o in output.split("\n"): print o
    if rc:
        log.error("Payload returned exit code %i, see above for more details.", rc)
        try:
            job.updateStatus("Running" if DEBUG_TEST else "Failed", "ApplicationExitCode%i" % rc)
        except Exception as err: log.exception(err)
        if not DEBUG_TEST: exit(5)
    log.info("successfully completed running application")
    log.info("content of current working directory %s: %s",os.path.abspath(os.curdir),str(os.listdir(os.curdir)))
    
    # finally, compile output file.
    job.updateStatus("Running", "PreparingOutputData")
    for fi in job.OutputFiles:
        src = os.path.expandvars(fi['source'])
        tg = os.path.expandvars(fi['target'])
        _dir = os.path.dirname(tg)
        if not os.path.isdir(_dir): 
            log.info("creating output directory %s",_dir)
            mkdir(_dir)
        try:
            safe_copy(src, tg, attempts=4, sleep='4s')
        except IOError, e:
            try:
                job.updateStatus("Running" if DEBUG_TEST else "Failed", camelize(e))
            except Exception as err: log.exception(err)
            if not DEBUG_TEST: exit(6)
    log.info("successfully completed staging.")
    log.info("job complete, cleaning up working directory")
    os.chdir(pwd)
    rm(my_exec_dir)
    try:
        job.updateStatus("Done", "ApplicationComplete")
    except Exception as err:
        log.exception(err)
    

