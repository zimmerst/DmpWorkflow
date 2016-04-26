"""
Created on Apr 19, 2016

@author: zimmer
@brief: payload script
"""
import os
import sys
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.utils.tools import safe_copy, camelize, mkdir, rm
from DmpWorkflow.utils.shell import run
import logging, socket

if __name__ == '__main__':
    pwd = os.curdir
    DEBUG_TEST = False
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    FORMAT = '%(asctime)s %(levelname)s:%(message)s'
    logging.basicConfig(format=FORMAT, level=LOG_LEVEL, datefmt='%m/%d/%Y %I:%M:%S %p')
    log = logging.getLogger()
    fii = sys.argv[1]
    if os.path.isfile(fii):
        fii = open(fii, 'rb').read()
    log.info("reading json input")
    job = DmpJob.fromJSON(fii)
    os.environ["DWF_SIXDIGIT"] = job.getSixDigits()
    EXEC_DIR = os.getenv("EXEC_DIR_ROOT","/tmp")
    batchId = os.getenv("LSF_JOBID", "-1")
    my_exec_dir = os.path.join(EXEC_DIR,"local" if batchId == "-1" else batchId,job.getJob())
    mkdir(my_exec_dir)
    os.chdir(my_exec_dir)
    log.info("execution directory %s",my_exec_dir)
    try:
        job.updateStatus("Running", "PreparingInputData", hostname=socket.gethostname(), batchId=batchId)
    except Exception as err:
        log.exception(err)
    # first, set all variables
    for var in job.MetaData: os.environ[var['name']] = os.path.expandvars(var['value'])
    log.debug("current environment settings %s",str(os.environ))
    
    for fi in job.InputFiles:
        src = os.path.expandvars(fi['source'])
        tg = os.path.expandvars(fi['target'])
        try:
            safe_copy(src, tg, attempts=4, sleep='4s')
        except IOError, e:
            try:
                job.updateStatus("Running" if DEBUG_TEST else "Failed", camelize(e))
            except Exception as err:
                log.exception(err)
            finally:
                if not DEBUG_TEST:
                    exit(4)
    log.info("successfully completed staging.")
    # next, run the executable
    with open('payload', 'w') as foo:
        foo.write(job.exec_wrapper)
    log.info("about to run payload")
    CMD = "%s payload" % job.executable
    log.info("CMD: %s" % CMD)
    job.updateStatus("Running", "ExecutingApplication")
    output, error, rc = run([CMD])
    if rc:
        log.error("Payload returned exit code %i, see above for more details.", rc)
        try:
            job.updateStatus("Running" if DEBUG_TEST else "Failed", "ApplicationExitCode%i" % rc)
        except Exception as err:
            log.exception(err)
        finally:
            if not DEBUG_TEST:
                exit(5)
    log.info("successfully completed running application")
    
    # finally, compile output file.
    job.updateStatus("Running", "PreparingOutputData")
    for fi in job.OutputFiles:
        src = os.path.expandvars(fi['source'])
        tg = os.path.expandvars(fi['target'])
        try:
            safe_copy(src, tg, attempts=4, sleep='4s')
        except IOError, e:
            try:
                job.updateStatus("Running" if DEBUG_TEST else "Failed", camelize(e))
            except Exception as err:
                log.exception(err)
            finally:
                if not DEBUG_TEST:
                    exit(6)
    log.info("successfully completed staging.")
    log.info("job complete, cleaning up working directory")
    os.chdir(pwd)
    rm(my_exec_dir)
    try:
        job.updateStatus("Done", "ApplicationComplete")
    except Exception as err:
        log.exception(err)
    

