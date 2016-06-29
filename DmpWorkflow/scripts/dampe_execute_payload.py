"""
Created on Apr 19, 2016

@author: zimmer
@brief: payload script
"""
from os.path import expandvars, abspath, dirname, isdir, isfile, join as oPjoin
from os import curdir, environ, listdir, chdir, getenv
from importlib import import_module
from socket import gethostname
from sys import exit as sys_exit, argv
from DmpWorkflow.config.defaults import EXEC_DIR_ROOT, BATCH_DEFAULTS
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.utils.tools import safe_copy, camelize, mkdir, rm, ResourceMonitor
from DmpWorkflow.utils.shell import run
from re import findall
HPC = import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])
from time import ctime

def logThis(msg,*args):
    val= msg%args
    print "%s: %s: %s"%(ctime(), gethostname(), val)

def __prepare(job, resources=None):
    try:
        job.updateStatus("Running", "PreparingInputData", hostname=gethostname(), batchId=batchId, resources=resources)
    except Exception as err: logThis("EXCEPTION: %s",err)
    # first, set all variables
    for var in job.MetaData: environ[var['name']] = expandvars(var['value'])
    logThis("current environment settings")
    #log.info("\n".join(["%s: %s"%(key,value) for key, value in sorted(environ.iteritems())]))    
    for fi in job.InputFiles:
        src = expandvars(fi['source'])
        tg = expandvars(fi['target'])
        try:
            safe_copy(src, tg, attempts=4, sleep='4s', checksum=True)
        except IOError, e:
            try:
                job.updateStatus("Running" if DEBUG_TEST else "Failed", camelize(e), resources=resources)
            except Exception as err: logThis("EXCEPTION: %s",err)
            if not DEBUG_TEST: return 4
    logThis("content of current working directory %s: %s",abspath(curdir),str(listdir(curdir)))
    logThis("successfully completed staging.")
    return 0

def __runPayload(job, resources=None):
    def __file_cleanup(file1, file2):
        file1.close()
        file2.close()
        rm(file1.name)
        rm(file2.name)
    with open('payload', 'w') as foop: 
        foop.write(job.exec_wrapper)
        foop.close()
    logThis("about to run payload")
    CMD = "%s payload" % job.executable
    logThis("CMD: %s", CMD)
    job.updateStatus("Running", "ExecutingApplication", resources=resources)
    output, error, rc = run(CMD.split(),suppressLevel=True, cache=True, chunksize=36) # use caching to file!
    for o in output: print o
    if rc:
        logThis("ERROR: Payload returned exit code %i, see below for more details.", rc)
        for e in error:
            if len(e): logThis("ERROR: %s",e)
        try:
            job.updateStatus("Running" if DEBUG_TEST else "Failed", "ApplicationExitCode%i" % rc, resources=resources)
        except Exception as err: logThis("EXCEPTION: %s",err)
        __file_cleanup(output, error)
        if not DEBUG_TEST: return 5
    logThis("successfully completed running application")
    logThis("content of current working directory %s: %s",abspath(curdir),str(listdir(curdir)))
    __file_cleanup(output, error)
    return 0

def __postRun(job, resources=None):
    job.updateStatus("Running", "PreparingOutputData", resources= resources)
    for fi in job.OutputFiles:
        src = expandvars(fi['source'])
        tg = expandvars(fi['target'])
        _dir = dirname(tg)
        if not isdir(_dir): 
            logThis("creating output directory %s",_dir)
            mkdir(_dir)
        try:
            safe_copy(src, tg, attempts=4, sleep='4s', checksum=True)
            job.registerDS(filename=tg, overwrite=True)
        except Exception, e:
            try:
                job.updateStatus("Running" if DEBUG_TEST else "Failed", camelize(e), resources=resources)
            except Exception as err: logThis("EXCEPTION: %s",err)
            if not DEBUG_TEST: return 6
        ## add registerDS
    logThis("successfully completed staging.")
    return 0

if __name__ == '__main__':
    RM = ResourceMonitor()
    pwd = curdir
    DEBUG_TEST = False
    fii = argv[1]
    if isfile(fii):
        fii = open(fii, 'rb').read()
    logThis("reading json input")
    job = DmpJob.fromJSON(fii)
    environ["DWF_SIXDIGIT"] = job.getSixDigits()
    batchId = getenv(HPC.BATCH_ID_ENV, "-1")
    if "." in batchId:
        res = findall("\d+",batchId)
        if len(res):
            batchId = int(res[0])
    print 'batchId : %s'%str(batchId)
    print 'EXEC_DIR_ROOT: %s'%EXEC_DIR_ROOT
    print 'instanceId : %s'%str(job.getSixDigits())
    my_exec_dir = oPjoin(EXEC_DIR_ROOT,job.getSixDigits(),"local" if batchId == "-1" else str(batchId))
    mkdir(my_exec_dir)
    chdir(my_exec_dir)
    logThis("execution directory %s",my_exec_dir)
    #DMPSWSYS = getenv("DMPSWSYS")
    #DAMPE_SW_DIR = getenv("DAMPE_SW_DIR",None)
    #if DAMPE_SW_DIR is None:
    #    raise Exception("must define $DAMPE_SW_DIR")
    # next, run the executable
    #if not DAMPE_SW_DIR in DMPSWSYS:
    #    log.info("trying to re-source setup script.")
    #    job.sourceSetupScript()
    rc = 0
    rc += __prepare(job, resources=RM)
    if rc: sys_exit(rc)
    rc += __runPayload(job, resources=RM)
    if rc: sys_exit(rc)
    rc += __postRun(job, resources=RM)
    if rc: sys_exit(rc)
    # finally, compile output file.
    logThis("job complete, cleaning up working directory")
    chdir(pwd)
    rm(my_exec_dir)
    try:
        job.updateStatus("Done", "ApplicationComplete", resources=RM)
    except Exception as err:
        logThis("EXCEPTION: %s",err)
        

