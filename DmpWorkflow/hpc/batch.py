"""
Created on Mar 22, 2016

@author: zimmer
"""
from DmpWorkflow.utils.shell import run
import logging

BATCH_ID_ENV = "NOT_DEFINED"
class BatchJob(object):
    """ generic batch job which can be expanded by classes inheriting from this class """
    name = None
    logFile = None
    queue = None
    cputime = 0.
    memory = 0.
    command = ""
    extra = ""
    defaults = None
    requirements = []
    status = None
    logging = logging.getLogger("batch")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.__processDefaults__()

    def __processDefaults__(self):
        if self.defaults is None: return
        self.queue = self.defaults['queue']
        self.requirements = self.defaults['requirements']
        self.extra = self.defaults['extra']
        self.memory = self.defaults['memory']
        self.cputime = self.defaults['cputime']

    def __execWithUpdate__(self, cmd, key, value=None):
        """ execute command cmd & update key with output from running """
        output, error, rc = run(cmd.split(),suppressLevel=True,interleaved=False,useLogging=False, suppressLevel=True)
        self.logging.debug("execution with rc: %i",int(rc))
        if error:
            for e in error.split("\n"):
                if len(e): self.logging.error(e)

        if value is None:
            self.update(key, output)
        else:
            self.update(key, value)

    def submit(self, **kwargs):
        """ each class MUST implement its own submission command """
        pass

    def kill(self):
        """ likewise, it should implement its own batch-specific removal command """
        pass

    def update(self, key, value):
        if key in self.__dict__:
            self.__dict__[key] = value

    #def get(self, key, callable=str):
    #    if key in self.__dict__:
    #        return callable(self.__dict__[key])
    #    return None

    def getCPU(self):
        return 0.
    def getMemory(self,unit='kB'):
        print unit
        return 0.
    
    def __run__(self,cmd):
        if not isinstance(cmd,list): cmd = cmd.split()
        output, error, rc = run(cmd,useLogging=False,interleaved=False, suppressLevel=True)
        self.logging.debug("execution with rc: %i",int(rc))
        if error:
            for e in error.split("\n"):
                if len(e): self.logging.error(e)
        if rc:
            err = "exception during execution"
            self.logging.error(err)
            raise Exception(err)
        return output


class BATCH(object):
    """
    generic Batch class, all HPC-specific modules should inherit from it.
    """
    allJobs = {}
    keys = []
    status_map = {}
    parameter_map = {}
    kind = "generic"
    user = None

    def __init__(self):
        self.logging = logging.getLogger("core")

    def update(self):
        return {}
    def getCPUtime(self,job, key = None):
        print job, key
        return 0.
    def getMemory(self,job, key = None, unit='kB'):
        print job, key, unit
        return 0.
    
    def getRunningJobs(self,pending=False):
        """ should be implemented by subclass """
        if pending: print 'including pending jobs'
        return []
    
    def addBatchJob(self,job):
        if not isinstance(job,BatchJob):
            self.logging.error("must be BatchJob instance")
            raise Exception
        self.allJobs[job.batchId]=job
        return
    
    def __checkKeys__(self, key):
        if key not in self.keys:
            self.logging.error("could not extract key, allowed keys %s", str(self.keys))
            raise Exception

    def setUser(self,user):
        self.user = user

    def getUser(self): 
        return self.user

    def getJob(self, jobID, key="STAT"):
        if jobID not in self.allJobs:
            self.logging.error("could not find job %s", jobID)
        self.__checkKeys__(key)
        return callable(self.allJobs[jobID][key])

    def getAttributeForAllJobs(self, attr="MEM"):
        """ convenience function to return all values for a certain attribute """
        key = attr
        self.__checkKeys__(key)
        ret = {}
        for jobID in self.allJobs:
            val = None
            if key not in self.allJobs[jobID]:
                self.logging.error("could not find key %s in job %s", key, jobID)
            else:
                val = self.allJobs[jobID][key]
            #print val
            ret[jobID] = val
        return ret