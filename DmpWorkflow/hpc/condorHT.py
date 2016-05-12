'''
Created on May 12, 2016

@author: zimmer
@brief: interface to Condor API
'''
from DmpWorkflow.hpc.batch import BATCH, BatchJob as HPCBatchJob
from DmpWorkflow.utils.shell import run
from importlib import import_module
classad = import_module("classad")
htcondor= import_module("htcondor")


# LSF-specific stuff
BATCH_ID_ENV = "CONDOR_ID"

class BatchJob(HPCBatchJob):
    def submit(self, **kwargs):
        """ each class MUST implement its own submission command """
        return -1
    def getCPU(self):
        """ format is 00:00 """
        cputime = None
        blocks = self.cputime.split(":")
        if len(blocks) == 2:
            cputime = float(blocks[0])*3600+float(blocks[1])*60
        else:
            cputime = 0.
        return cputime
    
    def getMemory(self,unit='kB'):
        kret = float(self.memory)
        if unit in ['MB','GB']:
            kret/=1024.
            if unit == 'GB':
                kret/=1024.
        return kret
    
    def __regexId__(self,_str):
        """ returns the batch Id using some regular expression, lsf specific """
        # default: Job <32110807> is submitted to queue <dampe>.
        bk = -1
        return bk


    def kill(self):
        """ likewise, it should implement its own batch-specific removal command """
        self.update("status","Failed")


class BatchEngine(BATCH):
    kind = "HTCondor"
    keys = []
    status_map = {}
    parameter_map = {"mem":None, "cpu": None}

    def update(self):
        self.allJobs.update(self.aggregateStatii())
    def getCPUtime(self, job, key=None):
        """ format is: 000:00:00.00 """
        del job
        del key
        totalSecs = 0.
        return totalSecs
        
    def getMemory(self,jobId, key = "MEM", unit='kB'):
        """ format is kb, i believe."""
        mem = 0.
        del jobId
        del key
        del unit
        return mem

    def getRunningJobs(self,pending=False):
        self.update()
        running = []
        pendingJobs = []
        val = running
        if pending: val+=pendingJobs 
        return val
    
    def aggregateStatii(self, asDict=True, command=None):
        """ get status of all jobs in all queues """
        jobs = []
        if asDict: pass
        if command is not None: pass
        return jobs