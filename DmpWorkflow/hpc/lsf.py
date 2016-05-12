"""
Created on Mar 23, 2016

@author: zimmer
"""
from re import findall
from DmpWorkflow.hpc.batch import BATCH, BatchJob as HPCBatchJob
from DmpWorkflow.utils.shell import run


# LSF-specific stuff
BATCH_ID_ENV = "LSB_JOBID"

class BatchJob(HPCBatchJob):
    def submit(self, **kwargs):
        """ each class MUST implement its own submission command """
        extra = "%s" % self.extra if isinstance(self.extra, str) else None
        if isinstance(self.extra, dict):
            self.extra.update(kwargs)
            extra = "-%s %s".join([(k, v) for (k, v) in self.extra.iteritems()])
        while "\"" in extra: extra = extra.replace("\"","")
        # explicit list conversion
        if self.requirements == "": self.requirements = []
        if isinstance(self.requirements,str): self.requirements = self.requirements.split(",")
        self.requirements.append("rusage[mem=%i]"%int(self.memory))

        req_str = " && ".join(self.requirements)
        #print self.requirements, "STRING: ",req_str # to be removed!
        req = "-R \"%s\""%req_str
        cmd = "bsub -J {5} -W {6} -q {0} -oo {1} {2} {3} {4}".format(self.queue, self.logFile, req, extra,\
                                                                     self.command, self.name, self.cputime)
        if 'verbose' in kwargs and kwargs['verbose']: print cmd
        #print cmd
        output = self.__run__(cmd)
        return self.__regexId__(output)
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
        res = findall("\d+",_str)
        if len(res):
            bk = int(res[0])
        return bk


    def kill(self):
        """ likewise, it should implement its own batch-specific removal command """
        cmd = "bkill %s" % self.batchId
        self.__run__(cmd)
        self.update("status","Failed")


class BatchEngine(BATCH):
    kind = "lsf"
    keys = ["USER","STAT","QUEUE","FROM_HOST","EXEC_HOST","JOB_NAME","SUBMIT_TIME","PROJ_NAME","CPU_USED",\
                "MEM","SWAP","PIDS","START_TIME","FINISH_TIME","SLOTS"]
    status_map = {"RUN": "Running", "PEND": "Submitted", "SSUSP": "Suspended",
                       "EXIT": "Failed", "DONE": "Done", "UNKWN": "Failed"}
    parameter_map = {"mem":"MEM","cpu":"CPU_USED"}

    def update(self):
        self.allJobs.update(self.aggregateStatii())
    
    def getCPUtime(self,jobId, key = "CPU_USED"):
        """ format is: 000:00:00.00 """
        if jobId not in self.allJobs:
            return 0.
        cpu_str = self.allJobs[jobId][key]
        hr,_min,secs = cpu_str.split(":")
        totalSecs = float(secs)+60*float(_min)+3600*float(hr)
        return totalSecs
    
    def getMemory(self,jobId, key = "MEM", unit='kB'):
        """ format is kb, i believe."""
        if jobId not in self.allJobs:
            return 0.
        mem_str = self.allJobs[jobId][key]
        mem = float(mem_str)
        if unit in ['MB','GB']:
            mem/=1024.
            if unit == 'GB':
                mem/=1024.
        return mem

    def getRunningJobs(self,pending=False):
        self.update()
        running = [j for j in self.allJobs if j['STAT']=="RUN"]
        pending = [j for j in self.allJobs if j['STAT']=="PEND"]
        return running + pending
    
    def aggregateStatii(self, asDict=True, command=None):
        #print self.keys
        if command is None:
            command = ["bjobs -Wa"]
        jobs = {}
        output, error, rc = run(command)
        self.logging.debug("rc: %i",int(rc))
        if error is not None:
            #print error.split("\n")
            for e in error.split("\n"):
                if len(e): self.logging.error(e)
        if not asDict:
            return output
        else:
            for i, line in enumerate(output.split("\n")):
                if i > 0:
                    this_line = line.split(" ")
                    jobID = this_line[0]
                    this_line.remove(this_line[0])
                    while "" in this_line:
                        this_line.remove("")
                    #print len(this_line), len(self.keys)
                    this_job = dict(zip(self.keys, this_line))
                    if len(this_job):
                        #print i, this_job
                        jobs[jobID] = this_job
            return jobs