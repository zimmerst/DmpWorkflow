"""
Created on Mar 23, 2016

@author: zimmer
"""
import re
from DmpWorkflow.hpc.batch import BATCH, BatchJob as HPCBatchJob
from DmpWorkflow.utils.shell import run
from importlib import import_module
xml2dict= import_module("xmltodict")
# LSF-specific stuff

#raise ImportError("SGE class not supported")
BATCH_ID_ENV = ""

class BatchJob(HPCBatchJob):
    def submit(self, **kwargs):
        ''' each class MUST implement its own submission command '''
        extra = "%s" % self.extra if isinstance(self.extra, str) else None
        if isinstance(self.extra, dict):
            self.extra.update(kwargs)
            extra = "-%s %s".join([(k, v) for (k, v) in self.extra.iteritems()])

        cmd = "qsub -q %s -eo %s -R \"%s\" %s %s" % (self.queue, self.logFile,
                                                     "&&".join(self.requirements),
                                                     extra, self.command)
        output = self.__run__(cmd)
        return self.__regexId__(output)
    
    def __regexId__(self,_str):
        """ returns the batch Id using some regular expression, sge specific """
        # default: 
        bk = -1
        res = re.findall("\d+",_str)
        if len(res):
            bk = int(res[0])
        return bk

    def kill(self):
        ''' likewise, it should implement its own batch-specific removal command '''
        cmd = "qdel %s" % self.batchId
        self.__run__(cmd)
        self.update("status","Failed")


class BatchEngine(BATCH):
    kind = "sge"
    status_map = {"r": "Running", "qw": "Submitted", "s": "Suspended",
                  "EXIT": "e"}

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
        running = [j for j in self.allJobs if self.allJobs[j]['STAT']=="RUN"]
        pending = [j for j in self.allJobs if self.allJobs[j]['STAT']=="PEND"]
        return running + pending
    
    def aggregateStatii(self, asDict=True, command=None):
        if command is None:
            command = "qstat"
        jobs = {}
        uL = iL = True
        if asDict: 
            uL = iL = False
            command+=" -x -e"
        output, error, rc = run(command.split(), useLogging=uL, interleaved=iL)
        self.logging.debug("rc: %i",int(rc))
        if rc: raise Exception("error during execution")
        if error is not None:
            for e in error.split("\n"): self.logging.error(e)
        if not asDict:
            return output
        else:
            i = 0
            while not output.startswith("<"): output = output[i:-1]; i+=1
            if not output.endswith(">"): output+=">"
            output = xml2dict.parse(output)
            data = output.get("Data","None")
            if "None": 
                self.logging.error("could not get data content from qstat, check SGE")
            sge_jobs = data.get("Job","None")
            if sge_jobs == "None": sge_jobs = []
            if len(sge_jobs):
                for j in sge_jobs:
                    usr = j.get("Job_Owner","None")
                    if "@" in usr: usr = usr.rsplit("@")[0]
                    stat= j.get("job_state","U") # unknown
                    if stat.lower() not in self.status_map.keys(): stat = 'u'
                    cpu = None
                    mem = None
                    res = j.get("resources_used","None")
                    if res != "None":
                        mem = float(res.get("mem","0kb").rsplit("kb")[0])
                        cpu = res.get("cput","00:00:00.000")
                    this_job = {"USER":usr, "MEM":mem, "CPU_USED":cpu, "STAT": stat, "EXEC_HOST": j.get("exec_host")}
                    jobs[j.get("Job_Id","None").split(".")[0]]=this_job
            
