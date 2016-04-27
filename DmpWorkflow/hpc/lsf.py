"""
Created on Mar 23, 2016

@author: zimmer
"""
from DmpWorkflow.hpc.batch import BATCH, logging, BatchJob as HPCBatchJob
from DmpWorkflow.utils.shell import run


# LSF-specific stuff

class BatchJob(HPCBatchJob):
    def submit(self, **kwargs):
        """ each class MUST implement its own submission command """
        extra = "%s" % self.extra if isinstance(self.extra, str) else None
        if isinstance(self.extra, dict):
            self.extra.update(kwargs)
            extra = "-%s %s".join([(k, v) for (k, v) in self.extra.iteritems()])
        extra+= " -W \"%s\" "%self.cputime
        if isinstance(self.requirements,str): self.requirements.split(",")
        req = "-R \"%s\""%"&&".join(self.requirements+["rusage[mem=%i"%int(self.memory)])
        cmd = "bsub -q {0} -eo {1} {2} {3} {4}".format(self.queue, self.logFile, req, extra, self.command)    
        print cmd
        self.__execWithUpdate__(cmd, "batchId")

    def kill(self):
        """ likewise, it should implement its own batch-specific removal command """
        cmd = "bkill %s" % self.batchId
        self.__execWithUpdate__(cmd, "status", value="Failed")


class BatchEngine(BATCH):
    kind = "lsf"
    keys = "USER,STAT,QUEUE,FROM_HOST,EXEC_HOST,JOB_NAME,"
    keys += "SUBMIT_TIME,PROJ_NAME,CPU_USED,MEM,SWAP,PIDS,START_TIME,FINISH_TIME,SLOTS"
    keys = keys.split(",")
    status_map = {"RUN": "Running", "PEND": "Submitted", "SSUSP": "Suspended",
                  "EXIT": "Failed", "DONE": "Completed"}

    def update(self):
        self.allJobs.update(self.aggregateStatii())

    def aggregateStatii(self, asDict=True, command=None):
        if command is None:
            command = ["bjobs -Wa"]
        jobs = {}
        output, error, rc = run(command)
        logging.debug("rc: i",int(rc))
        if error is not None:
            print error.split("\n")
            for e in error.split("\n"): 
                if len(e): logging.error(e)
        
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
                    this_job = dict(zip(self.keys, this_line))
                    if len(this_job):
                        jobs[jobID] = this_job
            return jobs
