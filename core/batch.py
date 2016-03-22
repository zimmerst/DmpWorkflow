'''
Created on Mar 22, 2016

@author: zimmer
'''
from core.shell import run
import logging

class batch(object):
    '''
    classdocs
    '''
    allJobs = {}
    keys = []
    
    def __init__(self):
        '''
        Constructor
        '''
        allJobs = self.update()

    def update(self):
        return {}
    
    def getJob(self,jobID,key="STAT",callable=str):
        if not jobID in self.allJobs:
            logging.error("could not find job %s"%jobID)
        if not key in self.keys:
            logging.error("could not extract key, allowed keys %s"%str(self.keys))
        return callable(self.allJobs[jobID][key])   

# LSF-specific stuff
class lsf(batch):
    keys = "USER,STAT,QUEUE,FROM_HOST,EXEC_HOST,JOB_NAME,"
    keys+= "SUBMIT_TIME,PROJ_NAME,CPU_USED,MEM,SWAP,PIDS,START_TIME,FINISH_TIME,SLOTS"
    keys = keys.split(",")
    
    def update(self):
        self.allJobs.update(self.aggregateStatii())
    
    def aggregateStatii(self,asDict=True,command=["bjobs -Wa"]):
        jobs = {}
        output = run(command)
        if not asDict: return output
        else:
            for i, line in enumerate(output.split("\n")):
                if i>0:
                    this_line = line.split(" ")
                    jobID = this_line[0]
                    this_line.remove(this_line[0])
                    while "" in this_line: this_line.remove("")
                    this_job = dict(zip(self.keys,this_line))
                    if len(this_job):
                        jobs[jobID]=this_job
            return jobs
