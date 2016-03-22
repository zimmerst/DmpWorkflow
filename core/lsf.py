'''
Created on Mar 22, 2016

@author: zimmer
'''
from core.shell import run

class lsf(object):
    '''
    classdocs
    '''


    def __init__(self, params):
        '''
        Constructor
        '''
        pass
    
    def aggregateStatii(self,asDict=True,command=["bjobs -W"]):
        jobs = {}
        keys = "USER,STAT,QUEUE,FROM_HOST,EXEC_HOST,JOB_NAME,"
        keys+= "SUBMIT_TIME,PROJ_NAME,CPU_USED,MEM,SWAP,PIDS,START_TIME,FINISH_TIME,SLOTS"
        keys = keys.split(",")
        output = run(command)
        if not asDict: return output
        else:
            for i, line in enumerate(output.split("\n")):
                if i>0:
                    this_line = line.split(" ")
                    jobID = this_line[0]
                    this_line.remove(this_line[0])
                    while "" in this_line: this_line.remove("")
                    this_job = dict(zip(keys,this_line))
                    if len(this_job):
                        jobs[jobID]=this_job
            return jobs