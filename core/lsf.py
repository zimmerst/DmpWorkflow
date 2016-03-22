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
        keys = "USER,STAT,QUEUE,FROM_HOST,EXEC_HOST,JOB_NAME,\
                SUBMIT_TIME,PROJ_NAME,CPU_USED,MEM,SWAP,PIDS,START_TIME,FINISH_TIME,SLOTS".split(",")
        output = run(command)
        for i, line in enumerate(output.split("\n")):
            if i>0:
                this_line = line.split(" ")
                jobID = this_line[0]
                this_line.remove(this_line[0])
                print this_line
                #jobs[this_line[0]]=dict(zip(keys,this_line[1:-1]))
        return jobs