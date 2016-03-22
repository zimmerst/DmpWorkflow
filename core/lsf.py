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
    
    def aggregateStatii(self,asDict=True,command=["bjobs","-W"]):
        keys = "JOBID,USER,STAT,QUEUE,FROM_HOST,EXEC_HOST,JOB_NAME,\
                SUBMIT_TIME,PROJ_NAME,CPU_USED,MEM,SWAP,PIDS,START_TIME,FINISH_TIME,SLOTS".split(",")
        output = run(command)
        return output