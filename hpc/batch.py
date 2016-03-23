'''
Created on Mar 22, 2016

@author: zimmer
'''
from utils.shell import run
import logging

class BATCH(object):
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

