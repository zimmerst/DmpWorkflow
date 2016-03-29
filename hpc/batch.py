'''
Created on Mar 22, 2016

@author: zimmer
'''
from utils.shell import run
import logging

class BATCH(object):
    '''
    generic Batch class, all HPC-specific modules should inherit from it.
    '''
    allJobs = {}
    keys = []
    status_map = {}
    
    def __init__(self):
        '''
        Constructor
        '''
        allJobs = self.update()

    def update(self):
        return {}

    def __checkKeys__(self,key):
        if not key in self.keys:
            logging.error("could not extract key, allowed keys %s"%str(self.keys))
            raise Exception
    def getJob(self,jobID,key="STAT",callable=str):
        if not jobID in self.allJobs:
            logging.error("could not find job %s"%jobID)
        self.__checkKeys__(key)
        return callable(self.allJobs[jobID][key])   
    
    def getAttributeForAllJobs(self,attr="MEM"):
        ''' convenience function to return all values for a certain attribute '''
        key = attr
        self.__checkKeys__(key)
        ret = {}
        for jobID in self.allJobs:
            val = None
            if not self.allJobs[jobID].has_key(key):
                logging.error("could not find key %s in job %s"%(key,jobID))
            else:
                val = self.allJobs[jobID][key]
            print val
            ret[jobID]=val
        return ret
    