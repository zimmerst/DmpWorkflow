'''
Created on Mar 15, 2016
@author: zimmer
@brief: base class for DAMPE Workflow (HPC/client side)
'''
import os.path
from models import JobInstance
from utils.flask_helpers import parseJobXmlToDict, update_status
from utils.tools import mkdir, touch, rm
from hpc.lsf import LSF, BatchJob


# todo2: add cfg parsing variables.

class DmpJob(object):
    def __init__(self,job,**kwargs):
        self.wd = os.path.abspath(".")
        self.DBjob = job
        self.jobId = str(job.id)
        self.instanceId = None
        self.batchId = None
        self.InputFiles = []
        self.OutputFiles = []
        self.MetaData = []
        self.logfile = None
        self.executable = ""
        self.exec_wrapper = ""
        self.script = None
        self.__dict__.update(kwargs)
        self.extract_xml_metadata(job.body)
            
    def getJobName(self):
        return "-".join([self.jobId,self.instanceId])

    def extract_xml_metadata(self,xmldoc):
        ''' given the structured job definition, read out and set variables '''
        el = parseJobXmlToDict(xmldoc)
        self.InputFiles = el['InputFiles']
        self.OutputFiles = el['OutputFiles']
        self.MetaData = el['MetaData']
        self.exec_wrapper = el['script']
        self.executable = el['executable']
 
    def setInstanceParameters(self,JobInstance):
        ''' extract jobInstanceParameters to fully define job '''
        body = JobInstance.body
        self.instanceId = JobInstance.instanceId # aka stream
        keys = ['InputFiles','OutputFiles','MetaData']
        if isinstance(body,dict):
            for key in keys:
                if key in body and isinstance(body[key],list):
                    if len(body[key]):
                        self.__dict__[key]+=body[key]

    


    def write_script(self,outfile):
        ''' based on meta-data should create job-executable '''
        pass
    
    def createLogFile(self):
        mkdir(os.path.join("%s/logs"%self.wd))
        self.logfile = os.path.join("%s/logs"%self.wd,"%s.log"%self.getJobName())
        if os.path.isfile(self.logfile):
            rm(self.logfile)
        # create the logfile before submitting.
        touch(self.logfile)  
    
    def getExecCommand(self):
        return " ".join([self.executable,os.path.abspath(self.script)])
    
    def updateStatus(self,majorStatus,minorStatus):
        ''' passes status '''
        update_status(self.joibId, self.instanceId, majorStatus, minor_status=minorStatus)
                   
    def getStatusBatch(self):
        ''' interacts with the backend HPC stuff and returns the status of the job '''
        # todo: make the whole thing batch-independent!
        batch = LSF()
        ret = batch.status_map[batch.getJob(self.batchId,key="STAT")]
        return ret

    def submit(self,**kwargs):
        ''' handles the submission part '''
        self.createLogFile()
        bj = BatchJob(name=self.getJobName(),command=self.getExecCommand(),logFile=self.logfile)
        bj.submit(**kwargs)
        self.batchId = bj.get("batchId")