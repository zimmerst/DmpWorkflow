"""
Created on Mar 15, 2016
@author: zimmer
@brief: base class for DAMPE Workflow (HPC/client side)
"""

import os.path as oPath
from os import environ
from jsonpickle import encode as Jencode, decode as Jdecode
from json import dumps
from requests import post as Rpost
from importlib import import_module
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, DAMPE_WORKFLOW_ROOT, BATCH_DEFAULTS, cfg
from DmpWorkflow.utils.tools import mkdir, touch, rm, safe_copy, parseJobXmlToDict, getSixDigits, ResourceMonitor
from DmpWorkflow.utils.shell import run, make_executable#, source_bash
HPC = import_module("DmpWorkflow.hpc.%s"%BATCH_DEFAULTS['system'])
PYTHONBIN = ""
ExtScript = cfg.get("site","ExternalsScript")

# todo2: add cfg parsing variables.
class DmpJob(object):
    def __init__(self, job_id, body=None, **kwargs):
        self.wd = oPath.abspath(".")
        self.title = None
        self.jobId = str(job_id)
        self.instanceId = None
        self.batchId = None
        self.InputFiles = []
        self.OutputFiles = []
        self.MetaData = []
        self.type = None
        self.release = None
        self.logfile = None
        self.execCommand = None
        self.executable = ""
        self.exec_wrapper = ""
        self.script = None
        self.__dict__.update(kwargs)
        self.extract_xml_metadata(body)
        self.__updateEnv__()
    
    def registerDS(self,filename=None):
        site = cfg.get("site","name")
        if filename is None: 
            files = [fi['target'] for fi in self.OutputFiles]
        else:
            files = [filename]
        for fi in files:
            tg = oPath.expandvars(fi)     
            res = Rpost("%s/datacat/" % DAMPE_WORKFLOW_URL, data = {"filename":tg, "site":site , "action":"register"})
            res.raise_for_status()
            if not res.json().get("result", "nok") == "ok":
                raise Exception(res.json().get("error","No error provided."))
            
    def getWorkDir(self):
        wdROOT = cfg.get("site","workdir")
        wd = oPath.join(wdROOT,str(self.title),self.getSixDigits(asPath=True))
        return wd

    def __updateEnv__(self):
        override_keys = ["BATCH_OVERRIDE_%s"%key.upper() for key in BATCH_DEFAULTS.keys()]
        for var in self.MetaData:
            if var['name'] in override_keys:
                bkey = var['name'].replace("BATCH_OVERRIDE_","").lower()
                BATCH_DEFAULTS[bkey] = var['value']
            else:
                environ[var['name']]=var['value']
        for fil in self.InputFiles + self.OutputFiles:
            for key in ['source','target']:
                fil[key]=oPath.expandvars(fil[key])

        if self.title is not None:
            environ['DWF_TASKNAME']=self.title
        if self.release is not None:
            environ['RELEASE_TAG']=self.release
        #print 'BatchOverride keys', BATCH_DEFAULTS
        return      

    def getJobName(self):
        return "-".join([str(self.jobId), self.getSixDigits()])

    def extract_xml_metadata(self, xmldoc):
        """ given the structured job definition, read out and set variables """
        if xmldoc is None: return
        el = parseJobXmlToDict(xmldoc)
        self.setBodyFromDict(el)

    def setBodyFromDict(self,el):
        self.InputFiles  += el['InputFiles']
        self.OutputFiles += el['OutputFiles']
        self.MetaData    += el['MetaData']
        self.exec_wrapper = el['script']
        self.executable   = el['executable']
        self.__dict__.update(el['atts'])
        
    def setInstanceParameters(self, instance_id, JobInstance_body):
        """ extract jobInstanceParameters to fully define job """
        body = JobInstance_body
        self.instanceId = instance_id  # aka stream
        keys = ['InputFiles', 'OutputFiles', 'MetaData']
        if isinstance(body, dict):
            for key in keys:
                if key in body and isinstance(body[key], list):
                    if len(body[key]):
                        self.__dict__[key] += body[key]

    def write_script(self,pythonbin=None,debug=False):
        """ based on meta-data should create job-executable """
        if pythonbin is None:
            pythonbin = "python"
        #print pythonbin
        self.wd = self.getWorkDir()
        if oPath.isdir(self.wd):
            rm(self.wd)
        mkdir(self.wd)
        safe_copy(oPath.join(DAMPE_WORKFLOW_ROOT, "scripts/dampe_execute_payload.py"),
                  oPath.join(self.wd, "script.py"), debug=debug)
        with open(oPath.join(self.wd, "job.json"), "wb") as json_file:
            json_file.write(self.exportToJSON())
        jsonLOC = oPath.abspath(oPath.join(self.wd, "job.json"))
        script_file = open(oPath.join(self.wd,"script"),"w")
        rel_path = self.getReleasePath()
        setup_script = self.getSetupScript()
        setup_script = setup_script.replace(rel_path,"")
        if setup_script.startswith("/"):
            setup_script = setup_script.replace("/","")
        cmds = ["#!/bin/bash","echo \"batch wrapper executing on $(date)\"",\
                "source %s"%oPath.expandvars(ExtScript),\
                "unset DMPSWSYS",\
                "cd %s"%rel_path,\
                "source %s"%setup_script,\
                "cd %s"%self.wd,\
                "%s script.py %s"%(pythonbin,jsonLOC),\
                "echo \"batch wrapper completed at $(date)\""]
        script_file.write("\n".join(cmds))
        script_file.close()
        make_executable(oPath.join(self.wd,"script"))
        self.execCommand = "%s/script"%self.wd
        return

    def getSetupScript(self):
        rpath = self.getReleasePath()
        return oPath.expandvars("%sbin/thisdmpsw.sh" % rpath)
    
    def getReleasePath(self):
        return oPath.expandvars("${DAMPE_SW_DIR}/releases/DmpSoftware-%s/" % self.release)

    #def sourceSetupScript(self):
    #    src = self.getSetupScript()
    #    source_bash(src)

    def createLogFile(self):
        #mkdir(oPath.join("%s/logs" % self.wd))
        self.logfile = oPath.join(self.wd, "output.log")
        if oPath.isfile(self.logfile): rm(self.logfile)
        touch(self.logfile)

    def updateStatus(self, majorStatus, minorStatus, **kwargs):
        """ passes status """
        my_dict = {"t_id": self.jobId, "inst_id": self.instanceId, "major_status": majorStatus,
                   "minor_status": minorStatus}
        if 'resources' in kwargs:
            if kwargs['resources']:
                RM = kwargs['resources']
                if not isinstance(RM,ResourceMonitor):
                    raise Exception("resource must be of type resource monitor")
                my_dict['memory'] = RM.getMemory(unit='Mb')
                my_dict['cpu'] = RM.getCpuTime()
                del kwargs['resources']
        my_dict.update(kwargs)
        #print '*DEBUG* my_dict: %s'%str(my_dict)
        res = Rpost("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args": dumps(my_dict)})
        res.raise_for_status()
        if not res.json().get("result", "nok") == "ok":
            raise Exception(res.json().get("error","ErrorMissing"))
        if majorStatus in ["Done","Failed","Terminated"]:
            witness = open(oPath.join(self.wd,"%s"%majorStatus.upper()),'w')
            witness.write(self.getJobName())
            witness.close()
        return
        # update_status(self.jobId, self.instanceId, majorStatus, minor_status=minorStatus, **kwargs)

    def getStatusBatch(self):
        """ interacts with the backend HPC stuff and returns the status of the job """
        # todo: make the whole thing batch-independent!
        batch = HPC.BatchEngine()
        ret = batch.status_map[batch.getJob(self.batchId, key="STAT")]
        return ret

    def submit(self, **kwargs):
        """ handles the submission part """
        #print "batchdefaults: ",BATCH_DEFAULTS
        dry = kwargs['dry'] if 'dry' in kwargs else False
        local = kwargs['local'] if 'local' in kwargs else False
        if not dry: 
            self.createLogFile()
        
        bj = HPC.BatchJob(name=self.getJobName(), command=self.execCommand, logFile=self.logfile, defaults = BATCH_DEFAULTS)
        if dry:
            print "DRY_COMMAND: %s"%self.execCommand
            return -1
        if local: 
            run(["%s &> %s"%(self.execCommand,self.logfile)])
            self.batchId = -1
        else:
            self.batchId = bj.submit(**kwargs)
        return self.batchId

    def kill(self,msg="ReceivedKillCommand",dry=False):
        """ handles the submission part """
        #print BATCH_DEFAULTS
        if not dry: 
            self.createLogFile()
        bj = HPC.BatchJob(batchId=self.batchId)
        if dry:
            print "DRY_COMMAND: bkill %s"%self.batchId
            return -1
        else:
            bj.kill()
            self.updateStatus("Terminated", msg)

    def exportToJSON(self):
        """ return a pickler of itself as JSON format """
        return Jencode(self)

    def getSixDigits(self, asPath=False):
        return getSixDigits(self.instanceId, asPath=asPath)

    @classmethod
    def fromJSON(cls, jsonstr):
        kc = Jdecode(jsonstr)
        kc.__updateEnv__()
        return kc
