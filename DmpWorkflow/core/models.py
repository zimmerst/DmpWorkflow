# pylint: disable=E1002
import logging
from datetime import datetime
import sys
from mongoengine import CASCADE
from copy import deepcopy
from flask import url_for
from ast import literal_eval
#from StringIO import StringIO
from DmpWorkflow.config.defaults import cfg, MAJOR_STATII, FINAL_STATII, TYPES, SITES
from DmpWorkflow.core import db
from DmpWorkflow.utils.tools import random_string_generator, exceptionHandler
from DmpWorkflow.utils.tools import parseJobXmlToDict, convertHHMMtoSec, sortTimeStampList
from DmpWorkflow.core.datacat import DataFile, DataReplica, DataSet # flask may need this...
if not cfg.getboolean("site", "traceback"): sys.excepthook = exceptionHandler
log = logging.getLogger("core")


class HeartBeat(db.Document):
    ''' dummy class to test DB connection from remote workers '''    
    created_at = db.DateTimeField(default=datetime.now, required=True)
    timestamp = db.DateTimeField(verbose_name="timestamp",required=True)
    hostname = db.StringField(max_length=255, required=False)
    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'hostname'],
        'ordering': ['-created_at']
    }    

class Job(db.Document):
    created_at = db.DateTimeField(default=datetime.now, required=True)
    slug = db.StringField(verbose_name="slug", required=True, default=random_string_generator)
    title = db.StringField(max_length=255, required=True)
    body = db.FileField()
    type = db.StringField(verbose_name="type", required=False, default="Other", choices=TYPES)
    release = db.StringField(max_length=255, required=False)
    dependencies = db.ListField(db.ReferenceField("Job"))
    execution_site = db.StringField(max_length=255, required=True, default="local", choices=SITES)
    jobInstances = db.ListField(db.ReferenceField("JobInstance"))
    archived = db.BooleanField(verbose_name="task closed", required=False, default=False)
    comment = db.StringField(max_length=1024, required=False, default="N/A")

    def addDependency(self, job):
        if not isinstance(job, Job):
            raise Exception("Must be job to be added")
        self.dependencies.append(job)
    
    def archiveJob(self):
        self.archived = True

    def evalBody(self):
        evalKeys = ['InputFiles','OutputFiles','MetaData']
        meta = {}
        jobBody = self.getBody()
        for k in evalKeys:  
            assert k in jobBody.keys(), "error, missing key %s in job body"%k
            meta[k]=jobBody[k]
        return meta
    
    def getOutputFiles(self):
        m = self.evalBody()
        out = [v['target'] for v in m['OutputFiles']]
        return out
    
    def getInputFiles(self):
        m = self.evalBody()
        out = [v['source'] for v in m['InputFiles']]
        return out

    def getMetaDataVariables(self):
        m = self.evalBody()
        out = {}
        for v in m['MetaData']: out.update({v['name']:v['value']})
        return out

    def getData(self):
        return self._data

    def getDependency(self, pretty=False):
        if not len(self.dependencies):
            return ()
        else:
            if pretty:
                return tuple([d.slug for d in self.dependencies])
            else: 
                return tuple(self.dependencies) 

    def getNevents(self):
        return self.getNeventsFast()

    def getNeventsFast(self):
        return JobInstance.objects.filter(job=self).aggregate_sum("Nevents")

    def getBody(self):
        # os.environ["DWF_JOBNAME"] = self.title
        bdy = deepcopy(self.body.get().read())
        self.body.get().seek(0)
        #bdy_file = StringIO(deepcopy(bdy))
        #self.body.delete()
        #self.body.put(bdy_file,content_type="application/xml")
        #self.update()
        return parseJobXmlToDict(bdy)
    
    def resetBody(self,body,content_type="application/xml"):
        self.body.replace(open(body,"rb"), content_type=content_type)
        self.save()                        

    def getInstance(self, _id):
        jI = JobInstance.objects.filter(job=self, instanceId=_id)
        log.debug("jobInstances from query: %s",str(jI))
        if jI.count(): return jI.first()
        log.exception("could not find matching id")             
        return None

    def addInstance(self, jInst, inst=None):
        if self.archived:
            raise Exception("cannot append new instances to job that is archived, must unlock first.")
        if len(self.jobInstances)>=1000000:
            raise Exception("reached maximum of job instances, consider cloning this job instead.")
        if not isinstance(jInst, JobInstance):
            log.exception("must be job instance to be added")
            raise Exception("Must be job instance to be added")
        last_stream = len(self.jobInstances)
        if inst is not None:
            # FIXME: offsets one, but then goes back to the length counter.
            last_stream = inst - 1
            if self.getInstance(last_stream + 1):
                log.exception("job with instance %i exists already", inst)
                raise Exception("job with instance %i exists already" % inst)
        jInst.instanceId = last_stream + 1
        if not len(jInst.status_history):
            sH = {"status": jInst.status, "update": jInst.last_update, "minor_status": jInst.minor_status}
            jInst.status_history.append(sH)
        jInst.job = self # add self reference?
        #jInst.getResourcesFromMetadata()
        jInst.save()
        self.jobInstances.append(jInst)

    def aggregateStatii(self, asdict=False):
        # just an alias
        """ will return an aggregated summary of all instances in all statuses """
        return self.aggregateStatiiFast(asdict=asdict)

    def aggregateStatiiFast(self, asdict=False):
        """ will return an aggregated summary of all instances in all statuses """
        counting_dict = {unicode(key):0 for key in MAJOR_STATII}
        counting_dict.update(JobInstance.objects.filter(job=self).item_frequencies("status"))
        if asdict: return counting_dict
        else: return [(key, value) for key, value in counting_dict.iteritems()]

    def countInstances(self):
        return JobInstance.objects.filter(job=self).count()

    def get_absolute_url(self):
        return url_for('job', kwargs={"slug": self.slug})

    def __unicode__(self):
        return self.title
    
    def delete(self):
        instances = JobInstance.objects.filter(job=self)
        self.body.delete()
        if len(instances):
            for ji in instances: ji.delete()
        super(Job,self).delete()

    #    def save(self):
    #        req = Job.objects.filter(title=self.title, type=self.type)
    #        if req:
    #            raise Exception("a task with the specified name & type exists already.")
    #        super(Job, self).save()

    def update(self):
        log.warning("deprecated method, use save")
        super(Job, self).save()

    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'slug', 'title', 'id', 'execution_site'],
        'ordering': ['-created_at']
    }

class JobInstance(db.Document):
    instanceId = db.LongField(verbose_name="instanceId", required=False, default=None)
    created_at = db.DateTimeField(default=datetime.now, required=True)
    body = db.StringField(verbose_name="JobInstance", required=False, default="")
    last_update = db.DateTimeField(default=datetime.now, required=True)
    batchId = db.LongField(verbose_name="batchId", required=False, default=None)
    Nevents = db.LongField(verbose_name="Nevents", required=False, default=0)
    job = db.ReferenceField("Job", reverse_delete_rule=CASCADE)
    site = db.StringField(verbose_name="site", required=True, choices=SITES)
    hostname = db.StringField(verbose_name="hostname", required=False, default=None)
    status = db.StringField(verbose_name="status", required=False, default="New", choices=MAJOR_STATII)
    minor_status = db.StringField(verbose_name="minor_status", required=False, default="AwaitingBatchSubmission")
    status_history = db.ListField(db.DictField())
    memory = db.ListField(db.DictField())
    cpu = db.ListField(db.DictField())
    log = db.StringField(verbose_name="log", required=False, default="")
    cpu_max = db.FloatField(verbose_name="maximal CPU time (seconds)",required=False, default= -1.)
    mem_max = db.FloatField(verbose_name="maximal memory (mb)",required=False, default= -1.)
    
    def setBody(self,bdy):
        self.body = str(bdy)
        self.update()

    def __evalBody(self,includeParent=False):
        evalKeys = ['InputFiles','OutputFiles','MetaData']
        meta = {}
        if includeParent: 
            meta.update(self.job.evalBody())
        inst_body = literal_eval(self.body)
        if not isinstance(inst_body, dict):
            raise Exception("Error in parsing body of JobInstance, not of type DICT")
        if len(inst_body):
            for k in evalKeys:
                assert k in inst_body.keys(), "error, missing key %s in instance body"%k 
                if len(inst_body[k]):
                    meta[k]+=inst_body[k]
        return meta
    
    def getOutputFiles(self,includeJob=True):
        m = self.__evalBody(includeParent=includeJob)
        out = []
        if len(m): out = [v['target'] for v in m['OutputFiles']]
        return out
    
    def getInputFiles(self,includeJob=True):
        m = self.__evalBody(includeParent=includeJob)
        out = []
        if len(m): out = [v['source'] for v in m['InputFiles']]
        return out

    def getMetaDataVariables(self,includeJob=True):
        m = self.__evalBody(includeParent=includeJob)
        out = {}
        if len(m): 
            for v in m['MetaData']: out.update({v['name']:v['value']})
        return out
    
    def setMetaDataVariablesFromDict(self,_dict):
        if not isinstance(_dict,dict): raise Exception("Must be a dictionary!")
        bdy = literal_eval(self.body)
        for k, v in _dict.iteritems(): bdy['MetaData'].append({'value':v, 'name':k, 'type':'str'})
        self.set("body",str(bdy))
    
    def getResourcesFromMetadata(self):
        md = []
        res = {"BATCH_OVERRIDE_CPUTIME":self.cpu_max, "BATCH_OVERRIDE_MEMORY": self.mem_max}
        var_map = {"BATCH_OVERRIDE_CPUTIME":"cpu_max", "BATCH_OVERRIDE_MEMORY": "mem_max"}
        metadata = self.job.getBody()
        if isinstance(metadata,dict): 
            if 'MetaData' in metadata: md = metadata['MetaData']
        if self.body != "":
            instance_dict = literal_eval(self.body)
            if 'MetaData' in instance_dict:
                md+=instance_dict['MetaData'] 
        # next, set the values
        for v in md:
            if v['name'] in var_map:
                val = v['value']
                if ":" in val: val = convertHHMMtoSec(val)
                res[v['name']]=float(val)
        for k,v in var_map.iteritems():
            self.set(v,res[k])            
        return 
    
    def getWallTime(self,unit='sec'):
        if self.status not in FINAL_STATII:
            log.warning("job not find in final status, CPU time may not be accurate")
        dt1 = self.status_history[0]['update']
        dt2 = self.status_history[1]['update']
        total_sec = (dt2 - dt1).total_seconds()
        if unit == "min": return float(total_sec)/60.
        elif unit == "hrs": return float(total_sec)/3600.
        else: 
            if unit != "s":
                log.warning("unsupported unit, returning seconds")
            return total_sec

    def getCpuTime(self,unit='sec'):
        if self.status not in FINAL_STATII:
            log.warning("job not find in final status, CPU time may not be accurate")
        total_sec = self.cpu[-1]['value']
        if unit == "min": return float(total_sec)/60.
        elif unit == "hrs": return float(total_sec)/3600.
        else: 
            if unit != "s":
                log.warning("unsupported unit, returning seconds")
            return total_sec
        
    def getEfficiency(self):
        cpt = self.getCpuTime()
        wct = self.getWallTime()
        eff = cpt/wct    
        return eff
    
    def getMemory(self,method='average'):
        """ get memory of job in Mb """
        if self.status not in FINAL_STATII: 
            log.warning("job not find in final status, result may not be accurate")
        assert method in ['average','min','max'], "method not supported"
        all_memory = [float(v["value"]) for v in self.memory]
        if method == 'min':
            return min(all_memory)
        elif method == 'max':
            return max(all_memory)
        else:
            return sum(all_memory)/float(len(all_memory))
        
    def checkDependencies(self,check_status=u"Done"):
        dependent_tasks = self.job.getDependency()
        isReady = True
        for task in dependent_tasks:
            inst = task.getInstance(self.instanceId)
            if inst.status != check_status: isReady = False        
        return isReady

#    def parseBodyXml(self,key="MetaData"):
#        p = parseJobXmlToDict(self.job.body.read())
#        return p[key]

    def getLog(self):
        lines = self.log.split("\n")
        return lines

    def get(self,key):
        if key in ['cpu_max','mem_max']:
            if key not in self._data.keys(): return 0.
            else:
                return float(self._data.get(key))                
        elif key == 'cpu':
            # -1 doesn't appear to be a valid key
            if not len(self.cpu): return 0.
            index = len(self.cpu)-1
            if index<0: index=0
            return self.cpu[index]['value']
        elif key == 'memory':
            if not len(self.memory): return 0.
            index = len(self.memory)-1
            if index<0: index=0
            return self.memory[index]['value']
        else:
            return 0.
        
    def set(self, key, value):
        if key == "created_at" and value == "Now": 
            value = datetime.now()
        elif key == 'cpu':
            self.cpu.append({"time":datetime.now(),"value":value})
        elif key == 'memory':
            self.memory.append({"time":datetime.now(),"value":value})
        elif key in ['cpu_max','mem_max']:
            self._data.__setitem__(key,value)
        else:
            self.__setattr__(key, value)
        log.debug("setting %s : %s",key,value)
        self.__setattr__("last_update", datetime.now())
        self.update()

    def setStatus(self, stat):
        log.debug("calling JobInstance.setStatus")
        if stat not in MAJOR_STATII:
            raise Exception("status not found in supported list of statii: %s", stat)
        curr_status = self.status
        curr_time = datetime.now()
        self.last_update = curr_time
        if curr_status == stat and self.minor_status == self.status_history[-1]['minor_status']:
            return
        if curr_status in FINAL_STATII:
            if not stat == 'New':
                raise Exception("job found in final state, can only set to New")
        self.last_update = self.last_update
        self.set("status", stat)
        sH = {"status": self.status, 
              "update": self.last_update,
              "minor_status": self.minor_status}
        log.debug("statusSet %s",str(sH))
        self.status_history.append(sH)
        if curr_status in FINAL_STATII: self.__sortTimeStampedLists()
        self.update()
        return

    def __sortTimeStampedLists(self):
        # final step - sort time-stamped lists to be chronological
        if len(self.status_history)>1:
            self.status_history = sortTimeStampList(self.status_history, timestamp = "update")
        if len(self.cpu)>1:
            self.cpu = sortTimeStampList(self.cpu)
        if len(self.memory)>1:
            self.memory = sortTimeStampList(self.memory)
        return 

    def sixDigit(self, size=6):
        return str(self.instanceId).zfill(size)

    def update(self):
        log.debug("calling update on JobInstance")
        super(JobInstance, self).save()

    def save(self):
        req = JobInstance.objects.filter(job=self.job,instanceId=self.instanceId)
        if req:
            raise Exception("instance exists already.")
        super(JobInstance, self).save()

        
    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'instanceId', 'site'],
        'ordering': ['-created_at']
    }

##### DATA CATALOG MODELS ####

fileStatii = ("New","Orphaned","Bad","Good")


class DataReplica(db.Document):
    site     = db.StringField(max_length=24, required=True)
    status   = db.StringField(max_length=16, default="New",choices=fileStatii)
    path     = db.StringField(max_length=1024, required=True)
    CheckSum = db.StringField(max_length=72, required=False)
    DataFile = db.ReferenceField("DataFile", reverse_delete_rule=CASCADE)

    meta     = {
                 'allow_inheritance': True,
                 'indexes': ['-created_at', 'status', 'site'],
                 'ordering': ['-created_at']
               }

    def update(self,**kwargs):
        self.site = kwargs.get("site",cfg.get("site", "name"))
        self.status = kwargs.get("status","new")
        path = kwargs.get("path",None)
        if path is None: return
        self.path = path
        self.save()

        
    def getFileName(self):
        return op_join(self.path,self.DataFile.filename)

class DataFile(db.Document):
    created_at = db.DateTimeField(default=datetime.now, required=True)
    dataset = db.ReferenceField("DataSet", reverse_delete_rule=CASCADE)
    replicas   = db.ListField(db.ReferenceField("DataReplica")) 
    filename= db.StringField(max_length=1024, required=True)
    filetype = db.StringField(max_length=16, required=False, default="root")
    origin = db.ReferenceField("DataReplica")
    # here are attributes specific to the datafile
    TStart = db.DateTimeField(required=False)
    TStop  = db.DateTimeField(required=False)
    GTI = db.FloatField(required=False)

    def verifyReplicas(self):
        cs_origin = self.origin.CheckSum
        for rep in DataReplica.objects.filter(DataFile=self,status="New"):
            if rep != self.origin: 
                cs_replica = rep.CheckSum
                if cs_replica!=cs_origin: 
                    log.warning("CheckSum failed for replica of file %s at %s",self.filename, rep.site)
                    rep.status("Bad")
                else: rep.status("Good")
                rep.save()
    
    def declareOrigin(self,replica):
        if not isinstance(replica,DataReplica): raise Exception("must be a replica instance")
        self.origin = replica
        if not self.origin in self.replicas:
            log.warning("could not find replica marked as origin in list of replicas, registering a new one.")
            self.origin.DataFile = self
            self.origin.save()
            self.replicas.append(self.origin)
        self.save()
        
    def declareOriginFromDict(self,_dict):
        """ create the replica on the fly, from a dictionary alone
            required items in dictionary:
            path : full path to source
            checksum: checksum at source
            site : site name
        """
        path = _dict.get("path",None)
        checksum = _dict.get("checksum",None)
        site = _dict.get("site",None)
        if path is None:        log.error("path missing")
        elif checksum is None:  log.error("checksum missing")
        elif site is None:      log.error("site missing")
        origin = None
        try:
            origin = DataReplica.objects.filter(site=site,path=path,status="Good",DataFile=self,CheckSum=checksum)
        except DataReplica.DoesNotExist:
            log.warning("registering new replica as origin")
            origin = DataReplica(site=site,path=path,status="Good",DataFile=self,CheckSum=checksum)
            origin.save()
            
    def registerReplica(self,**kwargs):
        site = kwargs.get("site",cfg.get("site", "name"))
        status = kwargs.get("status","new")
        path = kwargs.get("path",None)
        if path is None:
            log.error("trying to register empty replica, must provide path")
            return
        force = bool(kwargs.get("force","false"))
        try:
            replica = DataReplica.objects.get(site=site, DataFile=self)
            if force: replica.update(site=site,status=status,path=path)
            log.warning("trying to re-register an existing replica")
        except DataReplica.DoesNotExist:
            replica = DataReplica(site=site,status=status,DataFile=self,path=path)
            replica.save()
        return replica 
    
    def removeReplica(self,**kwargs):
        site = kwargs.get("site",cfg.get("site", "name")) 
        try:
            replica = DataReplica.objects.get(site=site, DataFile=self)
            replica.delete()
        except DataReplica.DoesNotExist:
            log.error("requested replica does not exist")
        return
    
    def updateReplicaStatus(self,**kwargs):
        site = kwargs.get("site",cfg.get("site", "name")) 
        status = kwargs.get("status",None)
        if status is None:
            log.error("must supply a status")
            return
        try:
            replica = DataReplica.objects.get(site=site, DataFile=self)
            replica.status = status
            replica.save()
        except DataReplica.DoesNotExist:
            log.error("requested replica does not exist")
            return
        
    meta = {
            'allow_inheritance': True,
            'indexes': ['-created_at', 'filename', 'site', 'filetype'],
            'ordering': ['-created_at']
            }

class DataSet(db.Document):
    created_at = db.DateTimeField(default=datetime.now, required=True)
    files = db.ListField(db.ReferenceField("DataFile"))
    release = db.StringField(max_length=64, required=False)
    name = db.StringField(max_length=128,required=True)
    FileType = db.StringField(max_length=16, required=False, default="root")
    DataType = db.StringField(max_length=4, required=True, default="USR", choices=("USR","MC","OBS","BT"))
    DataClass = db.StringField(max_length=4, required=False, default="2A")
    
    def findDataFile(self,register=True,**kwargs):
        defaultTime="20000101000000" # January 1st 2000, 00:00:00
        FileName = kwargs.get("FileName",None)
        if FileName is None: 
            log.error("must provide at least a file name!")
            return
        TStart = datetime.strptime(kwargs.get("TStart",defaultTime),"%Y%m%d%H%M%S")
        TStop =  datetime.strptime(kwargs.get("TStop",defaultTime),"%Y%m%d%H%M%S")
        Gti   = float(kwargs.get("Gti",0.))
        FileType = kwargs.get("FileType",None)
        ds = None
        try:
            ds = DataFile.objects.filter(filename=FileName,TStart=TStart,TStop=TStop,GTI=Gti)
        except DataFile.DoesNotExist:
            if register:
                ds = DataFile(filename=FileName,TStart=TStart,TStop=TStop,GTI=Gti)
                ds.save()
        return ds
    
    
    meta = {
            'allow_inheritance': True,
            'indexes': ['-created_at', 'release', 'name'],
            'ordering': ['-created_at']
            }
####### FIXME: separate DC from Workflow! ######

