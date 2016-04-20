import datetime, time, os, sys, logging
from flask import url_for
from core import db, cfg
from bson import ObjectId
from utils.tools import random_string_generator, Ndigits, exceptionHandler
from utils.flask_helpers import parseJobXmlToDict

MAJOR_STATII = tuple(cfg.get("JobDB","task_major_statii").split(","))
FINAL_STATII = tuple(cfg.get("JobDB","task_final_statii").split(","))
TYPES = tuple(cfg.get("JobDB","task_types").split(","))
SITES = tuple(cfg.get("JobDB","batch_sites").split(","))

if not cfg.getboolean("site","traceback"):
    sys.excepthook = exceptionHandler

log = logging.getLogger()

class JobInstance(db.EmbeddedDocument):
    _id = db.ObjectIdField( required=True, default=lambda: ObjectId()) # drop this in future.
    instanceId = db.LongField(verbose_name="instanceId", required=False, default=None)
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    body = db.StringField(verbose_name="JobInstance", required=False, default="")
    last_update = db.DateTimeField(default=datetime.datetime.now, required=True)
    batchId = db.LongField(verbose_name="batchId", required=False, default=None)
    Nevents = db.LongField(verbose_name="Nevents", required=False, default=None)
    site = db.StringField(verbose_name="site", required=False, default="CNAF")
    hostname = db.StringField(verbose_name="hostname",required=False,default=None)
    status = db.StringField(verbose_name="status", required=False, default="New", choices=MAJOR_STATII)
    minor_status = db.StringField(verbose_name="minor_status", required=False, default="AwaitingBatchSubmission")
    status_history = db.ListField()
    log = db.StringField(verbose_name="log", required=False, default="")
    
    def getLog(self):
        lines = self.log.split("\n")
        return lines
    
    def set(self,key,value):
        self.__setattr__(key,value)
        self.__setattr__("last_update",time.ctime())
    
    def setStatus(self,stat):
        if not stat in MAJOR_STATII:
            raise Exception("status not found in supported list of statii")
        curr_status = self.status
        curr_time = time.ctime()
        self.last_update = curr_time
        if curr_status == stat and self.minor_status == self.status_history[-1]['minor_status']:
            return
        if curr_status in FINAL_STATII:
            if not stat == 'New':
                raise Exception("job found in final state, can only set to New")
        self.set("status",stat)
        sH = {"status":self.status,"update":self.last_update,"minor_status":self.minor_status}
        self.status_history.append(sH)
        return
    
    def sixDigit(self,size=6):
        return Ndigits(self.instanceId, size=size)

class Job(db.Document):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    slug = db.StringField(verbose_name="slug", required = True, default = random_string_generator)
    title = db.StringField(max_length=255, required=True)
    body = db.StringField(required=True)
    type = db.StringField(verbose_name="type", required=False, default="Other", choices=TYPES)
    release = db.StringField(max_length=255, required=False)
    dependencies = db.ListField(db.ObjectIdField)
    execution_site = db.StringField(max_length=255, required=False, default="CNAF", choices=SITES)
    jobInstances = db.ListField(db.EmbeddedDocumentField('JobInstance'))
 
    def addDependency(self,job):
        if not isinstance(job, Job):
            raise Exception("Must be job to be added")
        self.dependencies.append(job._id)
    
    def getDependency(self):
        if not len(self.dependencies):
            return "None"
        else: 
            return tuple(self.dependencies)
    
    def getSite(self):
        my_site = self.execution_site
        for jI in self.jobInstances:
            if jI.site!=my_site:
                my_site="Mixed"
        return my_site

    def getNevents(self):
        log.warning("FIXME: need to implement fast query")
        return "NaN"
    
    def getBody(self):
        os.environ["DWF_JOBNAME"]=self.title
        return parseJobXmlToDict(self.body)
     
    def getInstance(self,_id):
        for jI in self.jobInstances:
            if long(jI.instanceId) == long(_id):
                return jI
        print "could not find matching id"
        return None
    
    def addInstance(self,jInst):
        if not isinstance(jInst, JobInstance):
            raise Exception("Must be job instance to be added")
        last_stream = len(self.jobInstances)
        jInst.set("instanceId",last_stream+1)
        if not len(jInst.status_history):
            sH = {"status":jInst.status,"update":jInst.last_update,"minor_status":jInst.minor_status}
            jInst.status_history.append(sH)
        self.jobInstances.append(jInst)
    
    def aggregateStatii(self):
        ''' will return an aggregated summary of all instances in all statuses '''
        counting_dict = dict(zip(MAJOR_STATII,[0 for m in MAJOR_STATII]))
        for jI in self.jobInstances:
            if not jI.status in MAJOR_STATII: raise Exception("Instance found in status not known to system")
            counting_dict[jI.status]+=1
        return [(k,counting_dict[k]) for k in MAJOR_STATII]
        #return counting_dict
                    
    def get_absolute_url(self):
        return url_for('job', kwargs={"slug": self.slug})

    def __unicode__(self):
        return self.title
    
    def save(self):
        req = Job.objects.filter(title=self.title)
        if req: raise Exception("a task with the specified name exists already.")
        super(db.Document,self).save()

    def update(self):
        super(db.Document,self).save()

    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'slug'],
        'ordering': ['-created_at']
    }

