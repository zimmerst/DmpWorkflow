import datetime, time
from flask import url_for
from core import db, cfg
from bson import ObjectId
from utils.tools import random_string_generator, Ndigits
from utils.flask_helpers import parseJobXmlToDict

MAJOR_STATII = tuple(cfg.get("JobDB","task_major_statii").split(","))
FINAL_STATII = tuple(cfg.get("JobDB","task_final_statii").split(","))
TYPES = tuple(cfg.get("JobDB","task_types").split(","))
SITES = tuple(cfg.get("JobDB","batch_sites").split(","))

class JobInstance(db.EmbeddedDocument):
    _id = db.ObjectIdField( required=True, default=lambda: ObjectId()) # drop this in future.
    instanceId = db.LongField(verbose_name="instanceId", required=False, default=None)
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    body = db.StringField(verbose_name="JobInstance", required=False, default="")
    last_update = db.DateTimeField(default=datetime.datetime.now, required=True)
    batchId = db.LongField(verbose_name="batchId", required=False, default=None)
    hostname = db.StringField(verbose_name="hostname",required=False,default=None)
    status = db.StringField(verbose_name="status", required=False, default="New", choices=MAJOR_STATII)
    minor_status = db.StringField(verbose_name="minor_status", required=False, default="AwaitingBatchSubmission")
    ## store status history....
    #status_history_time = db.ListField([datetime.datetime.now])
    #status_history_time = db.ListField(["New"])
    
    def set(self,key,value):
        self.__setattr__(key,value)
        self.__setattr__("last_update",time.ctime())
    
    def setStatus(self,stat):
        #print 'calling setStatus'
        if not stat in MAJOR_STATII:
            raise Exception("status not found in supported list of statii")
        curr_status = self.status
        curr_time = time.ctime()
        if curr_status == stat:
            #print 'no status change, do nothing.'
            return
        if curr_status in FINAL_STATII:
            if not stat == 'New':
                raise Exception("job found in final state, can only set to New")
        # todo store status history
        #sHist = StatusHistory(last_update=curr_time,status=stat)
        #self.status_history.append(sHist)
        self.set("status",stat)
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
    
    execution_site = db.StringField(max_length=255, required=False, default="CNAF", choices=SITES)
    jobInstances = db.ListField(db.EmbeddedDocumentField('JobInstance'))
    
    def getBody(self,html=True):
        dd = parseJobXmlToDict(self.body)
        if not html: return dd
        if 'script' in dd:
            do = dd['script']
            if '\n' in do: do = dd['script'].replace("\n","<br/>")
            while '\n' in do:
                do = do.replace("\n","<br/>")
            dd['script']=do
        return dd
    
    def getInstance(self,_id):
        for jI in self.jobInstances:
            if str(jI.instanceId) == _id:
                return jI
        print "could not find matching id"
        return None
    
    def addInstance(self,jInst):
        if not isinstance(jInst, JobInstance):
            raise Exception("Must be job instance to be added")
        last_stream = len(self.jobInstances)
        jInst.set("instanceId",last_stream+1)
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

