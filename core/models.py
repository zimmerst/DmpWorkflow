import datetime, time
from flask import url_for
from core import db, cfg
from bson import ObjectId
from utils.tools import random_string_generator
from utils.flask_helpers import parseJobXmlToDict

MAJOR_STATII = tuple(cfg.get("JobDB","task_major_statii").split(","))
TYPES = tuple(cfg.get("JobDB","task_types").split(","))
SITES = tuple(cfg.get("JobDB","batch_sites").split(","))

class JobInstance(db.EmbeddedDocument):    
    _id = db.ObjectIdField( required=True, default=lambda: ObjectId() )
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    body = db.StringField(verbose_name="JobInstance", required=False, default="")

    last_update = db.DateTimeField(default=datetime.datetime.now, required=True)
    batchId = db.LongField(verbose_name="batchId", required=False, default=None)
    hostname = db.StringField(verbose_name="hostname",required=False,default=None)
    status = db.StringField(verbose_name="status", required=False, default="New", choices=MAJOR_STATII)
    minor_status = db.StringField(verbose_name="minor_status", required=False, default="AwaitingBatchSubmission")
 
    def set(self,key,value):
        self.__setattr__(key,value)
        self.__setattr__("last_update",time.ctime())

class Job(db.Document):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    slug = db.StringField(verbose_name="slug", required = True, default = random_string_generator)

    title = db.StringField(max_length=255, required=True)
    body = db.StringField(required=True)
    type = db.StringField(verbose_name="type", required=False, default="Other", choices=TYPES)
    release = db.StringField(max_length=255, required=False)
    
    execution_site = db.StringField(max_length=255, required=False, default="CNAF", choices=SITES)
    jobInstances = db.ListField(db.EmbeddedDocumentField('JobInstance'))
    
    def getBody(self):
        return parseJobXmlToDict(self.body)
    
    def getInstance(self,_id):
        for jI in self.jobInstances:
            if str(jI._id) == _id:
                return jI
        print "could not find matching id"
        return None
    
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

    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'slug'],
        'ordering': ['-created_at']
    }

