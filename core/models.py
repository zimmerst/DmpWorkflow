import datetime
from flask import url_for
from core import db
from utils.tools import random_string_generator

MAJOR_STATII = ('New','Running','Failed','Terminated','Done','Submitted')
TYPES = ("Generation","Digitization","Reconstruction","User","Other")

class JobInstance(db.EmbeddedDocument):
    
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    body = db.StringField(verbose_name="JobInstance", required=False, default="")
    #    author = db.StringField(verbose_name="Name", max_length=255, required=True)
    status = db.StringField(verbose_name="status", required=False, default="New", choices=MAJOR_STATII)
    minor_status = db.StringField(verbose_name="minor_status", required=False, default="AwaitingBatchSubmission")
    batchId = db.LongField(verbose_name="batchId", required=False, default=0)
    uniqueId = db.StringField(verbose_name="uniqueId", required = False, default = random_string_generator)
    last_update = db.DateTimeField(default=datetime.datetime.now, required=True)
    
class Job(db.Document):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    title = db.StringField(max_length=255, required=True)
    slug = db.StringField(verbose_name="slug", required = True, default = random_string_generator)
    type = db.StringField(verbose_name="type", required=False, default="Other", choices=TYPES)
    release = db.StringField(max_length=255, required=False)
    body = db.StringField(required=True)
    jobInstances = db.ListField(db.EmbeddedDocumentField('JobInstance'))
    
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

