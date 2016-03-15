import datetime
from flask import url_for
from core import db

MAJOR_STATII = ('New','Running','Failed','Terminated','Done','Submitted')

class JobInstance(db.EmbeddedDocument):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    body = db.StringField(verbose_name="JobInstance", required=True)
    author = db.StringField(verbose_name="Name", max_length=255, required=True)
    status = db.StringField(verbose_name="status", required=False, default="New", choices=MAJOR_STATII)
    batchId = db.LongField(verbose_name="batchId", required=False, default=None)
    
class Job(db.Document):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    title = db.StringField(max_length=255, required=True)
    slug = db.StringField(max_length=255, required=True)
    body = db.StringField(required=True)
    jobInstances = db.ListField(db.EmbeddedDocumentField('JobInstance'))

    def get_absolute_url(self):
        return url_for('job', kwargs={"slug": self.slug})

    def __unicode__(self):
        return self.title

    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'slug'],
        'ordering': ['-created_at']
    }

