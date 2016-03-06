'''
Created on Mar 2, 2016

@author: zimmer
'''

import datetime, random
from flask import url_for
from core import db
from bson.objectid import ObjectId

VERY_LARGE_NUMBER = 100000000000

class JobInstance(db.EmbeddedDocument):
    _id = db.ObjectIdField( required=True, default=lambda: ObjectId() )
    randomSeed = db.LongField(default=random.randint(0,VERY_LARGE_NUMBER),required=True)
    def get_absolute_url(self):
        return url_for('job', kwargs={"taskName": self.taskName})
    def __unicode__(self):
        return "%s:%s"%(self.taskName,str(self._id))

class Job(db.Document):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    taskName = db.StringField(verbose_name="taskName", required=True)
    type = db.StringField(verbose_name="type",default="None",required=True,
                        choices=["Generation","Digitization","Reconstruction","None"])    
    particle = db.StringField(verbose_name="particle", required=True)
    instances = db.ListField(db.EmbeddedDocumentField('JobInstance'))
    def get_absolute_url(self):
        return url_for('job', kwargs={"taskName": self.taskName})
    def __unicode__(self):
        return self.taskName
    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'taskName'],
        'ordering': ['-created_at']
    }

