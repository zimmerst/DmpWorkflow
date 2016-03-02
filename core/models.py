'''
Created on Mar 2, 2016

@author: zimmer
'''

import datetime, random
from flask import url_for
from core import db

VERY_LARGE_NUMBER = 100000000000

class Job(db.Document):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    release = db.StringField(verbose_name="release", required=False)
    taskName = db.StringField(verbose_name="taskName", required=True)
    particle = db.StringField(verbose_name="particle", required=True)
    type = db.StringField(verbose_name="type",default="None",required=True,
                        choices=["Generator","Digitization","Reconstruction","None"])    
    def get_absolute_url(self):
        return url_for('job', kwargs={"taskName": self.taskName})
    def __unicode__(self):
        return self.taskName
    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'taskName'],
        'ordering': ['-created_at']
    }

class JobInstance(Job):
    randomSeed = db.LongField(default=random.randint(0,VERY_LARGE_NUMBER),required=True)
    def get_absolute_url(self):
        return url_for('job', kwargs={"taskName": self.taskName})
    def __unicode__(self):
        return self._id
