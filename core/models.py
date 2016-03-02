'''
Created on Mar 1, 2016

@author: zimmer
'''
import datetime
from flask import url_for
from core import db

class Job(db.Document):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    taskName = db.StringField(verbose_name="taskName", required=True)
    release = db.StringField(verbose_name="release", required=False)
    type = db.StringField(verbose_name="type", required=True)
    particle = db.StringField(verbose_name="particle", required=True)
     = db.StringField(verbose_name="particle", required=True)

class Comment(db.EmbeddedDocument):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    body = db.StringField(verbose_name="Comment", required=True)
    author = db.StringField(verbose_name="Name", max_length=255, required=True)

class Post(db.Document):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    title = db.StringField(max_length=255, required=True)
    slug = db.StringField(max_length=255, required=True)
    body = db.StringField(required=True)
    comments = db.ListField(db.EmbeddedDocumentField('Comment'))

    def get_absolute_url(self):
        return url_for('post', kwargs={"slug": self.slug})

    def __unicode__(self):
        return self.title

    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'slug'],
        'ordering': ['-created_at']
    }

