'''
Created on Mar 2, 2016
@deprecated: depends on 3rd party marrow.mongo package... try dropping
@author: zimmer
special thanks to GothAlice in the #mongodb freenode channel
'''

import datetime, random
from flask import url_for

from marrow.mongo.core.document import Document
from marrow.mongo.field.base import String
from marrow.mongo.field.number import Long

class Job(Document):
    release = String(required=False)
    taskName = String(required=True)
    particle = String(required=True)
    type = String(default=None, choices=("None", "Generator", 
                                         "Digitization", "Reconstruction"))
    @property
    def created_at(self):
        return self._id.generation_time
    def get_id(self):
        return self._id
    
    def get_absolute_url(self):
        return url_for('job', kwargs={"taskName": self.taskName})
    
    def __unicode__(self):
        return self.taskName


class JobInstance(Job):
    randomSeed = Long(default=0)
    
    def get_absolute_url(self):
        return url_for('job', kwargs={"taskName": self.taskName})
    
    def __unicode__(self):
        return unicode(self._id)