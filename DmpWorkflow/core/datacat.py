'''
Created on Jul 5, 2016

@author: zimmer
@brief: models for datacatalog access

'''
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

if not cfg.getboolean("site", "traceback"): sys.excepthook = exceptionHandler
log = logging.getLogger("core")

class DataFile(db.Document):
    my_choices = ("New","Copied","Orphaned")
    created_at = db.DateTimeField(default=datetime.now, required=True)
    filename = db.StringField(max_length=1024, required=True)
    site = db.StringField(max_length=24, required=True)
    filetype = db.StringField(max_length=16, required=False, default="root")
    status = db.StringField(max_length=16, default="New")

    def setStatus(self,stat):
        if stat not in self.my_choices:
            raise Exception("status not supported in DB")
        self.status = stat

    def save(self):
        req = DataFile.objects.filter(filename=self.filename, site=self.site)
        if req:
            raise Exception("a file with the specified properties exists already, consider updating instead!")
        super(DataFile, self).save()

    def update(self):
        log.debug("calling update on DataFile")
        super(DataFile, self).save()

    meta = {
            'allow_inheritance': True,
            'indexes': ['-created_at', 'filename', 'site'],
            'ordering': ['-created_at']
            }
