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
from os.path import join as op_join
#from copy import deepcopy
#from flask import url_for
#from ast import literal_eval
#from StringIO import StringIO
from DmpWorkflow.config.defaults import cfg #, MAJOR_STATII, FINAL_STATII, TYPES, SITES
from DmpWorkflow.core import db
#from DmpWorkflow.utils.tools import random_string_generator, exceptionHandler
#from DmpWorkflow.utils.tools import parseJobXmlToDict, convertHHMMtoSec, sortTimeStampList

if not cfg.getboolean("site", "traceback"): sys.excepthook = exceptionHandler
log = logging.getLogger("core")

class DataReplica(db.Document):
    site     = db.StringField(max_length=24, required=True)
    status   = db.StringField(max_length=16, default="New")
    path     = db.StringField(max_length=1024, required=True)
    checksum = db.StringField(max_length=72, required=False)
    DataFile = db.ReferenceField("DataFile", reverse_delete_rule=CASCADE)
    meta     = {
                 'allow_inheritance': True,
                 'indexes': ['-created_at', 'status', 'site'],
                 'ordering': ['-created_at']
               }

    def getFileName(self):
        return op_join(self.path,self.DataFile.filename)

class DataFile(db.Document):
    created_at = db.DateTimeField(default=datetime.now, required=True)
    dataset = db.ReferenceField("DataSet", reverse_delete_rule=CASCADE)
    replicas   = db.ListField(db.ReferenceField("DataReplica")) 
    filename= db.StringField(max_length=1024, required=True)
    filetype = db.StringField(max_length=16, required=False, default="root")
    # here are attributes specific to the datafile
    tstart = db.DateTimeField(required=False)
    tstop  = db.DateTimeField(required=False)
    gti = db.FloatField(required=False)
    
    meta = {
            'allow_inheritance': True,
            'indexes': ['-created_at', 'filename', 'site', 'filetype'],
            'ordering': ['-created_at']
            }

class DataSet(db.Document):
    created_at = db.DateTimeField(default=datetime.now, required=True)
    files = db.ListField(db.ReferenceField("DataFile"))
    release = db.StringField(max_length=64, required=False)
    name = db.StringField(max_length=128,required=True)
    filetype = db.StringField(max_length=16, required=False, default="root")
    datatype = db.StringField(max_length=4, required=True, default="USR", choices=("USR","MC","OBS","BT"))
    dataclass = db.StringField(max_length=4, required=False, default="2A"):
    meta = {
            'allow_inheritance': True,
            'indexes': ['-created_at', 'release', 'name'],
            'ordering': ['-created_at']
            }
    