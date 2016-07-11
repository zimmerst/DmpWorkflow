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

fileStatii = ("New","Orphaned","Bad","Good")


class DataReplica(db.Document):
    site     = db.StringField(max_length=24, required=True)
    status   = db.StringField(max_length=16, default="New",choices=fileStatii)
    path     = db.StringField(max_length=1024, required=True)
    CheckSum = db.StringField(max_length=72, required=False)
    DataFile = db.ReferenceField("DataFile", reverse_delete_rule=CASCADE)

    meta     = {
                 'allow_inheritance': True,
                 'indexes': ['-created_at', 'status', 'site'],
                 'ordering': ['-created_at']
               }

    def update(self,**kwargs):
        self.site = kwargs.get("site",cfg.get("site", "name"))
        self.status = kwargs.get("status","new")
        path = kwargs.get("path",None)
        if path is None: return
        self.path = path
        self.save()

        
    def getFileName(self):
        return op_join(self.path,self.DataFile.filename)

class DataFile(db.Document):
    created_at = db.DateTimeField(default=datetime.now, required=True)
    dataset = db.ReferenceField("DataSet", reverse_delete_rule=CASCADE)
    replicas   = db.ListField(db.ReferenceField("DataReplica")) 
    filename= db.StringField(max_length=1024, required=True)
    filetype = db.StringField(max_length=16, required=False, default="root")
    origin = db.ReferenceField("DataReplica")
    # here are attributes specific to the datafile
    TStart = db.DateTimeField(required=False)
    TStop  = db.DateTimeField(required=False)
    GTI = db.FloatField(required=False)

    def verifyReplicas(self):
        cs_origin = self.origin.CheckSum
        for rep in DataReplica.objects.filter(DataFile=self,status="New"):
            if rep != self.origin: 
                cs_replica = rep.CheckSum
                if cs_replica!=cs_origin: 
                    log.warning("CheckSum failed for replica of file %s at %s",self.filename, rep.site)
                    rep.status("Bad")
                else: rep.status("Good")
                rep.save()
    
    def declareOrigin(self,replica):
        if not isinstance(replica,DataReplica): raise Exception("must be a replica instance")
        self.origin = replica
        if not self.origin in self.replicas:
            log.warning("could not find replica marked as origin in list of replicas, registering a new one.")
            self.origin.DataFile = self
            self.origin.save()
            self.replicas.append(self.origin)
        self.save()
        
    def declareOriginFromDict(self,_dict):
        """ create the replica on the fly, from a dictionary alone
            required items in dictionary:
            path : full path to source
            checksum: checksum at source
            site : site name
        """
        path = _dict.get("path",None)
        checksum = _dict.get("checksum",None)
        site = _dict.get("site",None)
        if path is None:        log.error("path missing")
        elif checksum is None:  log.error("checksum missing")
        elif site is None:      log.error("site missing")
        origin = None
        try:
            origin = DataReplica.objects.filter(site=site,path=path,status="Good",DataFile=self,CheckSum=checksum)
        except DataReplica.DoesNotExist:
            log.warning("registering new replica as origin")
            origin = DataReplica(site=site,path=path,status="Good",DataFile=self,CheckSum=checksum)
            origin.save()
            
    def registerReplica(self,**kwargs):
        site = kwargs.get("site",cfg.get("site", "name"))
        status = kwargs.get("status","new")
        path = kwargs.get("path",None)
        if path is None:
            log.error("trying to register empty replica, must provide path")
            return
        force = bool(kwargs.get("force","false"))
        try:
            replica = DataReplica.objects.get(site=site, DataFile=self)
            if force: replica.update(site=site,status=status,path=path)
            log.warning("trying to re-register an existing replica")
        except DataReplica.DoesNotExist:
            replica = DataReplica(site=site,status=status,DataFile=self,path=path)
            replica.save()
        return replica 
    
    def removeReplica(self,**kwargs):
        site = kwargs.get("site",cfg.get("site", "name")) 
        try:
            replica = DataReplica.objects.get(site=site, DataFile=self)
            replica.delete()
        except DataReplica.DoesNotExist:
            log.error("requested replica does not exist")
        return
    
    def updateReplicaStatus(self,**kwargs):
        site = kwargs.get("site",cfg.get("site", "name")) 
        status = kwargs.get("status",None)
        if status is None:
            log.error("must supply a status")
            return
        try:
            replica = DataReplica.objects.get(site=site, DataFile=self)
            replica.status = status
            replica.save()
        except DataReplica.DoesNotExist:
            log.error("requested replica does not exist")
            return
        
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
    FileType = db.StringField(max_length=16, required=False, default="root")
    DataType = db.StringField(max_length=4, required=True, default="USR", choices=("USR","MC","OBS","BT"))
    DataClass = db.StringField(max_length=4, required=False, default="2A"):
    
    def findDataFile(self,register=True,**kwargs):
        FileName = kwargs.get("FileName",None)
        if FileName is None: 
            log.error("must provide at least a file name!")
            return
        TStart = datetime.strptime(kwargs.get("TStart",defaultTime),"%Y%m%d%H%M%S")
        TStop =  datetime.strptime(kwargs.get("TStop",defaultTime),"%Y%m%d%H%M%S")
        Gti   = float(kwargs.get("Gti",0.))
        FileType = kwargs.get("FileType"None)
        ds = None
        try:
            ds = DataFile.objects.filter(filename=FileName,TStart=TStart,TStop=TStop,GTI=Gti)
        except DataFile.DoesNotExist:
            if register:
                ds = DataFile(filename=FileName,TStart=TStart,TStop=TStop,GTI=Gti)
                ds.save()
        return ds
    
    
    meta = {
            'allow_inheritance': True,
            'indexes': ['-created_at', 'release', 'name'],
            'ordering': ['-created_at']
            }
    