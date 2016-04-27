# pylint: disable=E1002
import datetime
import sys
import mongoengine
from flask import url_for
from DmpWorkflow.config.defaults import cfg, MAJOR_STATII, FINAL_STATII, TYPES, SITES
from DmpWorkflow.core import db, app
from DmpWorkflow.utils.tools import random_string_generator, exceptionHandler, parseJobXmlToDict

if not cfg.getboolean("site", "traceback"):
    sys.excepthook = exceptionHandler

log = app.logger

class Job(db.Document):
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    slug = db.StringField(verbose_name="slug", required=True, default=random_string_generator)
    title = db.StringField(max_length=255, required=True)
    body = db.FileField()
    type = db.StringField(verbose_name="type", required=False, default="Other", choices=TYPES)
    release = db.StringField(max_length=255, required=False)
    dependencies = db.ListField(db.ReferenceField("Job"))
    execution_site = db.StringField(max_length=255, required=True, default="local", choices=SITES)
    jobInstances = db.ListField(db.ReferenceField("JobInstance"))

    def addDependency(self, job):
        if not isinstance(job, Job):
            raise Exception("Must be job to be added")
        self.dependencies.append(job)

    def getDependency(self):
        if not len(self.dependencies):
            return "None"
        else:
            return tuple(self.dependencies)

    def getNevents(self):
        #log.warning("FIXME: need to implement fast query")
        return "NaN"

    def getBody(self):
        # os.environ["DWF_JOBNAME"] = self.title
        return parseJobXmlToDict(self.body.read())

    def getInstance(self, _id):
        jI = JobInstance.objects.filter(job=self, instanceId=_id)
        log.info("jobInstances from query: %s",str(jI))
        if len(jI):
            return jI[0]
        # for jI in self.jobInstances:
        #    if long(jI.instanceId) == long(_id):
        #         return jI
        log.exception("could not find matching id")             
        return None

    def addInstance(self, jInst, inst=None):
        if not isinstance(jInst, JobInstance):
            log.exception("must be job instance to be added")
            raise Exception("Must be job instance to be added")
        last_stream = len(self.jobInstances)
        if inst is not None:
            # FIXME: offsets one, but then goes back to the length counter.
            last_stream = inst - 1
            if self.getInstance(last_stream + 1):
                log.exception("job with instance %i exists already", inst)
                raise Exception("job with instance %i exists already" % inst)
        jInst.instanceId = last_stream + 1
        if not len(jInst.status_history):
            sH = {"status": jInst.status, "update": jInst.last_update, "minor_status": jInst.minor_status}
            jInst.status_history.append(sH)
        jInst.job = self # add self reference?
        jInst.save()
        self.jobInstances.append(jInst)

    def aggregateStatii(self, asdict=False):
        """ will return an aggregated summary of all instances in all statuses """
        counting_dict = dict(zip(MAJOR_STATII, [0 for _ in MAJOR_STATII]))
        for jI in self.jobInstances:
            if jI.status not in MAJOR_STATII:
                raise Exception("Instance found in status not known to system")
            counting_dict[jI.status] += 1
        ret = [(k, counting_dict[k]) for k in MAJOR_STATII]
        if asdict: 
            return {v[0]:v[1] for v in ret} 
        else:
            return ret

    def get_absolute_url(self):
        return url_for('job', kwargs={"slug": self.slug})

    def __unicode__(self):
        return self.title

    def save(self):
        req = Job.objects.filter(title=self.title)
        if req:
            raise Exception("a task with the specified name exists already.")
        super(Job, self).save()

    def update(self):
        log.info("calling update on Job")
        super(Job, self).save()

    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'slug', 'title', 'id', 'execution_site'],
        'ordering': ['-created_at']
    }

class JobInstance(db.Document):
    instanceId = db.LongField(verbose_name="instanceId", required=False, default=None)
    created_at = db.DateTimeField(default=datetime.datetime.now, required=True)
    body = db.StringField(verbose_name="JobInstance", required=False, default="")
    last_update = db.DateTimeField(default=datetime.datetime.now, required=True)
    batchId = db.LongField(verbose_name="batchId", required=False, default=None)
    Nevents = db.LongField(verbose_name="Nevents", required=False, default=None)
    site = db.StringField(verbose_name="site", required=False, default="local", choices=SITES)
    hostname = db.StringField(verbose_name="hostname", required=False, default=None)
    status = db.StringField(verbose_name="status", required=False, default="New", choices=MAJOR_STATII)
    minor_status = db.StringField(verbose_name="minor_status", required=False, default="AwaitingBatchSubmission")
    status_history = db.ListField()
    log = db.StringField(verbose_name="log", required=False, default="")
    job = db.ReferenceField("Job", reverse_delete_rule=mongoengine.CASCADE)

    def getLog(self):
        lines = self.log.split("\n")
        return lines

    def set(self, key, value):
        self.__setattr__(key, value)
        self.__setattr__("last_update", datetime.datetime.now())
        self.save()

    def setStatus(self, stat):
        log.debug("calling JobInstance.setStatus")
        if stat not in MAJOR_STATII:
            raise Exception("status not found in supported list of statii")
        curr_status = self.status
        curr_time = datetime.datetime.now()
        self.last_update = curr_time
        if curr_status == stat and self.minor_status == self.status_history[-1]['minor_status']:
            return
        if curr_status in FINAL_STATII:
            if not stat == 'New':
                raise Exception("job found in final state, can only set to New")
        self.last_update = self.last_update
        self.set("status", stat)
        sH = {"status": self.status, 
              "update": self.last_update,
              "minor_status": self.minor_status}
        log.info("statusSet %s",str(sH))
        self.status_history.append(sH)
        self.save()
        return

    def sixDigit(self, size=6):
        return str(self.instanceId).zfill(size)

    meta = {
        'allow_inheritance': True,
        'indexes': ['-created_at', 'instanceId', 'site'],
        'ordering': ['-created_at']
    }
