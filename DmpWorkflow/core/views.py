import logging
import copy
import json
from flask import Blueprint, request, redirect, render_template, url_for
from flask.ext.mongoengine.wtf import model_form
from flask.views import MethodView
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.core.models import Job, JobInstance
#from DmpWorkflow.utils.db_helpers import update_status
from DmpWorkflow.utils.tools import parseJobXmlToDict

jobs = Blueprint('jobs', __name__, template_folder='templates')

logger = logging.getLogger("core")


class ListView(MethodView):
    def get(self):
        logger.debug("request %s",str(request))
        jobs = Job.objects.all()
        return render_template('jobs/list.html', jobs=jobs)


class DetailView(MethodView):
    form = model_form(JobInstance, exclude=['created_at', 'status_history', 'memory','cpu'])

    def get_context(self, slug):
        job = Job.objects.get_or_404(slug=slug)
        form = self.form(request.form)
        context = {
            "job": job,
            "form": form
        }
        return context

    def get(self, slug):
        logger.debug("request %s",str(request))
        context = self.get_context(slug)
        logger.debug("redering jobs/detail.html")
        return render_template('jobs/detail.html', **context)

    def post(self, slug):
        logger.debug("request %s",str(request))
        context = self.get_context(slug)
        form = context.get('form')

        if form.validate():
            jobInstance = JobInstance()
            form.populate_obj(jobInstance)

            job = context.get('job')
            job.addInstance(jobInstance)
            job.save()

            return redirect(url_for('jobs.detail', slug=slug))

        return render_template('jobs/detail.html', **context)

#class InstanceView(MethodView):
#
#    def get_context(self, slug, inst_id=""):
#        job = Job.objects.get_or_404(slug=slug)
#        jobInstance = None
#        if inst_id!="": 
#            jobInstance = Job.objects.filter(job=job, instanceId=inst_id)[0]
#        context = {
#            "job": job,
#            "jobInstance": jobInstance,
#        }
#        return context
#
#    def get(self):
#        inst_id = str(request.form.get("inst_id",""))
#        slug    = str(request.form.get("slug",""))
#        logger.debug("slug %s - inst_id %s",slug, inst_id)
#        if slug == "":
#            raise Exception("slug must be non-zero")
#        logger.debug("InstanceView: get slug, inst_id",slug, inst_id)
#        context = self.get_context(slug,inst_id=inst_id)
#        logger.debug("rendering jobs/instanceDetail")
#        return render_template('jobs/instanceDetail.html', **context)

class JobView(MethodView):
    def get(self):
        return "Nothing to display"

    def post(self):
        try:    
            logger.debug("request %s",str(request)) 
            taskname = request.form.get("taskname",None)
            jobdesc = request.files.get("file",None)
            t_type = request.form.get("t_type",None)
            site = request.form.get("site","local")
            n_instances = int(request.form.get("n_instances","0"))
            if taskname is None:
                logger.exception("task name must be defined.")
                raise Exception("task name must be defined")
            job = Job(title=taskname, type=t_type, execution_site=site)
            #job = Job.objects(title=taskname, type=t_type).modify(upsert=True, new=True, title=taskname, type=t_type)
            job.body.put(jobdesc, content_type="application/xml")
            job.save()
            dout = parseJobXmlToDict(job.body.read())
            if 'type' in dout['atts']:
                job.type = dout['atts']['type']
            if 'release' in dout['atts']:
                job.release = dout['atts']['release']
            if t_type is not None: job.type = t_type
            dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
            if n_instances:
                for j in range(n_instances):
                    jI = JobInstance(body=str(dummy_dict))
                    job.addInstance(jI)
                    logger.debug("added instance %i to job %s",(j+1),job.id)
            # print len(job.jobInstances)
            job.update()
            return json.dumps({"result": "ok", "jobID": str(job.id)})
        except Exception as err:
            logger.info("request dict: %s",str(request.form))
            logger.exception(err)
            return json.dumps({"result": "nok", "jobID": "None", "error":str(err)})

class JobInstanceView(MethodView):
    def get(self):
        return 'Nothing yet'

    def post(self):
        logger.debug("request %s",str(request))
        taskName = request.form.get("taskname",None)
        ninst = int(request.form.get("n_instances","0"))
        jobs = Job.objects.filter(title=taskName)
        if len(jobs):
            logger.debug("Found job")
            job = jobs[0]
            site = job.execution_site
            dout = parseJobXmlToDict(job.body.read())
            if 'type' in dout['atts']:
                job.type = unicode(dout['atts']['type'])
            if 'release' in dout['atts']:
                job.release = dout['atts']['release']
            dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
            if ninst:
                for j in range(ninst):
                    jI = JobInstance(body=str(dummy_dict), site=site)
                    # if opts.inst and j == 0:
                    #    job.addInstance(jI,inst=opts.inst)
                    # else:
                    job.addInstance(jI)
                    logger.debug("added instance %i to job %s",(j+1),job.id)
            # print len(job.jobInstances)
            job.update()
            return json.dumps({"result": "ok"})
        else:
            logger.error("Cannot find job")
            return json.dumps({"result": "nok", "error": 'Could not find job %s' % taskName})


#class RefreshJobAlive(MethodView):
#    def post(self):
#        try:
#            taskid = request.form.get("taskid",None)
#            instance_id = request.form.get("instanceid",None)
#            hostname = request.form.get("hostname","")
#            status = request.form.get("status","None")
#            my_job = Job.objects.filter(id=taskid)
#            if not len(my_job): raise Exception("could not find Job")
#            my_job = my_job[0]
#            jInstance = my_job.getInstance(instance_id)
#            jInstance.set("hostname", hostname)
#            oldStatus = jInstance.status
#            if status != oldStatus:
#                jInstance.setStatus(status)
#            my_job.update()
#            return json.dumps({"result": "ok"})
#        except Exception as err:
#            logger.exception(err)
#           return json.dumps({"result": "nok", "error": "server error"})


class SetJobStatus(MethodView):
    def post(self):
        arguments = json.loads(request.form.get("args","{}"))
        if not isinstance(arguments,dict):      logger.exception("arguments MUST be dictionary.")        
        if 't_id' not in arguments:         logger.exception("couldn't find t_id in arguments")
        if 'inst_id' not in arguments:      logger.exception("couldn't find inst_id in arguments")
        if 'major_status' not in arguments: logger.exception("couldn't find major_status in arguments")
        logger.debug("request arguments %s", str(arguments))
        t_id = arguments["t_id"]
        inst_id = arguments["inst_id"]
        major_status = arguments["major_status"]
        minor_status = arguments["minor_status"]
        try:
            my_job = Job.objects.filter(id=t_id)
            if not len(my_job): raise Exception("could not find Job")
            my_job = my_job[0]
            jInstance = my_job.getInstance(inst_id)
            oldStatus = jInstance.status
            minorOld  = jInstance.minor_status
            if minor_status is not None and minor_status!=minorOld:
                logger.debug("updating minor status")
                jInstance.set("minor_status",minor_status)
                del arguments['minor_status']
            if major_status != oldStatus:
                jInstance.setStatus(major_status)
            for key in ["t_id","inst_id","major_status"]: del arguments[key]
            for key,value in arguments.iteritems():
                jInstance.set(key,value)
            #update_status(t_id,inst_id,major_status, **arguments)
        except Exception as err:
            logger.exception(err)
            return json.dumps({"result": "nok", "error": str(err)})
        return json.dumps({"result": "ok"})

    def get(self):
        logger.debug("request %s",str(request))
        title = unicode(request.form.get("title",None))
        stat  = unicode(request.form.get("stat","Any"))
        instId = int(request.form.get("inst",-1))
        n_min = int(request.form.get("n_min",-1))
        n_max = int(request.form.get("n_max",-1))
        jobs = Job.objects.filter(title=title)
        queried_instances = []
        if len(jobs):
            logger.debug("get: found jobs matching query %s",jobs)
            if len(jobs)!=1:
                logger.error("found multiple jobs matching query, that shouldn't happen!")
            job = jobs[0]
            if instId == -1:
                logger.debug("Q: job=%s status=%s",job,stat)
                if stat == "Any":
                    queried_instances = JobInstance.objects.filter(job=job)
                else:
                    queried_instances = JobInstance.objects.filter(job=job,status=str(stat))
                logger.debug("query returned %i queried_instances",len(queried_instances))
                filtered_instances = []
                for inst in queried_instances:
                    keep = True
                    instId = inst.instanceId
                    if n_min != -1 and instId <= n_min: keep = False
                    if n_max != -1 and instId  > n_max: keep = False
                    if keep: filtered_instances.append(inst)
                queried_instances = filtered_instances
            else:
                queried_instances = JobInstance.objects.filter(job=job, instanceId = instId)
            logger.debug("query returned %i instances",len(queried_instances))
            queried_instances = [{"instanceId":q.instanceId, "jobId":str(q.job.id)} for q in queried_instances]
            if len(queried_instances): 
                logger.debug("example query instance %s",queried_instances[-1])
        else:
            logger.exception("could not find job")
            return json.dumps({"result":"nok","error": "could not find job"})
        return json.dumps({"result":"ok", "jobs": queried_instances})

class NewJobs(MethodView):
    def get(self):
        logger.debug("request %s",str(request))
        batchsite = unicode(request.form.get("site","local"))
        newJobInstances = []
        allJobs = Job.objects.filter(execution_site=batchsite)
        logger.debug("allJobs = %s",str(allJobs))
        for job in allJobs:
            newJobs = JobInstance.objects.filter(job=job, status=u"New")
            logger.debug("newJobs: %s",str(newJobs))
            if len(newJobs):
                logger.debug("found %i new instances for job %s",len(newJobs),str(job.title))
                dJob = DmpJob(job.id, job.body.read(), title=job.title)
                for j in newJobs:
                    if j.checkDependencies():
                        dInstance = copy.deepcopy(dJob)
                        dInstance.setInstanceParameters(j.instanceId, j.body)
                        newJobInstances.append(dInstance.exportToJSON())
                    else:
                        logger.debug("dependencies not fulfilled yet")
                logger.debgug("found %i new jobs after dependencies",len(newJobs))
        return json.dumps({"result":"ok", "jobs": newJobInstances})

class JobResources(MethodView):
    def get(self):
        batchsite = unicode(request.form.get("site","local"))
        runningJobs = JobInstance.objects.filter(site=batchsite, status=u"Running")
        logger.info("number of runningJobs = %i", len(runningJobs))
        try:
            allJobs = [{"batchId":j.batchId, "cpu":j.get("cpu"), 
                    "memory":j.get("memory"), 
                    "t_id":str(j.job.id), 
                    "inst_id":j.instanceId,
                    "major_status":j.major_status,
                    "meta":j.parseBodyXml()} for j in runningJobs]
            logger.info("dumping %i jobs",len(allJobs))
            return json.dumps({"result":"ok", "jobs": allJobs})
        except Exception as err:
            return json.dumps({"result":"nok", "error": err})
        
# Register the urls
jobs.add_url_rule('/', view_func=ListView.as_view('list'))
jobs.add_url_rule('/<slug>/', view_func=DetailView.as_view('detail'))
jobs.add_url_rule("/job/", view_func=JobView.as_view('jobs'), methods=["GET", "POST"])
jobs.add_url_rule("/jobInstances/", view_func=JobInstanceView.as_view('jobinstances'), methods=["GET", "POST"])
jobs.add_url_rule("/jobstatus/", view_func=SetJobStatus.as_view('jobstatus'), methods=["GET","POST"])
jobs.add_url_rule("/newjobs/", view_func=NewJobs.as_view('newjobs'), methods=["GET"])
jobs.add_url_rule("/watchdog/",view_func=JobResources.as_view("watchdog"), methods=["GET"])
#jobs.add_url_rule('/InstanceDetail', view_func=InstanceView.as_view('instancedetail'), methods=['GET'])
