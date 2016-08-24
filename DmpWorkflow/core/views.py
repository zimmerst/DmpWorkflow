import logging
from copy import deepcopy
from os.path import basename
from json import loads, dumps
from DmpWorkflow.utils.tools import dumpr
from flask import Blueprint, request, redirect, render_template, url_for
from flask.ext.mongoengine.wtf import model_form
from datetime import datetime
from flask.views import MethodView
from ast import literal_eval
from re import findall
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.core.models import Job, JobInstance, HeartBeat, DataFile

jobs = Blueprint('jobs', __name__, template_folder='templates')

logger = logging.getLogger("core")


class ListView(MethodView):
    def get(self):
        logger.debug("request %s", str(request))
        jobs = Job.objects.all()
        return render_template('jobs/list.html', jobs=jobs)

class InstanceView(MethodView):
    def get(self):
        logger.debug("InstanceView: request %s",str(request))
        slug  = request.args.get("slug",None)
        instId= int(request.args.get("instanceId",-1))
        if slug is None:
            msg = "must be called with slug"
            logger.error(msg)
            raise Exception(msg)
        try:
            job = Job.objects.get(slug=slug)
        except Job.DoesNotExist():
            msg = "job cannot be found %s"%slug
            logger.error(msg)
            raise Exception(msg)
        if instId == -1:
            msg = "must be called with instanceId"
            logger.error(msg)
            raise Exception(msg)
        logger.info("looking for instances with instId %i & job %s",instId,job.title)
        try:
            instance = JobInstance.objects.get(job=job,instanceId=instId)
            logger.debug("found instance, rendering templates")
        except JobInstance.DoesNotExist:
            jobs = Job.objects.all()
            return render_template('jobs/list.html', jobs=jobs)
        return render_template('jobs/instanceDetail.html', instance=instance)
    
#     def post(self):
#         job = None
#         slug  = request.args.get("slug",None)
#         if slug is None:
#             msg = "must be called with slug"
#             logger.error(msg)
#             raise Exception(msg)
#         try:
#             job = Job.objects.get(slug=slug)
#         except Job.DoesNotExist():
#             msg = "job cannot be found %s"%slug
#             logger.error(msg)
#             raise Exception(msg)
#         instId= int(request.args.get("instanceId",-1))
#         action= request.args.get("action","none")
#         if action != "rollback":
#             return
#         # assume it's rollback from here on.
#         override_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
#         my_dict = {"t_id": job.id, "inst_id": instId,
#                    "major_status": "New", "minor_status": "AwaitingBatchSubmission", "hostname": None,
#                    "batchId": None, "status_history": [], "body": str(override_dict),
#                    "log": "", "cpu": [], "memory": [], "created_at": "Now"}
#         logger.info("submitting my_dict %s",my_dict)
#         return redirect(url_for('jobs.detail', slug=slug))

        
class StatsView(MethodView):
    def get(self):
        logger.debug("request %s", str(request))
        heartbeats = HeartBeat.objects.filter(process="JobFetcher")
        now = datetime.now()
        for h in heartbeats:
            last_life = h.timestamp
            deltaT = (now - last_life).seconds
            h.deltat = deltaT
        return render_template('stats/siteSummary.html', heartbeats=heartbeats)


class DetailView(MethodView):
    form = model_form(JobInstance, exclude=['created_at', 'status_history', 'memory', 'cpu'])

    def get_context(self, slug):
        job = Job.objects.get_or_404(slug=slug)
        form = self.form(request.form)
        aux_data = {'timestamp': unicode(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    'n_jobs': Job.objects.all().count(),
                    'n_instance': JobInstance.objects.all().count()}
        context = {
            "job": job,
            "form": form,
            "aux_data": aux_data,
            "instances": JobInstance.objects.filter(job=job)
        }
        return context

    def get(self, slug):
        logger.debug("request %s", str(request))
        context = self.get_context(slug)
        logger.debug("redering jobs/detail.html")
        return render_template('jobs/detail.html', **context)

    def post(self, slug):
        logger.debug("request %s", str(request))
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


# class InstanceView(MethodView):
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
            dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
            logger.debug("request %s", str(request))
            taskname = request.form.get("taskname", None)
            jobdesc = request.files.get("file", None)
            t_type = request.form.get("t_type", None)
            site = request.form.get("site", "local")
            depends = request.form.get("depends", "None")
            override_dict = literal_eval(request.form.get("override_dict", str(dummy_dict)))
            n_instances = int(request.form.get("n_instances", "0"))
            if taskname is None:
                logger.exception("task name must be defined.")
                raise Exception("task name must be defined")
            job = Job.objects.filter(title=taskname, type=t_type, execution_site=site)
            if job.count():
                raise Exception("job exists already")
            try:
                job = Job.objects.get(title=taskname, type=t_type, execution_site=site)
            except Job.DoesNotExist:
                job = Job(title=taskname, type=t_type, execution_site=site)
            # job = Job.objects(title=taskname, type=t_type).modify(upsert=True, new=True, title=taskname, type=t_type)
            job.body.put(jobdesc, content_type="application/xml")
            job.save()
            dout = job.getBody()
            if 'type' in dout['atts']:
                job.type = dout['atts']['type']
            if 'release' in dout['atts']:
                job.release = dout['atts']['release']
            if t_type is not None:
                job.type = t_type
            if n_instances:
                for j in range(n_instances):
                    jI = JobInstance(body=str(override_dict), site=site)
                    job.addInstance(jI)
                    logger.debug("added instance %i to job %s", (j + 1), job.id)
            # print len(job.jobInstances)
            if depends != "None":
                depends = depends.split(",")
                for d in depends:
                    dependent_job = Job.objects.filter(slug=unicode(d))
                    if dependent_job.count():
                        job.addDependency(dependent_job[0])
                    else:
                        logger.warning("could not find job dependency %s for job %s", d, job.slug)
            job.save()
            return dumpr({"result": "ok", "jobID": str(job.id)})
        except Exception as err:
            logger.error("request dict: %s", str(request.form))
            logger.exception(err)
            return dumpr({"result": "nok", "jobID": "None", "error": str(err)})


class JobInstanceView(MethodView):
    def get(self):
        return 'Nothing yet'

    def post(self):
        logger.debug("request %s", str(request))
        logger.debug("request form dict %s", request.form)
        dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
        taskName = request.form.get("taskname", None)
        tasktype = request.form.get("tasktype", None)
        ninst = int(request.form.get("n_instances", "0"))
        override_dict = literal_eval(request.form.get("override_dict", str(dummy_dict)))
        if taskName is None and tasktype is None:
            return dumpr({"result": "nok", "error": "query got empty taskname & type"})
        jobs = Job.objects.filter(title=taskName, type=tasktype)
        if jobs.count():
            logger.debug("Found job")
            job = jobs[0]
            site = job.execution_site
            try:
                dout = job.getBody()
            except Exception as err:
                logger.error(err)
                return dumpr({"result": "nok", "error": err})
            if 'type' in dout['atts']:
                job.type = unicode(dout['atts']['type'])
            if 'release' in dout['atts']:
                job.release = dout['atts']['release']
            # logger.info('extracted body %s',dout)
            if ninst:
                logger.debug("adding %i instances", ninst)
                for j in range(ninst):
                    try:
                        jI = JobInstance(body=dumps(override_dict), site=site)
                        # if opts.inst and j == 0:
                        #    job.addInstance(jI,inst=opts.inst)
                        # else:
                        job.addInstance(jI)
                    except Exception as err:
                        logger.error(err)
                        return dumpr({"result": "nok", "error": str(err)})
                    logger.debug("added instance %i to job %s", (j + 1), job.id)
            # print len(job.jobInstances)
            job.save()
            return dumpr({"result": "ok"})
        else:
            logger.error("Cannot find job")
            return dumpr({"result": "nok", "error": 'Could not find job %s' % taskName})


# class RefreshJobAlive(MethodView):
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
#            return json.dumpr({"result": "ok"})
#        except Exception as err:
#            logger.exception(err)
#           return json.dumpr({"result": "nok", "error": "server error"})


class SetJobStatus(MethodView):
    def post(self):
        dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
        arguments = loads(request.form.get("args", "{}"))
        logger.debug("request arguments %s", str(arguments))
        if not isinstance(arguments, dict):
            logger.exception("arguments MUST be dictionary.")
        if 'major_status' not in arguments:
            logger.exception("couldn't find major_status in arguments")
        t_id = arguments.get("t_id", "None")
        bId = arguments.get("batchId", None)
        logger.debug("batchId passed to DB %s", bId)
        try:
            if bId is not None:
                res = findall(r"\d+", str(bId))
                if len(res):
                    bId = int(res[0])
        except Exception as err:
            return dumpr({"result": "nok", "error": "Error parsing batchId, message below \n%s" % str(err)})
        site = str(arguments.get("site", "None"))
        inst_id = arguments.get("inst_id", "None")
        bdy = literal_eval(arguments.get("body", str(dummy_dict)))
        major_status = arguments["major_status"]
        minor_status = arguments.get("minor_status", None)
        logger.debug("BODY: %s (type %s)", bdy, type(bdy))
        logger.debug("batchId: %s", bId)
        if 'body' in arguments:
            del arguments['body']
        try:
            jInstance = None
            if t_id != "None" and inst_id != "None":
                my_job = Job.objects.filter(id=t_id)
                if not my_job.count():
                    raise Exception("could not find Job")
                my_job = my_job.first()
                jInstance = my_job.getInstance(inst_id)
            else:
                if bId is not None and site != "None":
                    jInstance = JobInstance.objects.filter(batchId=bId, site=site)
                    if not jInstance.count():
                        raise Exception("could not find JobInstance")
                    jInstance = jInstance.first()
            if jInstance is not None:
                oldStatus = jInstance.status
                minorOld = jInstance.minor_status
                if minor_status is not None and minor_status != minorOld:
                    logger.debug("updating minor status")
                    jInstance.set("minor_status", minor_status)
                    del arguments['minor_status']
                if major_status != oldStatus:
                    jInstance.setStatus(major_status)
                    jInstance.setBody(bdy)
                    if 'body' in arguments: del arguments['body']
                for key in ["t_id", "inst_id", "major_status"]:
                    del arguments[key]                        
                for key, value in arguments.iteritems():
                    # if key == 'batchId' and value is None: value = "None"
                    jInstance.set(key, value)
                    # update_status(t_id,inst_id,major_status, **arguments)
        except Exception as err:
            logger.exception(err)
            return dumpr({"result": "nok", "error": str(err)})
        return dumpr({"result": "ok"})

    def get(self):
        logger.debug("request %s", str(request))
        title = unicode(request.form.get("title", None))
        jtype = unicode(request.form.get("type", "Generation"))
        stat = unicode(request.form.get("stat", "Any"))
        instId = int(request.form.get("inst", -1))
        n_min = int(request.form.get("n_min", -1))
        n_max = int(request.form.get("n_max", -1))
        jobs = Job.objects.filter(title=title, type=jtype)
        logger.debug("jobs found %s", str(jobs))
        queried_instances = []
        if jobs.count():
            logger.debug("get: found jobs matching query %s", jobs)
            if jobs.count() != 1:
                logger.error("found multiple jobs matching query, that shouldn't happen!")
            job = jobs.first()
            if instId == -1:
                logger.debug("Q: job=%s status=%s", job, stat)
                if stat == "Any":
                    queried_instances = JobInstance.objects.filter(job=job)
                else:
                    queried_instances = JobInstance.objects.filter(job=job, status=str(stat))
                logger.debug("query returned %i queried_instances", queried_instances.count())
                filtered_instances = []
                logger.debug("queried: %i filtered: %i", queried_instances.count(), len(filtered_instances))
                for inst in queried_instances:
                    keep = True
                    instId = inst.instanceId
                    if n_min != -1 and instId <= n_min:
                        keep = False
                    if n_max != -1 and instId > n_max:
                        keep = False
                    if not keep:
                        continue
                    filtered_instances.append(inst)
                logger.debug("queried: %i filtered: %i", queried_instances.count(), len(filtered_instances))
                queried_instances = filtered_instances
            else:
                queried_instances = JobInstance.objects.filter(job=job, instanceId=instId)
            logger.debug("query returned %i instances", len(queried_instances))
            try:
                queried_instances = [{"instanceId": q.instanceId, "jobId": str(q.job.id)} for q in queried_instances]
            except Exception as err:
                logger.error(err)
                return dumpr({"result": "nok", "error": "error occurred when forming final output"})
            if len(queried_instances):
                logger.debug("example query instance %s", queried_instances[-1])
        else:
            logger.exception("could not find job")
            return dumpr({"result": "nok", "error": "could not find job"})
        return dumpr({"result": "ok", "jobs": queried_instances})


class NewJobs(MethodView):
    def get(self):
        logger.debug("request %s", str(request))
        jstatus = unicode(request.form.get("status", u"New"))
        batchsite = unicode(request.form.get("site", "local"))
        _limit = int(request.form.get("limit", 1000))
        newJobInstances = []
        allJobs = Job.objects.filter(execution_site=batchsite)
        logger.debug("allJobs = %s", str(allJobs))
        for job in allJobs:
            logger.debug("processing job %s", job.slug)
            dependent_tasks = job.getDependency()
            logger.debug("dependent tasks: %s", dependent_tasks)
            newJobs = JobInstance.objects.filter(job=job, status=jstatus).limit(int(_limit))
            logger.debug("#newJobs: %i", newJobs.count())
            if newJobs.count():
                logger.debug("found %i new instances for job %s", newJobs.count(), str(job.title))
                dJob = DmpJob(job.id, body=None, title=job.title)
                logger.debug("DmpJob instantiation.")
                for dt in dependent_tasks:
                    logger.debug("processing dependencies for job %s", dt)
                    dJob.InputFiles += [{"source": fil, "target": basename(fil)} for fil in dt.getOutputFiles()]
                    dJob.MetaData += [{"name": k, "value": v, "type": "string"} for k, v in
                                      dt.getMetaDataVariables().iteritems()]
                logger.debug("setBodyFromDict")
                dJob.setBodyFromDict(job.getBody())
                for j in newJobs:
                    if j.checkDependencies():
                        logger.debug("depedency satisfied")
                        j.getResourcesFromMetadata()
                        logger.debug("resources read out")
                        dInstance = deepcopy(dJob)
                        dInstance.setInstanceParameters(j.instanceId, j.body)
                        logger.debug('** DEBUG ** instance body : %s',j.body)
                        newJobInstances.append(dInstance.exportToJSON())
                    else:
                        logger.info("dependencies not fulfilled yet")
                logger.debug("found %i new jobs after dependencies", len(newJobInstances))
        return dumpr({"result": "ok", "jobs": newJobInstances})


#class JobResources(MethodView):
#    def get(self):
#       logger.debug("request %s", str(request))
#       batchsite = unicode(request.form.get("site", "local"))
#       runningJobs = JobInstance.objects.filter(site=batchsite, status=u"Running")
#       logger.debug("number of runningJobs = %i", runningJobs.count())
#       try:
#           allJobs = []
#           for j in runningJobs:
#               allJobs = [{"batchId": j.batchId, "cpu": j.get("cpu"),
#                           "memory": j.get("memory"),
#                           "t_id": str(j.job.id),
#                           "inst_id": j.instanceId,
#                           "major_status": j.status,
#                           "max_cpu": j.get("max_cpu"),
#                           "max_mem": j.get("max_mem")} for j in runningJobs]
#           logger.debug("dumping %i jobs", len(allJobs))
#           return dumpr({"result": "ok", "jobs": allJobs})
#       except Exception as err:
#           return dumpr({"result": "nok", "error": err})


class TestView(MethodView):
    def post(self):
        logger.debug("TestView: request form %s", str(request.form))
        hostname = str(request.form.get("hostname", "None"))
        proc = str(request.form.get("process","default"))
        timestamp = datetime.now()
        logger.debug("TestView: hostname: %s timestamp: %s ", hostname, timestamp)
        if (hostname == "None") or (timestamp == "None"):
            logger.debug("request empty")
            return dumpr({"result": "nok", "error": "request empty"})
        try:
            if hostname == "None" and proc != "default":
                q = HeartBeat.objects.filter(process=proc)
                if q.count():
                    q.update(timestamp=timestamp)
                else:
                    return({"result":"nok", "error":"could not update timestamp because hostname was not specified"})
            elif proc == "default" and hostname != "None":
                q = HeartBeat.objects.filter(hostname=hostname)
                if q.count():
                    q.update(timestamp=timestamp)
                else:
                    HB = HeartBeat(hostname=hostname, timestamp=timestamp, process=proc)
                    HB.save()
            else:
                q = HeartBeat.objects.filter(hostname=hostname, process=proc)
                if q.count():
                    q.update(timestamp=timestamp)
                else:
                    HB = HeartBeat(hostname=hostname, timestamp=timestamp, process=proc)
                    HB.save()
        except Exception as ex:
            logger.error("failure during HeartBeat POST test. \n%s", ex)
            return dumpr({"result": "nok", "error": ex})
        return dumpr({"result": "ok"})

    def get(self):
        limit = int(request.form.get("limit", 1000))
        beats = []
        try:
            beats = HeartBeat.objects.all().limit(limit)
            logger.debug("found %i heartbeats", beats.count())
        except Exception as ex:
            logger.error("failure during HeartBeat GET test. \n%s", ex)
            return dumpr({"result": "nok", "error": ex})
        return dumpr({"result": "ok", "beats": [b.hostname for b in beats]})


class DataCatalog(MethodView):
    def __register__(self, args):
        query = args[0]
        filename = args[1]
        site = args[2]
        filetype = args[3]
        force = args[4]
        if len(query):
            if force:
                for f in query: f.delete()
            else:
                return dumpr({"result": "nok", "error": "called register but file apparently exists already"})
        else:
            df = DataFile(filename=filename, site=site, status="New", filetype=filetype)
            df.save()
            return None

    def __update_or_remove__(self, df, status=None, action="setStatus"):
        if status is None:
            return dumpr({"result": "nok", "error": "status is None"})
        if action == 'setStatus':
            df.setStatus(status)
            df.update()
        else:
            logger.info("requested removal!")
            df.delete()
        return None

    def post(self):
        logger.debug("DataCatalog: request form %s", str(request.form))
        filename = str(request.form.get("filename", "None"))
        site = str(request.form.get("site", "None"))
        action = str(request.form.get("action", 'register'))
        status = str(request.form.get("status", "New"))
        filetype = str(request.form.get("filetype", "root"))
        force = bool(request.form.get("overwrite", "False"))
        logger.debug("filename %s status %s", filename, status)
        if action not in ['register', 'setStatus', 'delete']:
            logger.error("action not supported")
            return dumpr({"result": "nok", "error": "action not supported"})
        if site == "None" and filename == "None":
            logger.error("request empty")
            return dumpr({"result": "nok", "error": "request empty"})
        try:
            df = None
            files = [filename]
            if "," in filename:
                files = filename.split(",")
                logger.info("bulk request, found %i files", len(files))
            touched_files = []
            for filename in files:
                fileQuery = DataFile.objects.filter(filename=filename, site=site, filetype=filetype)
                if action == 'register':
                    logger.debug("request a new file to be registered")
                    res = self.__register__([fileQuery, filename, site, filetype, force])
                    if res is not None: return res
                else:
                    if fileQuery.count():
                        df = fileQuery[0]
                        res = self.__update_or_remove__(df, status=status, action=action)
                        if res is not None: return res
                    else:
                        logger.debug("cannot find queried input file")
                        return dumpr({"result": "nok", "error": "cannot find file in DB"})
                if df is not None: touched_files.append(df)
        except Exception as ex:
            logger.error("failure during DataCatalog POST. \n%s", ex)
            return dumpr({"result": "nok", "error": str(ex)})
        return dumpr({"result": "ok",
                      "docId": [d.filename if action == 'delete' else str(d.id) for d in touched_files]})

    def get(self):
        limit = int(request.form.get("limit", 1000))
        site = str(request.form.get("site", "None"))
        status = str(request.form.get("status", "New"))
        filetype = str(request.form.get("filetype", "root"))
        try:
            dfs = DataFile.objects.filter(site=site, status=status, filetype=filetype).limit(limit)
            logger.debug("found %i files matching query", dfs.count())
        except Exception as ex:
            logger.error("failure during DataCatalog GET. \n%s", ex)
            return dumpr({"result": "nok", "error": str(ex)})
        return dumpr({"result": "ok", "files": [f.filename for f in dfs]})


# Register the urls
jobs.add_url_rule('/', view_func=ListView.as_view('list'))
jobs.add_url_rule('/stats', view_func=StatsView.as_view('stats'), methods=["GET"])
jobs.add_url_rule('/<slug>/', view_func=DetailView.as_view('detail'))
jobs.add_url_rule("/job/", view_func=JobView.as_view('jobs'), methods=["GET", "POST"])
jobs.add_url_rule('/jobInstances/detail', view_func=InstanceView.as_view('instanceDetail'))
jobs.add_url_rule("/jobInstances/", view_func=JobInstanceView.as_view('jobinstances'), methods=["GET", "POST"])
jobs.add_url_rule("/jobstatus/", view_func=SetJobStatus.as_view('jobstatus'), methods=["GET", "POST"])
jobs.add_url_rule("/newjobs/", view_func=NewJobs.as_view('newjobs'), methods=["GET"])
#jobs.add_url_rule("/watchdog/", view_func=JobResources.as_view('watchdog'), methods=["GET"])
jobs.add_url_rule("/testDB/", view_func=TestView.as_view('testDB'), methods=["GET", "POST"])
jobs.add_url_rule("/datacat/", view_func=DataCatalog.as_view('datacat'), methods=["GET", "POST"])

# jobs.add_url_rule('/InstanceDetail', view_func=InstanceView.as_view('instancedetail'), methods=['GET'])
