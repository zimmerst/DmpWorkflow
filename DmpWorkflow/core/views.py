import logging
from copy import deepcopy
from os.path import basename
from json import loads, dumps
from flask import Blueprint, request, render_template
from datetime import datetime
from flask.views import MethodView
from ast import literal_eval
from re import findall
from DmpWorkflow import version as DAMPE_VERSION
from DmpWorkflow.core.DmpJob import DmpJob
from DmpWorkflow.core.models import Job, JobInstance, HeartBeat, DataFile

jobs = Blueprint('jobs', __name__, template_folder='templates')

logger = logging.getLogger("core")


class ListView(MethodView):
    def get(self):
        logger.debug("ListView:GET: request %s", str(request))
        jobs = Job.objects.all()
        return render_template('jobs/list.html', jobs=jobs)

class InstanceView(MethodView):
    def get(self):
        logger.debug("InstanceView: request %s",str(request))
        slug  = request.args.get("slug",None)
        instId= int(request.args.get("instanceId",-1))
        try:
            if slug is None:
                msg = "must be called with slug"
                raise Exception(msg)
            job = Job.objects.get(slug=slug)
            if instId == -1:
                raise Exception("must be called with instanceId")
            logger.debug("InstanceView:GET: looking for instances with instId %i & job %s",instId,job.title)
            instance = JobInstance.objects.get(job=job,instanceId=instId)
            logger.debug("InstanceView:GET: found instance, rendering templates")
        except Exception as err:
            jobs = Job.objects.all()
            logger.exception("InstanceView:GET: caught exception %s",err)
            return render_template('jobs/list.html', jobs=jobs)
        return render_template('jobs/instanceDetail.html', instance=instance)
        
class StatsView(MethodView):
    def get(self):
        logger.debug("StatsView:GET: request %s", str(request))
        heartbeats = HeartBeat.objects.filter(process="JobFetcher")
        now = datetime.now()
        for h in heartbeats:
            last_life = h.timestamp
            deltaT = (now - last_life).seconds
            h.deltat = deltaT
        return render_template('stats/siteSummary.html', heartbeats=heartbeats, server_version = DAMPE_VERSION, server_time = now)

class DetailView(MethodView):
    def get(self, slug):
        logger.debug("DetailView:GET: request %s", str(request))
        job = Job.objects.get_or_404(slug=slug)
        status  = request.args.get("status",None)
        instances = []
        if status is None:
            instances = JobInstance.objects.filter(job=job)
            status = "None"
        else:
            logger.info("DetailView:GET: request called with status query")
            instances = JobInstance.objects.filter(job=job,status=status)
        logger.info("DetailView:GET: found %i instances for job %s",instances.count(),job.title)
        return render_template('jobs/detail.html',job=job, instances=instances, status = status)

    def post(self):
        dumps({"result":"ok","error":"Nothing to display"})

class JobView(MethodView):
    def get(self):
        dumps({"result":"ok","error":"Nothing to display"})

    def post(self):
        try:
            dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
            logger.debug("JobView:GET: request %s", str(request))
            taskname = request.form.get("taskname", None)
            jobdesc = request.files.get("file", None)
            t_type = request.form.get("t_type", None)
            site = request.form.get("site", "local")
            depends = request.form.get("depends", "None")
            override_dict = literal_eval(request.form.get("override_dict", str(dummy_dict)))
            n_instances = int(request.form.get("n_instances", "0"))
            if taskname is None:
                logger.exception("JobView:GET: task name must be defined.")
                raise Exception("task name must be defined")
            job = None 
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
                    logger.debug("JobView:GET: added instance %i to job %s", (j + 1), job.id)
            # print len(job.jobInstances)
            if depends != "None":
                depends = depends.split(",")
                for d in depends:
                    dependent_job = Job.objects.filter(slug=unicode(d))
                    if dependent_job.count():
                        job.addDependency(dependent_job[0])
                    else:
                        logger.warning("JobView:GET: could not find job dependency %s for job %s", d, job.slug)
            job.save()
            return dumps({"result": "ok", "jobID": str(job.id)})
        except Exception as err:
            logger.error("request dict: %s", str(request.form))
            logger.exception("JobView:GET: %s",err)
            return dumps({"result": "nok", "jobID": "None", "error": str(err)})

class JobInstanceView(MethodView):
    def get(self):
        dumps({"result":"ok","error":"Nothing yet"})

    def post(self):
        logger.debug("JobInstanceView:POST: request %s", str(request))
        logger.debug("JobInstanceView:POST: request form dict %s", request.form)
        dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
        taskName = request.form.get("taskname", None)
        tasktype = request.form.get("tasktype", None)
        ninst = int(request.form.get("n_instances", "0"))
        override_dict = literal_eval(request.form.get("override_dict", str(dummy_dict)))
        if taskName is None and tasktype is None:
            return dumps({"result": "nok", "error": "query got empty taskname & type"})
        jobs = Job.objects.filter(title=taskName, type=tasktype)
        if jobs.count():
            logger.debug("Found job")
            job = jobs[0]
            site = job.execution_site
            try:
                dout = job.getBody()
            except Exception as err:
                logger.error("JobInstanceView:POST: %s",err)
                return dumps({"result": "nok", "error": err})
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
                        logger.error("JobInstanceView:POST: %s",err)
                        return dumps({"result": "nok", "error": str(err)})
                    logger.debug("JobInstanceView:POST: added instance %i to job %s", (j + 1), job.id)
            # print len(job.jobInstances)
            job.save()
            return dumps({"result": "ok"})
        else:
            logger.error("JobInstanceView:POST: Cannot find job")
            return dumps({"result": "nok", "error": 'Could not find job %s' % taskName})

class SetJobStatus(MethodView):
    # these are helper methods to reduce complexity of POST    
    def __extractBatchId__(self,bId):
        logger.debug("SetJobStatus:POST: batchId passed to DB %s", bId)        
        if bId is None: return bId
        elif isinstance(bId,int): 
            logger.debug("SetJobStatus:POST: batchId already type of int, not doing anything") 
        else:
            res = findall(r"\d+", str(bId))
            if len(res):
                bId = int(res[0])
        logger.debug("SetJobStatus:POST: batchId: %s", bId)
        return bId
    
    def __readArgs__(self,request):
        arguments = loads(request.form.get("args", "{}"))
        logger.debug("SetJobStatus:POST: arguments %s,",str(arguments))
        if not len(arguments.keys()):
            logger.debug("SetJobStatus:POST: request appears empty, checking args")
            try:
                logger.debug("SetJobStatus:POST: request.json %s",request.json)
                arguments = request.json.get("args",{})
            except Exception as err:
                raise Exception("CRITICAL error reading arguments")
        logger.debug("SetJobStatus:POST: request arguments %s", str(arguments))
        if not isinstance(arguments, dict):
            err = "arguments MUST be dictionary."
            raise Exception(err)
        return arguments
    def __readBdy__(self,arguments):
        dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
        bdy = literal_eval(arguments.get("body", str(dummy_dict)))
        if 'body' in arguments: del arguments['body']
        logger.debug("SetJobStatus:POST: BODY: %s (type %s)", bdy, type(bdy))
        return bdy
    def __rollBack__(self,job,inst_id,body=None):
        """ only the body is taken from the method, everything else is flushed """
        logger.debug("SetJobStatus:POST: requesting roll back for job %s and instance %i",job.title,inst_id)
        if not isinstance(job,Job):
            raise Exception("must be a valid Job instance.")
        try:
            query = JobInstance.objects.filter(job=job,instanceId=inst_id)
            update_dict = {
                        "created_at"    : datetime.now(),
                        "last_update"   : datetime.now(),
                        "batchId"       : None,
                        "Nevents"       : 0,
                        "hostname"      : None,
                        "status"        : "New",
                        "minor_status"  : "AwaitingBatchSubmission",
                        "status_history": [],
                        "memory"        : [],
                        "cpu"           : [],
                        "log"           : ""
                           }
            res = query.update(**update_dict)
            if not res:
                raise Exception("error while updating instance")
            # at this stage the instance MUST exist...
            inst = query.first()
            inst.reload()
            if body is not None:
                inst.setBody(body)
                inst.getResourcesFromMetadata()
        except Exception as err:
            raise Exception(err)        
        return dumps({"result": "ok"}) 
           
    def post(self):
        #dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
        # extract arguments
        arguments = {}
        try:
            # this one may throw, but this is caught.
            arguments = self.__readArgs__(request)
            # otherwise, move on...
            major_status = arguments.get("major_status", None)
            if major_status is None:
                raise Exception("SetJobStatus:POST: couldn't find major_status in arguments")
            # keep going...
            t_id = arguments.get("t_id", None)
            if t_id is None:
                raise Exception("no task ID provided")
            inst_id = arguments.get("inst_id", None)
            if inst_id is None:
                raise Exception("no instance ID provided")    
            # additional information, not critical
            site = str(arguments.get("site", "None"))
            minor_status = arguments.get("minor_status", None)
            jInstance = None
            body = self.__readBdy__(arguments)
            if "body" in arguments:
                del arguments["body"]
            # this might throw, but that's caught down-stream...
            job = Job.objects.get(id=t_id) 
            # this one either throws an exception, which is caught down stream, or returns a valid json.
            if major_status == "New":
                return self.__rollBack__(job,inst_id,body=body)
            # check for batchId, not used in query.
            bId = arguments.get("batchId",None)
            try: 
                bId = self.__extractBatchId__(bId)
                arguments["batchId"]=bId
            except Exception as err:
                raise Exception("error extracting batchId,\n%s"%err)                        
            query = {"job":job, "instanceId":inst_id}
            if site != "None": query['site']=site
            # again, this may throw...
            logger.debug("SetJobStatus:POST: query to find instance %s",str(query))
            jInstance = JobInstance.objects.get(**query) 
            # now here we can update stuff...
            logger.debug("SetJobStatus:POST: found instance %s",str(jInstance))
            logger.debug("SetJobStatus:POST: arguments in request %s",str(arguments))
            
            oldStatus = jInstance.status
            minorOld = jInstance.minor_status
            if minor_status is not None and minor_status != minorOld:
                logger.debug("SetJobStatus:POST: updating minor status")
                jInstance.set("minor_status", minor_status)
                del arguments['minor_status']
            if oldStatus != major_status:
                jInstance.setStatus(major_status)            
            jInstance.setBody(body) # again, this could throw...
            for key in ["t_id", "inst_id", "major_status"]:
                if key in arguments: del arguments[key]  
            
            # for the rest, we can just use the setters.
            for key, value in arguments.iteritems():
                jInstance.set(key, value)

        except Exception as err:
            logger.exception("SetJobStatus:POST: %s",err)
            return dumps({"result": "nok", "error": str(err)})
        return dumps({"result": "ok"})

    def get(self):
        queried_instances = output = []
        logger.debug("SetJobStatus:GET: request %s", str(request))
        jtype = unicode(request.form.get("type", "Generation"))
        stat = unicode(request.form.get("stat", "Any"))
        instId = int(request.form.get("inst", -1))
        n_min = int(request.form.get("n_min", -1))
        n_max = int(request.form.get("n_max", -1))
        title = unicode(request.form.get("title", None))
        try:
            if title is None: 
                raise Exception("missing title in jobStatusQuery")
            # this might throw...
            job = Job.objects.get(title=title, type=jtype)
            logger.debug("SetJobStatus:GET: job found %s", str(job))
            query = {"job":job}
            if stat != "Any": query["status"]=stat
            if instId != -1 : query["instanceId"]=instId
            instances = JobInstance.objects.filter(**query)
            if not instances.count():
                raise Exception("could not find any job instances matching query")
            if instances.count() == 1:
                queried_instances.append(instances.first())
            else:
                for inst in instances:
                    keep = True
                    this_id = inst.instanceId
                    if n_min != -1 and this_id <= n_min:
                        keep = False
                    if n_max != -1 and this_id > n_max:
                        keep = False
                    if not keep: continue
                    queried_instances.append(inst)
            output = [{"instanceId": q.instanceId, "jobId": str(q.job.id)} for q in queried_instances]
        except Exception as err:
            logger.error("SetJobStatus:GET: %s",err)
            return dumps({"result":"nok","error":str(err)})
        return dumps({"result": "ok", "jobs": output})

class NewJobs(MethodView):
    def get(self):
        logger.debug("NewJobs:GET: request %s", str(request))
        jstatus = unicode(request.form.get("status", u"New"))
        batchsite = unicode(request.form.get("site", "local"))
        _limit = int(request.form.get("limit", 1000))
        newJobInstances = []
        allJobs = Job.objects.filter(execution_site=batchsite)
        logger.debug("NewJobs:GET: allJobs = %s", str(allJobs))
        for job in allJobs:
            logger.debug("NewJobs:GET: processing job %s", job.slug)
            dependent_tasks = job.getDependency()
            logger.debug("NewJobs:GET: dependent tasks: %s", dependent_tasks)
            newJobs = JobInstance.objects.filter(job=job, status=jstatus).limit(int(_limit))
            logger.debug("#newJobs: %i", newJobs.count())
            if newJobs.count():
                logger.debug("NewJobs:GET: found %i new instances for job %s", newJobs.count(), str(job.title))
                dJob = DmpJob(job.id, body=None, title=job.title)
                logger.debug("NewJobs:GET: DmpJob instantiation.")
                for dt in dependent_tasks:
                    logger.debug("NewJobs:GET: processing dependencies for job %s", dt)
                    dJob.InputFiles += [{"source": fil, "target": basename(fil)} for fil in dt.getOutputFiles()]
                    dJob.MetaData += [{"name": k, "value": v, "type": "string"} for k, v in
                                      dt.getMetaDataVariables().iteritems()]
                logger.debug("NewJobs:GET: setBodyFromDict")
                dJob.setBodyFromDict(job.getBody())
                for j in newJobs:
                    if j.checkDependencies():
                        logger.debug("NewJobs:GET: depedency satisfied")
                        j.getResourcesFromMetadata()
                        logger.debug("NewJobs:GET: resources read out")
                        dInstance = deepcopy(dJob)
                        dInstance.setInstanceParameters(j.instanceId, j.body)
                        logger.debug('NewJobs:GET: ** DEBUG ** instance body : %s',j.body)
                        newJobInstances.append(dInstance.exportToJSON())
                    else:
                        logger.info("NewJobs:GET: dependencies not fulfilled yet")
                logger.debug("NewJobs:GET: found %i new jobs after dependencies", len(newJobInstances))
        return dumps({"result": "ok", "jobs": newJobInstances})

class TestView(MethodView):
    def post(self):
        logger.debug("TestView:POST: request form %s", str(request.form))
        version = str(request.form.get("version","None"))
        hostname = str(request.form.get("hostname", "None"))
        proc = str(request.form.get("process","default"))
        timestamp = datetime.now()
        logger.debug("TestView:POST: hostname: %s timestamp: %s ", hostname, timestamp)
        try:
            if (hostname == "None") or (timestamp == "None"):
                raise Exception("request empty")
            query = {}
            if proc  != "default": query['process'] = proc
            if hostname != 'None': query['hostname']=hostname
            q = HeartBeat.objects.filter(**query)
            query["timestamp"] = timestamp
            query["version"]   = version
            if not q.count():
                HB = HeartBeat(**query)
                HB.save()
            else:
                q.update(**query)
        except Exception as err:
            logger.error("TestView:POST: %s",err)
            return dumps({"result":"nok","error":str(err)})
        return dumps({"result": "ok"})

    def get(self):
        limit = int(request.form.get("limit", 1000))
        beats = []
        try:
            beats = HeartBeat.objects.all().limit(limit)
            logger.debug("TestView:POST: found %i heartbeats", beats.count())
        except Exception as ex:
            logger.error("TestView:GET: failure during HeartBeat GET test. \n%s", ex)
            return dumps({"result": "nok", "error": ex})
        return dumps({"result": "ok", "beats": [b.hostname for b in beats]})

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
                return dumps({"result": "nok", "error": "called register but file apparently exists already"})
        else:
            df = DataFile(filename=filename, site=site, status="New", filetype=filetype)
            df.save()
            return None

    def __update_or_remove__(self, df, status=None, action="setStatus"):
        if status is None:
            return dumps({"result": "nok", "error": "status is None"})
        if action == 'setStatus':
            df.setStatus(status)
            df.update()
        else:
            logger.info("requested removal!")
            df.delete()
        return None

    def post(self):
        logger.debug("DataCatalog:POST: request form %s", str(request.form))
        filename = str(request.form.get("filename", "None"))
        site = str(request.form.get("site", "None"))
        action = str(request.form.get("action", 'register'))
        status = str(request.form.get("status", "New"))
        filetype = str(request.form.get("filetype", "root"))
        force = bool(request.form.get("overwrite", "False"))
        logger.debug("DataCatalog:POST: filename %s status %s", filename, status)
        if action not in ['register', 'setStatus', 'delete']:
            logger.error("DataCatalog:POST: action not supported")
            return dumps({"result": "nok", "error": "action not supported"})
        if site == "None" and filename == "None":
            logger.error("DataCatalog:POST: request empty")
            return dumps({"result": "nok", "error": "request empty"})
        try:
            df = None
            files = [filename]
            if "," in filename:
                files = filename.split(",")
                logger.info("DataCatalog:POST: bulk request, found %i files", len(files))
            touched_files = []
            for filename in files:
                fileQuery = DataFile.objects.filter(filename=filename, site=site, filetype=filetype)
                if action == 'register':
                    logger.debug("DataCatalog:POST: request a new file to be registered")
                    res = self.__register__([fileQuery, filename, site, filetype, force])
                    if res is not None: return res
                else:
                    if fileQuery.count():
                        df = fileQuery[0]
                        res = self.__update_or_remove__(df, status=status, action=action)
                        if res is not None: return res
                    else:
                        logger.debug("DataCatalog:POST: cannot find queried input file")
                        return dumps({"result": "nok", "error": "cannot find file in DB"})
                if df is not None: touched_files.append(df)
        except Exception as ex:
            logger.error("DataCatalog:POST: %s", ex)
            return dumps({"result": "nok", "error": str(ex)})
        return dumps({"result": "ok",
                      "docId": [d.filename if action == 'delete' else str(d.id) for d in touched_files]})

    def get(self):
        limit = int(request.form.get("limit", 1000))
        site = str(request.form.get("site", "None"))
        status = str(request.form.get("status", "New"))
        filetype = str(request.form.get("filetype", "root"))
        try:
            dfs = DataFile.objects.filter(site=site, status=status, filetype=filetype).limit(limit)
            logger.debug("DataCatalog:GET: found %i files matching query", dfs.count())
        except Exception as ex:
            logger.error("DataCatalog:GET: %s", ex)
            return dumps({"result": "nok", "error": str(ex)})
        return dumps({"result": "ok", "files": [f.filename for f in dfs]})

# Register the urls
jobs.add_url_rule('/', view_func=ListView.as_view('list'))
jobs.add_url_rule('/stats', view_func=StatsView.as_view('stats'), methods=["GET"])
jobs.add_url_rule('/<slug>/', view_func=DetailView.as_view('detail'))
jobs.add_url_rule("/job/", view_func=JobView.as_view('jobs'), methods=["GET", "POST"])
jobs.add_url_rule('/jobInstances/detail', view_func=InstanceView.as_view('instanceDetail'))
jobs.add_url_rule("/jobInstances/", view_func=JobInstanceView.as_view('jobinstances'), methods=["GET", "POST"])
jobs.add_url_rule("/jobstatus/", view_func=SetJobStatus.as_view('jobstatus'), methods=["GET", "POST"])
jobs.add_url_rule("/newjobs/", view_func=NewJobs.as_view('newjobs'), methods=["GET"])
jobs.add_url_rule("/testDB/", view_func=TestView.as_view('testDB'), methods=["GET", "POST"])
jobs.add_url_rule("/datacat/", view_func=DataCatalog.as_view('datacat'), methods=["GET", "POST"])
