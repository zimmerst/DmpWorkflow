import json
import logging
from flask import Blueprint, request, redirect, render_template, url_for
from flask.ext.mongoengine.wtf import model_form
from flask.views import MethodView, View
from DmpWorkflow.core.models import Job, JobInstance
from DmpWorkflow.utils.flask_helpers import parseJobXmlToDict, update_status

jobs = Blueprint('jobs', __name__, template_folder='templates')

logger = logging.getLogger("views")


class ListView(MethodView):
    def get(self):
        jobs = Job.objects.all()
        return render_template('jobs/list.html', jobs=jobs)


class DetailView(MethodView):
    form = model_form(JobInstance, exclude=['created_at', 'status_history'])

    def get_context(self, slug):
        job = Job.objects.get_or_404(slug=slug)
        form = self.form(request.form)

        context = {
            "job": job,
            "form": form
        }
        return context

    def get(self, slug):
        context = self.get_context(slug)
        return render_template('jobs/detail.html', **context)

    def post(self, slug):
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


class JobView(MethodView):
    def get(self):
        return "Nothing to display"

    def post(self):
        taskname = request.form['taskname']
        jobdesc = request.files['job_description']
        type = request.form['type']
        n_instances = request.form['n_instances']
        job = Job(title=taskname, body=jobdesc.read())
        dout = parseJobXmlToDict(job.body)
        if 'type' in dout['atts']:
            job.type = dout['atts']['type']
        if 'release' in dout['atts']:
            job.release = dout['atts']['release']
        if type is not None:
            job.type = type
        dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
        if n_instances:
            for j in range(n_instances):
                jI = JobInstance(body=str(dummy_dict))
                job.addInstance(jI)
        # print len(job.jobInstances)
        job.save()
        return json.dumps({"result": "ok", "jobID": str(job.id)})


class JobInstanceView(MethodView):
    def get(self):
        return 'Nothing yet'

    def post(self):
        taskName = request.form["taskname"]
        ninst = request.form['n_instances']
        jobs = Job.objects.filter(title=taskName)
        if len(jobs):
            logger.debug("Found job")
            job = jobs[0]
            dout = parseJobXmlToDict(job.body)
            if 'type' in dout['atts']:
                job.type = dout['atts']['type']
            if 'release' in dout['atts']:
                job.release = dout['atts']['release']
            dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
            if ninst:
                for j in range(ninst):
                    jI = JobInstance(body=str(dummy_dict))
                    # if opts.inst and j == 0:
                    #    job.addInstance(jI,inst=opts.inst)
                    # else:
                    job.addInstance(jI)
            # print len(job.jobInstances)
            job.update()
            return json.dumps({"result": "ok"})
        else:
            logger.error("Cannot find job")
            return json.dumps({"result": "ok", "message": 'Could not find job %s' % taskName})


class RefreshJobAlive(MethodView):
    def post(self):
        try:
            taskid = request.form["taskid"]
            instance_id = request.form["instanceid"]
            hostname = request.form["hostname"]
            status = request.form["status"]
            my_job = Job.objects.filter(id=taskid)
            jInstance = my_job.getInstance(instance_id)
            jInstance.set("hostname", hostname)
            oldStatus = jInstance.status
            if status != oldStatus:
                jInstance.setStatus(status)
            my_job.update()
            return json.dumps({"result": "ok"})
        except Exception as err:
            logger.exception(err)
            return json.dumps({"result": "nok", "error": "server error"})


class SetJobStatus(MethodView):
    def post(self):
        arguments = request.form['args']
        try:
            update_status(arguments['t_id'], arguments["inst_id"], arguments['major_status'], **arguments)
        except Exception as err:
            logger.exception(err)
            return json.dumps({"result": "nok", "error": str(err)})
        return json.dumps({"result": "ok"})

# Register the urls
jobs.add_url_rule('/', view_func=ListView.as_view('list'))
jobs.add_url_rule('/<slug>/', view_func=DetailView.as_view('detail'))
jobs.add_url_rule("/job/", view_func=JobView.as_view('jobs'), methods=["GET", "POST"])
jobs.add_url_rule("/jobInstances/", view_func=JobInstanceView.as_view('jobinstances'), methods=["GET", "POST"])
jobs.add_url_rule("/jobalive/", view_func=RefreshJobAlive.as_view('jobalive'), methods=["POST"])
jobs.add_url_rule("/jobstatus/", view_func=SetJobStatus.as_view('jobstatus'), methods=["POST"])
