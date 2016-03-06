'''
Created on Mar 1, 2016

@author: zimmer
'''
from flask import Blueprint, request, redirect, render_template, url_for
from flask.ext.mongoengine.wtf import model_form
from flask.views import MethodView
from core.models import Job, JobInstance

jobs = Blueprint('jobs', __name__, template_folder='../html/templates')

class ListView(MethodView):

    def get(self):
        jobs = Job.objects.all()
        return render_template('jobs/list.html', jobs=jobs)


class DetailView(MethodView):

    def get(self, taskName):
        job = Job.objects.get_or_404(taskName=taskName)
        return render_template('jobs/detail.html', job=job)

class DetailView(MethodView):

    form = model_form(JobInstance, exclude=['created_at'])

    def get_context(self, taskName):
        job = Job.objects.get_or_404(taskName=taskName)
        form = self.form(request.form, csrf_enabled=False)

        context = {
            "job": job,
            "form": form
        }
        return context

    def get(self, taskName):
        context = self.get_context(taskName)
        return render_template('jobs/detail.html', **context)

    def job(self, taskName):
        context = self.get_context(taskName)
        form = context.get('form')

        if form.validate():
            instance = JobInstance()
            form.populate_obj(instance)

            job = context.get('job')
            job.comments.append(instance)
            job.save()

            return redirect(url_for('jobs.detail', taskName=taskName))
        return render_template('jobs/detail.html', **context)
# Register the urls
jobs.add_url_rule('/', view_func=ListView.as_view('list'))
jobs.add_url_rule('/<taskName>/', view_func=DetailView.as_view('detail'))