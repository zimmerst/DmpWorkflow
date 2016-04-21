from flask import Blueprint, request, redirect, render_template, url_for
from flask.ext.mongoengine.wtf import model_form
from flask.views import MethodView
from DmpWorkflow.core.models import Job, JobInstance

jobs = Blueprint('jobs', __name__, template_folder='templates')


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


# Register the urls
jobs.add_url_rule('/', view_func=ListView.as_view('list'))
jobs.add_url_rule('/<slug>/', view_func=DetailView.as_view('detail'))
