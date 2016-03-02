'''
Created on Mar 1, 2016

@author: zimmer
'''
from flask import Blueprint, request, redirect, render_template, url_for
from flask.views import MethodView
from core.schema import Job, JobInstance

jobs = Blueprint('jobs', __name__, template_folder='templates')


class ListView(MethodView):

    def get(self):
        posts = Post.objects.all()
        return render_template('posts/list.html', posts=posts)


class DetailView(MethodView):

    def get(self, slug):
        post = Post.objects.get_or_404(slug=slug)
        return render_template('posts/detail.html', post=post)


# Register the urls
jobs.add_url_rule('/', view_func=ListView.as_view('list'))
jobs.add_url_rule('/<taskName>/', view_func=DetailView.as_view('detail'))