'''
Created on Mar 25, 2016

@author: zimmer
'''
from core.models import Job, JobInstance
from core import db
import random

if __name__ == '__main__':
    db.connect()
    for i in range(20): 
        job = Job(title="testJob-%i"%i,slug='test%i'%i,body="some test job")
        for j in range(random.randrange(20)):
            jI = JobInstance(body="some extra values %i"%j)
            job.jobInstances.append(jI)
        job.save()
    