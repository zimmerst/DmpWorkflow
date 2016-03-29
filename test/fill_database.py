'''
Created on Mar 25, 2016

@author: zimmer
'''
from core.models import Job, JobInstance, TYPES
from core import db
import random

jobs = 10
instances = 2000


if __name__ == '__main__':
    counter = 0
    releases = ['5-1-1','4-5-5','5-1-2','5-1-2']
    db.connect()
    for i in range(jobs): 
        job = Job(title="testJob-%i"%i,body="some test job", type=random.choice(TYPES),
                  release="DmpSoftware-%s"%random.choice(releases))
        for j in range(random.randrange(instances)):
            jI = JobInstance()
            job.jobInstances.append(jI)
        counter+=len(job.jobInstances)
        job.save()
    print 'added %i new items'%counter
    