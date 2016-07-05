"""
Created on Jun 1, 2016

@author: zimmer
"""

from timeit import timeit

number = 10  # 10 executions per command

# first let's do a check how long it takes to retrieve the total number of events per job

header = "from DmpWorkflow.core.models import Job, JobInstance; jobs = Job.objects.all();"
tests = ['Query nEvents', 'Query Instances']
slow_way = ["jobs[0].getNevents();", "len(jobs[0].jobInstances);"]

fast_way = ['JobInstance.objects.filter(job=jobs[0]).aggregate_sum("Nevents");',
            'JobInstance.objects.filter(job=jobs[0]).count();']

print 'testing - patience'
for i in xrange(len(slow_way)):
    slow = timeit(header + slow_way[i], number=number) / float(number)
    fast = timeit(header + fast_way[i], number=number) / float(number)
    print 'test %i "%s": normal query: %1.2f s fast query: %1.2f s  improvement: %d x' % (i, tests[i], slow, fast,
                                                                                          (slow / fast))

print 'test done'
