"""
Created on Aug 17, 2016
@author: zimmer
@brief:  aggregator, to be run on the server; queries DB and performs accounting calculations on jobs in final state
"""
from argparse import ArgumentParser
from DmpWorkflow.core.models import JobInstance
from mongoengine import Q
import progressbar


def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s [options]", description="aggregate old jobs")
    opts = parser.parse_args(args)
    # find all objects which have cpu & memory with size != 0
    query = JobInstance.objects(Q(cpu__not__size=0) & Q(memory__not__size=0) & \
                                (Q(status__exact="Done") | Q(status__exact="Failed") | Q(status__exact="Terminated")) & \
                                (Q(cpu_max_job__ne=-1) | Q(mem_max_job__ne=-1)))
    print 'found %i objects satifsifying query'%(query.count())
    pb = progressbar.ProgressBar(maxval=query.count())
    pb.start()
    for jInst in query.all():
        jInst.__aggregateResources__()
        jInst.update()
    pb.finish()
    print 'done'

if __name__ == "__main__":
    main()

