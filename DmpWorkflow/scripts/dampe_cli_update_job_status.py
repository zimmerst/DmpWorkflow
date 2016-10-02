"""
Created on Mar 15, 2016

@author: zimmer
"""
from requests import post
from requests.exceptions import HTTPError
from json import dumps
from sys import exit as sys_exit
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL
from DmpWorkflow.utils.tools import sleep

def main(args=None):
    usage = "Usage: %(prog)s JobID InstanceID status [options]"
    description = "update job in DB"
    parser = ArgumentParser(usage=usage, description=description)
    parser.add_argument("--t_id", dest='t_id', help="task ID")
    parser.add_argument("--inst_id", dest='inst_id', help="Instance ID")
    parser.add_argument("--retry", dest='retry', type=int, default=5, help="number of attempts being made to contact server")
    parser.add_argument("--major_status", dest='major_status', help='Major status')
    parser.add_argument("--minor_status", dest="minor_status", type=str, default=None, help='minor status',
                        required=False)
    parser.add_argument("--hostname", dest="hostname", type=str, default=None, help='hostname', required=False)
    parser.add_argument("--batchId", dest="batchId", type=str, default=None, help='batchId', required=False)
    opts = parser.parse_args(args)
    my_dict = {}
    for key in opts.__dict__:
        if opts.__dict__[key] is not None:
            my_dict[key] = opts.__dict__[key]
    natts = my_dict.get("retry",3)
    if 'retry' in my_dict: my_dict.pop("retry")
    res = None
    counter = 0
    while natts >= counter:
        res = post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args": dumps(my_dict)}, timeout=30.)
        try:
            res.raise_for_status()
        except HTTPError as err:
            counter+=1
            slt = 60*counter
            print err
            print '%i/%i: could not complete request, sleeping %i seconds and retrying again'%(counter, natts, slt)
            sleep(slt)
            res = None
        finally:
            if res is None and natts == 0:
                print 'exiting process'
                sys_exit(0)
    res = res.json()
    if res.get("result", "nok") != "ok":
        print 'error %s' % res.get("error")
    else:
        print 'Status updated'


if __name__ == '__main__':
    main()
