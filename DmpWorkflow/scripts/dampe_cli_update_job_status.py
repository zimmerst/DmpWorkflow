"""
Created on Mar 15, 2016

@author: zimmer
"""
import requests
import json
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL

def main(args=None):
    usage = "Usage: %(prog)s JobID InstanceID status [options]"
    description = "update job in DB"
    parser = ArgumentParser(usage=usage, description=description)
    parser.add_argument("t_id", help="task ID")
    parser.add_argument("inst_id", help="Instance ID")
    parser.add_argument("major_status", help='Major status')
    parser.add_argument("--minor_status", dest="minor_status", type=str, default=None, help='minor status', required=False)
    parser.add_argument("--hostname", dest="hostname", type=str, default=None, help='hostname', required=False)
    parser.add_argument("--batchId", dest="batchId", type=str, default=None, help='batchId', required=False)
    opts = parser.parse_args(args)
    # if len(sys.argv)!=3:
    #    print parser.print_help()
    #    raise Exception

    my_dict = {}
    for key in opts.__dict__:
        if opts.__dict__[key] is not None:
            my_dict[key] = opts.__dict__[key]
    res = requests.post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args": json.dumps(my_dict)})
    res.raise_for_status()
    res = res.json()
    if res.get("result", "nok") != "ok":
        print 'error %s' % res.get("error")
    else:
        print 'Status updated'
    # update_status(JobId, InstanceId, str(major_status), **my_dict)


if __name__ == '__main__':
    main()
