'''
Created on Jun 9, 2016
@author: zimmer
@brief:  performs aggregation of jobs in various statii across all sites
'''

from argparse import ArgumentParser
from os.path import isfile
from DmpWorkflow.config.defaults import SITES as batchSites
from DmpWorkflow.config.defaults import MAJOR_STATII as statii
from DmpWorkflow.core.models import JobInstance
from datetime import datetime
from json import dumps, loads

def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s [options]", description="query datacatalog")
    parser.add_argument("-o", "--output", dest="output", type = str, default="jobMonitor.json", help='name of output file')
    opts = parser.parse_args(args)
    out = {site:[] for site in batchSites}
    if isfile(opts.output):
        out = loads(open(opts.output,'r').read())
    else:
        fout = open(opts.output,'w')
    ts = datetime.now()
    for site in batchSites:
        status_dict = {key:0 for key in statii}
        stats = JobInstance.objects.filter(site=site).item_frequencies("status")
        status_dict.update(stats)
        out[site].append({"time":ts.isoformat(),"statii":status_dict})
    fout.write(dumps(out))
    fout.close()
    
if __name__ == "__main__":
    main()


