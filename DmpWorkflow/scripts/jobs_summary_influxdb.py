'''
Created on Aug 23, 2016

@author: zimmer
@brief: aggregates statii across all sites & reports to influxdb
'''
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import SITES as batchSites
from DmpWorkflow.config.defaults import MAJOR_STATII as statii
from DmpWorkflow.core.models import JobInstance
from sys import exit as sys_exit
from traceback import print_exc
from pprint import PrettyPrinter

influxdb = True
try:
    from influxdb import InfluxDBClient
except ImportError:
    influxdb = False
    print 'could not find influxdb, try to install via pip'

def __makeEntry__(stat,site,value):
    js = {
     "measurement":"instance_stats",
     "tags":{"site"  : site,
             "status":  stat
             },
     "fields": {"value": value}
     }
    return js


def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s [options]", description="query datacatalog")
    parser.add_argument("-H","--host",dest='host',help="hostname of influxdb instance")
    parser.add_argument("-u","--user",dest="user",help="username")
    parser.add_argument("-p","--password",dest="pw",help="password")
    parser.add_argument("-P","--port",dest="port",type=int,default=8086,help="influxdb ingest port")
    parser.add_argument("-n","--dbname", dest="dbname",help="name of DB to store data in.")
    parser.add_argument("-d", "--dry", dest="dry", action="store_true", default=False, type=bool, help="do not report results to grafana")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, type=bool, help="verbose mode")
    opts = parser.parse_args(args)
    json_bdy = []
    for site in batchSites:
        status_dict = {key: 0 for key in statii}
        stats = JobInstance.objects.filter(site=site).item_frequencies("status")
        status_dict.update(stats)
        for stat, freq in status_dict.iteritems():
            json_bdy.append(__makeEntry__(stat, site, freq))
    print 'found %i measurements to add'%len(json_bdy)
    pp = PrettyPrinter(indent=2)
    if opts.verbose: pp.pprint(json_bdy)
    if opts.dry:
        return
    if influxdb:
        client = InfluxDBClient(opts.host,opts.port,opts.user,opts.password,opts.dbname)
        client.create_database(opts.dbname)
        ret = client.write_points(json_bdy)
        if not ret: 
            try:
                raise Exception("Could not write points to DB")
            except:
                print_exc()
                sys_exit(int(ret))

if __name__ == "__main__":
    main()