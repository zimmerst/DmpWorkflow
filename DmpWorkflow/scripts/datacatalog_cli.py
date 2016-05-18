'''
Created on May 18, 2016

@author: zimmer
@brief: datacatalog script
'''
from requests import post, get
from os.path import expandvars, abspath
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL

def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s [options]", description="query datacatalog")
    
    parser.add_argument("-a", "--action", dest="action", type = str, default="None", help='action')
    parser.add_argument("-s", "--site", dest="site", type = str, default="None", help='site where the file is registered')
    parser.add_argument("-f", "--filename", dest="filename", type = str, default="None", help='complete filename')
    parser.add_argument("-t", "--filetype", dest="filetype", type = str, default="root", help='type')
    parser.add_argument("-S", "--setStatus", dest="status", type = str, default="New", help='site where the file is registered')
    
    parser.add_argument("-x", "--expandVars", dest="expandVars", action = 'store_true', default=False, help='if true, store absolute paths')
    parser.add_argument("-l", "--limit", dest="limit", type= int , default=100, help='limit list of entries returned')

    opts = parser.parse_args(args)
    assert opts.action in ['register','setStatus','delete','list'], "action not supported"
    action   = opts.action
    site     = opts.site
    filename = expandvars(opts.filename) if opts.expandVars else opts.filename   
    filetype = opts.filetype
    status   = opts.status 
    try:
        res = None
        if opts.action == 'list':
            res = get("%s/datacat/" % DAMPE_WORKFLOW_URL, data = {"site":site, 
                                                                  "status":status,
                                                                  "filetype":filetype,
                                                                  "limit":opts.limit})
        else:
            dd = {"site":opts.site, "action":action,"filetype": filetype, "status":status}
            if action == 'register': 
                dd['filename']=abspath(filename)
                dd['status']='New'
            res = post("%s/datacat/" % DAMPE_WORKFLOW_URL, data = dd)
        if res is None: return
        res.raise_for_status()
        result = res.json()
        if result.get("result", "nok") == "ok":
            if action == 'list':
                files = result.get("files",[])
                print 'found %i files'%len(files)
                for f in files: print f
            else: print "POST %s %s"%(action,result.get("docId","NONE"))
                
    except Exception as err:
        print 'ERROR: %s'%err          

if __name__ == "__main__":
    main()
