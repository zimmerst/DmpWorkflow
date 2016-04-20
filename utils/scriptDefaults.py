"""
Created on Apr 20, 2016

@author: zimmer
@brief: prototype script that handles config parsing etc.

"""
import ConfigParser, os, sys
from utils.tools import exceptionHandler
from utils.shell import source_bash

myDefaults = {
    "DAMPE_SW_DIR": ".",
    "ExternalsScript": "${DAMPE_SW_DIR}/setup/setup.sh",
    "use_debugger": "true",
    "use_reloader": "true",
    "traceback": "true",
    "task_types": "Generation,Digitization,Reconstruction,User,Other".split(","),
    "task_major_statii": "New,Running,Failed,Terminated,Done,Submitted,Suspended".split(",")
}

cfg = ConfigParser.SafeConfigParser(defaults=myDefaults)
cfg.read(os.getenv("WorkflowConfig", "config/dampe.cfg"))
os.environ["DAMPE_SW_DIR"] = cfg.get("site", "DAMPE_SW_DIR")

# print "seting up externals"
source_bash(cfg.get("site", "ExternalsScript"))

WorkflowRoot = os.getenv("DWF_ROOT", os.getenv("DAMPE_SW_DIR"))
# print "ROOT workflow: %s"%WorkflowRoot

pwd = os.getenv("PWD", ".")
# print "current path %s"%os.path.abspath(pwd)

dbg = cfg.getboolean("site", "traceback")
if not dbg:
    sys.excepthook = exceptionHandler


    # print "seting up flask"
    # activate_this_file = os.path.expandvars("${DWF_ROOT}/bin/activate")
    # os.system('/bin/bash --rcfile %s'%activate_this_file)

    # print 'calling execfile(%s)'%activate_this_file
    # execfile(activate_this_file, dict(__file__=activate_this_file))

    ## done with that.
