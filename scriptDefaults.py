'''
Created on Apr 20, 2016

@author: zimmer
@brief: prototype script that handles config parsing etc.

'''
import ConfigParser, os
from utils.shell import source_bash
myDefaults = {
              "DAMPE_SW_DIR":".",
              "ExternalsScript":"${DAMPE_SW_DIR}/setup/setup.sh",
              "use_debugger":True,
              "use_reloader":True,
              "task_types":"Generation,Digitization,Reconstruction,User,Other".split(","),
              "task_major_statii":"New,Running,Failed,Terminated,Done,Submitted,Suspended".split(",")
              }



cfg = ConfigParser.SafeConfigParser(defaults=myDefaults)
cfg.read(os.getenv("WorkflowConfig","config/dampe.cfg"))
os.environ["DAMPE_SW_DIR"]=cfg.get("site","DAMPE_SW_DIR")

# next set up externals
source_bash(cfg.get("site","ExternalsScript"))

pwd = os.getenv("PWD",".")
os.chdir(os.getenv("DWF_ROOT"))
# this one sources flask
source_bash("setup.sh")

## done with that.