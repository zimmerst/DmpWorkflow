'''
Created on Apr 19, 2016

@author: zimmer
@brief: payload script
'''

from core.DmpJob import createFromJSON
from utils.tools import mkdir, safe_copy, rm, camelize
from utils.shell import run
import sys, os, logging, pprint, socket
LOG_LEVEL = os.getenv("LOG_LEVEL","INFO")
FORMAT = '%(asctime)s %(levelname)s:%(message)s'
logging.basicConfig(format=FORMAT,level=LOG_LEVEL, datefmt='%m/%d/%Y %I:%M:%S %p')
log = logging.getLogger()

fii = sys.argv[1]
if os.path.isfile(fii):
    fii = open(fii,'rb').read()
log.info("reading json input")
job = createFromJSON(fii)
batchId = os.getenv("LSF_JOBID","1234")
job.updateStatus("Running","PreparingInputData",hostname=socket.gethostname(),batchId=batchId)
# first, set all variables 
for var in job.MetaData: os.environ[var['name']]=os.path.expandvars(var['value'])
log.debug("current environment settings {}".format(os.environ))

for fi in job.InputFiles:
    src = fi['source']
    tg  = fi['target']
    log.info("cp %s -> %s"%(src,tg))
    try:
        safe_copy(src, tg, attempts=3, sleep='4s')
    except IOError, e:        
        job.updateStatus("Failed",camelize(e))
        sys.exit(4)
log.info("successfully completed staging.")

# next, run the executable
foo = open('payload','w')
foo.write(job.exec_wrapper)
foo.close()
log.info("about to run payload")
CMD = "%s payload"%job.executable
log.info("CMD: %s"%CMD)
job.updateStatus("Running","ExecutingApplication")
output, error, rc = run([CMD])
if rc: 
    log.error("Payload returned exit code %i, see above for more details."%rc)
    job.updateStatus("Failed","ApplicationExitCode%i"%rc)
    #sys.exit(5)
log.info("successfully completed running application")

# finally, compile output file.
job.updateStatus("Running","PreparingOutputData")
for fi in job.OutputFiles:
    src = fi['source']
    tg  = fi['target']
    log.info("cp %s -> %s"%(src,tg))
    try:
        safe_copy(src, tg, attempts=3, sleep='4s')
    except IOError, e:        
        job.updateStatus("Failed",camelize(e))
        #sys.exit(6)
log.info("successfully completed staging.")
log.info("job complete")
updateStatus("Done","ApplicationComplete")