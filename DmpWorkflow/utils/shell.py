"""
Created on Mar 22, 2016

@author: zimmer
"""
import logging
from subprocess import PIPE, Popen
from os import chmod, stat, environ, remove
from os.path import expandvars

logger = logging.getLogger("core")
#FIXME: change to interleave stdout & stderr
# http://stackoverflow.com/questions/6809590/merging-a-python-scripts-subprocess-stdout-and-stderr-while-keeping-them-disti
def run(cmd_args, useLogging=True, suppressErrors=False):
    if not isinstance(cmd_args, list):
        raise RuntimeError('must be list to be called')
    logger.info("attempting to run: %s",str(cmd_args))
    proc = Popen(cmd_args, stdout=PIPE, stderr=PIPE, shell=True)
    (out, err) = proc.communicate()
    rc = proc.returncode
    if rc:
        for e in err.split("\n"):
            if len(e):
                if suppressErrors: continue
                if useLogging:
                    logger.error(e)
                else:
                    print e
    return out, err, rc

def make_executable(path):
    mode = stat(path).st_mode
    mode |= (mode & 0o444) >> 2    # copy R bits to X
    chmod(path, mode)

def source_bash(setup_script):
    foop = open("tmp.sh", "w")
    foop.write("#/bin/bash\nsource $1\nenv|sort")
    foop.close()
    out, err, rc = run(["bash tmp.sh %s" % expandvars(setup_script)], useLogging=False, suppressErrors=True)
    if rc:
        print 'source encountered error, returning that one'
        return err
    lines = [l for l in out.split("\n") if "=" in l]
    keys, values = [], []
    for l in lines:
        tl = l.split("=")
        if len(tl) == 2:
            if tl[0].startswith("_"):
                continue
            if tl[0].startswith("BASH_FUNC"):
                continue
            keys.append(tl[0])
            values.append(tl[1])
    environ.update(dict(zip(keys, values)))
    remove("tmp.sh")
    return
