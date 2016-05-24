"""
Created on Mar 22, 2016

@author: zimmer
"""
import logging
from subprocess import PIPE, Popen
from select import poll as spoll, POLLIN, POLLHUP

from os import chmod, stat, environ, remove
from os.path import expandvars

logger = logging.getLogger("core")
def run(cmd_args, useLogging=True, suppressErrors=False, interleaved=True):
    # inspired from http://tinyurl.com/hslhjfe (StackOverflow)
    if not isinstance(cmd_args, list):
        raise RuntimeError('must be list to be called')
    logger.info("attempting to run: %s",str(cmd_args))
    errors = []
    output = []
    tsk = Popen(cmd_args,stdout=PIPE,stderr=PIPE)
    poll = spoll()
    poll.register(tsk.stdout,POLLIN | POLLHUP)
    poll.register(tsk.stderr,POLLIN | POLLHUP)
    pollc = 2
    events = poll.poll()
    while pollc > 0 and len(events) > 0:
        for rfd, event in events:
            if event & POLLIN:
                if rfd == tsk.stdout.fileno():
                    line = tsk.stdout.readline()
                    if len(line) > 0:
                        if useLogging: logger.info(line[:-1])
                        output.append("INFO: %s"%str(line[:-1]))
                if rfd == tsk.stderr.fileno():
                    line = tsk.stderr.readline()
                    if len(line) > 0:
                        if suppressErrors: continue
                        errors.append(line[:-1])
                        if interleaved: output.append(errors[-1])
                        if useLogging: logger.error(errors[-1])
            if event & POLLHUP:
                poll.unregister(rfd)
                pollc = pollc - 1
            if pollc > 0: events = poll.poll()
    rc=tsk.wait()
    return "\n".join(output), "\n".join(errors), rc

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
