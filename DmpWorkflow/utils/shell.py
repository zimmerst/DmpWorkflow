"""
Created on Mar 22, 2016

@author: zimmer
"""
import logging
import subprocess
import os


def run(cmd_args, useLogging=True, suppressErrors=False):
    if not isinstance(cmd_args, list):
        raise RuntimeError('must be list to be called')
    logging.info("attempting to run: %s",str(cmd_args))
    proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    rc = proc.returncode
    if rc:
        for e in err.split("\n"):
            if len(e):
                if suppressErrors: continue
                if useLogging:
                    logging.error(e)
                else:
                    print e
    return out, err, rc


def source_bash(setup_script):
    foop = open("tmp.sh", "w")
    foop.write("#/bin/bash\nsource $1\nenv|sort")
    foop.close()
    out, err, rc = run(["bash tmp.sh %s" % os.path.expandvars(setup_script)], useLogging=False, suppressErrors=True)
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
    os.environ.update(dict(zip(keys, values)))
    os.remove("tmp.sh")
    return
