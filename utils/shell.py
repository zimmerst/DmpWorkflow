'''
Created on Mar 22, 2016

@author: zimmer
'''
import logging, subprocess, os

def run(cmd_args):
    err = None
    if not isinstance(cmd_args, list):
        raise RuntimeError('must be list to be called')
    logging.info("attempting to run: %s"%" ".join(cmd_args))
    proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    rc = proc.returncode
    if not err is None:
        for e in err.split("\n"): logging.error(e)
    return out, err, rc

def source_bash(setup_script):
    foo = open("tmp.sh","w")
    foo.write("#/bin/bash\nsource $1\nenv|sort")
    foo.close()
    old_env = os.environ
    out, err, rc = run(["bash tmp.sh %s"%setup_script])
    return (out, err, rc)