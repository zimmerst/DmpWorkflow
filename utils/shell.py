'''
Created on Mar 22, 2016

@author: zimmer
'''
import logging, subprocess, os

def run(cmd_args,useLogging=True):
    if not isinstance(cmd_args, list):
        raise RuntimeError('must be list to be called')
    logging.info("attempting to run: %s"%" ".join(cmd_args))
    proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    rc = proc.returncode
    if not err is None:
        for e in err.split("\n"): 
            if useLogging: logging.error(e)
            else: print e
    return out, err, rc

def source_bash(setup_script):
    foo = open("tmp.sh","w")
    foo.write("#/bin/bash\nsource $1\nenv|sort")
    foo.close()
    out, err, rc = run(["bash tmp.sh %s"%setup_script],useLogging=False)
    if rc:
        print 'source encountered error, returning that one'
        return err
    lines = [l for l in out.split("\n") if "=" in l]
    keys, values = [],[]
    for l in lines: 
        tl = l.split("=")
        if len(tl)==2:
            keys.append(tl[0])
            values.append(tl[1])
    os.environ.update(dict(zip(keys,values)))