'''
Created on Mar 25, 2016

@author: zimmer
'''
import random, string, os, os.path

def random_string_generator(size=16, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


def makeSafeName(srcname):
    rep = {".":"d","+":"p","-":"n"}
    for key in rep:
        srcname = srcname.replace(key,rep[key])
    return srcname

def pwd():
    # Careful, won't work after a call to os.chdir...
    return os.environ['PWD']

def mkdir(dir):
    if not os.path.exists(dir):  os.makedirs(dir)
    return dir

def rm(pwd):
    os.system("rm -rf %s"%pwd)

def mkscratch():
    if os.path.exists('/scratch/'):    
        return(mkdir('/scratch/%s/'%os.environ['USER']))
    elif os.path.exists('/tmp/'):
        return(mkdir('/tmp/%s/'%os.environ['USER']))
    else:
        raise Exception('...')

def touch(path):
    with open(path, 'a'):
        os.utime(path, None)