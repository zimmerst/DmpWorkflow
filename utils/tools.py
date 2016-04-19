'''
Created on Mar 25, 2016

@author: zimmer
'''
import random, string, os, os.path, time, shutil, shlex, subprocess as sub

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
        
def Ndigits(val,size=6):
    ''' returns a N-digit integer with leading zeros '''
    _sixDigit = "%i"%val
    while len(_sixDigit)<size: _sixDigit = "0"+_sixDigit
    return _sixDigit

def safe_copy(infile, outfile, sleep=10, attempts=10,debug=False):
    if debug: print 'cp %s -> %s'%(infile,outfile)
    infile = infile.replace("@","") if infile.startswith("@") else infile
    # Try not to step on any toes....
    sleep = parse_sleep(sleep)
    if infile.startswith("root:"):
        print 'file is on xrootd - switching to XRD library'
        cmnd = "xrdcp %s %s"%(infile,outfile)
    else:
        cmnd = "cp %s %s"%(infile,outfile)
    i = 1
    if debug: print "Attempting to copy file..."
    while i < attempts: 
        status = sub.call(shlex.split(cmnd))
        if status == 0: 
            return status
        else:
            print "%i - Copy failed; sleep %ss"%(i,sleep)
            time.sleep(sleep)
        i += 1
    raise IOError("Failed to copy file")

def parse_sleep(sleep):
    MINUTE=60
    HOUR=60*MINUTE
    DAY=24*HOUR
    WEEK=7*DAY
    if isinstance(sleep,float) or isinstance(sleep,int):
        return sleep
    elif isinstance(sleep,str):
        try: return float(sleep)
        except ValueError: pass
        
        if sleep.endswith('s'): return float(sleep.strip('s'))
        elif sleep.endswith('m'): return float(sleep.strip('m'))*MINUTE
        elif sleep.endswith('h'): return float(sleep.strip('h'))*HOUR
        elif sleep.endswith('d'): return float(sleep.strip('d'))*DAY
        elif sleep.endswith('w'): return float(sleep.strip('w'))*WEEK
        else: raise ValueError
    else:
        raise ValueError

def sleep(sleep):
    return time.sleep(parse_sleep(sleep))

def get_resources():
    import resource
    usage=resource.getrusage(resource.RUSAGE_SELF)
    return '''usertime=%s systime=%s mem=%s mb
           '''%(usage[0],usage[1],
                (usage[2]*resource.getpagesize())/1000000.0 )
           
def camelize(myStr):
    d = "".join(x for x in str(myStr).title() if not x.isspace())    
    return d