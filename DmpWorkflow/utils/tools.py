"""
Created on Mar 25, 2016

@author: zimmer
"""
import os
import os.path
import random
import shlex
import string
import subprocess as sub
import time
from StringIO import StringIO
from xml.dom import minidom as xdom

def exceptionHandler(exception_type, exception, traceback):
    # All your trace are belong to us!
    # your format
    print "%s: %s" % (exception_type.__name__, exception)


def random_string_generator(size=16, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def makeSafeName(srcname):
    rep = {".": "d", "+": "p", "-": "n"}
    for key in rep:
        srcname = srcname.replace(key, rep[key])
    return srcname


def pwd():
    # Careful, won't work after a call to os.chdir...
    return os.environ['PWD']


def mkdir(Dir):
    if not os.path.exists(Dir):
        os.makedirs(Dir)
    return Dir


def rm(pwd):
    os.system("rm -rf %s" % pwd)


def mkscratch():
    if os.path.exists('/scratch/'):
        return mkdir('/scratch/%s/' % os.environ['USER'])
    elif os.path.exists('/tmp/'):
        return mkdir('/tmp/%s/' % os.environ['USER'])
    else:
        raise Exception('...')


def touch(path):
    with open(path, 'a'):
        os.utime(path, None)


def Ndigits(val, size=6):
    """ returns a N-digit integer with leading zeros """
    _sixDigit = "%i" % val
    return _sixDigit.zfill(size)


def safe_copy(infile, outfile, sleep=10, attempts=10, debug=False):
    if debug:
        print 'cp %s -> %s' % (infile, outfile)
    infile = infile.replace("@", "") if infile.startswith("@") else infile
    # Try not to step on any toes....
    sleep = parse_sleep(sleep)
    if infile.startswith("root:"):
        print 'file is on xrootd - switching to XRD library'
        cmnd = "xrdcp %s %s" % (infile, outfile)
    else:
        if "$" in infile: infile = os.path.expandvars(infile)
        if "$" in outfile: outfile = os.path.expandvars(outfile)
        cmnd = "cp %s %s" % (infile, outfile)
    i = 1
    if debug:
        print "Attempting to copy file..."
    while i < attempts:
        status = sub.call(shlex.split(cmnd))
        if status == 0:
            return status
        else:
            print "%i - Copy failed; sleep %ss" % (i, sleep)
            time.sleep(sleep)
        i += 1
    raise IOError("Failed to copy file")


def parse_sleep(sleep):
    MINUTE = 60
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR
    WEEK = 7 * DAY
    if isinstance(sleep, float) or isinstance(sleep, int):
        return sleep
    elif isinstance(sleep, str):
        try:
            return float(sleep)
        except ValueError:
            pass

        if sleep.endswith('s'):
            return float(sleep.strip('s'))
        elif sleep.endswith('m'):
            return float(sleep.strip('m')) * MINUTE
        elif sleep.endswith('h'):
            return float(sleep.strip('h')) * HOUR
        elif sleep.endswith('d'):
            return float(sleep.strip('d')) * DAY
        elif sleep.endswith('w'):
            return float(sleep.strip('w')) * WEEK
        else:
            raise ValueError
    else:
        raise ValueError


def sleep(sleep):
    return time.sleep(parse_sleep(sleep))


def get_resources():
    import resource
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return '''usertime=%s systime=%s mem=%s mb
           ''' % (usage[0], usage[1],
                  (usage[2] * resource.getpagesize()) / 1000000.0)


def camelize(myStr):
    d = "".join(x for x in str(myStr).title() if not x.isspace())
    return d


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return random.randint(range_start, range_end)


def parseJobXmlToDict(domInstance, parent="Job", setVars=True):
    out = {}
    elems = xdom.parse(StringIO(domInstance)).getElementsByTagName(parent)
    if len(elems) > 1:
        print 'found multiple job instances in xml, will ignore everything but last.'
    if not len(elems):
        raise Exception('found no Job element in xml.')
    el = elems[-1]
    datt = dict(zip(el.attributes.keys(), [v.value for v in el.attributes.values()]))
    if setVars:
        for k, v in datt.iteritems():
            os.environ[k] = v
    nodes = [node for node in el.childNodes if isinstance(node, xdom.Element)]
    for node in nodes:
        name = str(node.localName)
        if name == "JobWrapper":
            out['executable'] = node.getAttribute("executable")
            out['script'] = node.firstChild.data
        else:
            if name in ["InputFiles", "OutputFiles"]:
                my_key = "File"
            else:
                my_key = "Var"
            section = []
            for elem in node.getElementsByTagName(my_key):
                section.append(dict(zip(elem.attributes.keys(), [v.value for v in elem.attributes.values()])))
            out[str(name)] = section
    if setVars:
        for var in out['MetaData']:
            key = var['name']
            value = var['value']
            if "$" in value:
                value = os.path.expandvars(value)
            os.environ[key] = value
            var['value'] = value
            # expand vars
    out['atts'] = datt
    if 'type' in datt:
        os.environ["DWF_TYPE"] = datt["type"]

    for var in out['InputFiles'] + out['OutputFiles']:
        if '$' in var['source']:
            var['source'] = os.path.expandvars(var['source'])
        if '$' in var['target']:
            var['target'] = os.path.expandvars(var['target'])
        # print var['source'],"->",var['target']
    return out