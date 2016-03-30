'''
Created on Mar 15, 2016

@author: zimmer
'''
import xml.dom.minidom as xdom
from StringIO import StringIO
from core import db
from core.models import *

def update_status(JobId,InstanceId,major_status,**kwargs):
    db.connect()
    my_job = Job.objects.filter(id=JobId)
    if not len(my_job):
        print 'could not find jobId %s'%JobId
        return
    my_job = my_job[0]
    assert major_status in MAJOR_STATII
    jInstance = my_job.getInstance(InstanceId)
    my_dict = {"last_update":time.ctime()}
    my_dict.update(kwargs)
    for key,value in my_dict.iteritems():
        jInstance.__setattr__(key,value)
    # finally, update status
    jInstance.setStatus(major_status) 
    #print 'calling my_job.save'
    my_job.save()
    return

def parseJobXmlToDict(domInstance,parent="JobInstance"):
    out = {}
    elems = xdom.parse(StringIO(domInstance)).getElementsByTagName(parent)
    if len(elems)>1:
        print 'found multiple job instances in xml, will ignore everything but last.'
    el = elems[-1]
    nodes = [node for node in el.childNodes if isinstance(node,xdom.Element)]
    for node in nodes:
        name = str(node.localName)
        if name == "JobWrapper":
            out['executable']=node.getAttribute("executable")
            out['script']=node.firstChild.data
        else:
            if name in ["InputFiles","OutputFiles"]:
                my_key = "File"
            else:
                my_key = "Var"
            section = []
            for elem in node.getElementsByTagName(my_key):
                section.append(dict(zip(elem.attributes.keys(),[v.value for v in elem.attributes.values()])))
            out[str(name)]=section
    return out