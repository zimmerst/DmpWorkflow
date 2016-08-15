# DmpWorkflow
Workflow framework for DAMPE remote computing &amp; accounting

external contributor: Stephane Poss @sposs on github (lots of help in packaging & dealing with requests as well as for the initial suggestion to using flask/mongodb)

[![Code Health](https://landscape.io/github/zimmerst/DmpWorkflow/master/landscape.svg?style=plastic)](https://landscape.io/github/zimmerst/DmpWorkflow/master)

Prerequisites: 
--------------
it's advisable to install virtualenv to handle the different python installations

```python
easy_install virtualenv OR
pip install virtualenv
```
Installation Instructions:
------------------------------
again, it's advisable to use a virtual environment.

1. 	set DAMPE as virtualenv:
	```bash
	mkvirtualenv DAMPE
	```

2.	get tarball:
	```bash
	wget --no-check-certificate https://dampevm3.unige.ch/dmpworkflow/releases/DmpWorkflow-0.0.1.dev247.tar.gz
	```

3.	install tarball:
	``` bash
	pip install DmpWorkflow-0.0.1.dev247.tar.gz
	```

4.	set configuration file:
	``` bash
	cdsitepackages
	nano DmpWorkflow/config/defaults.cfg
	```

5.	enjoy!

XML Job Definition:
-------------------
Job definition is done in Xml markup

```xml
<Jobs>
	<Job>
		<InputFiles>
			<File source="" target="" file_type="" />
		</InputFiles>
		<OutputFiles>
			<File source="" target="" file_type="" />
		</OutputFiles>
			<JobWrapper executable="/bin/bash"><![CDATA[
#/bin/bash
echo hostname
]]>		
			</JobWrapper>
		<MetaData>
			<Var name="" value="" var_type="string"/>
		</MetaData>
	</Job>
</Jobs>
```

Note that there are a few reserved metadata variables:
  * BATCH\_OVERRIDE\_REQUIREMENTS - will override whatever BATCH_REQUIREMENTS are defined in settings.cfg
  * BATCH\_OVERRIDE\_EXTRAS - complements requirements
  * BATCH\_OVERRIDE\_QUEUE - the queue to be used, overrides BATCH_QUEUE
  * BATCH\_OVERRIDE\_SYSTEM	- shouldn't be used.
These variables can be used to control the submission behavior for each batch job.

Hierarchy of variable resolution:
---------------------------------
first comes JobInstance, overrides anything that is defined at Job level. if neither instance nor job provide variables, top level variables are inherited from depedent parent task (if defined)

Adding a new SITE to system:
----------------------------
for details, send email to zimmer_at_cern.ch. To extend the existing system, a remote access to SITE_A (our remote site) is necessary. Test the ability to connect to the DB by submitting heart-beat requests. Provided the site can serve requests through HTTP to either PROD or DEVEL instances, create a new, empty config.file which will become your site-configuration for SITE_A. 
```bash
[global]
installation = client
randomSeed = true
trackeback = true
# or set seed here.

[server]
# use DEVEL server for now
url = http://url_to_flask_server

# here we have model definitions specific to the collections
[JobDB]
task_types = Generation,Digitization,Reconstruction,User,Other,Data,SimuDigi,Reco
task_major_statii = New,Running,Failed,Terminated,Done,Submitted,Suspended
task_final_statii = Terminated,Failed,Done
batch_sites = CNAF,local,UNIGE,BARI

[site]
name = SITE_A
DAMPE_SW_DIR = /lustrehome/exp_soft/dampe_local/dampe
EXEC_DIR_ROOT = /tmp/condor/
ExternalsScript = ${DAMPE_SW_DIR}/externals/setup.sh
workdir = /lustre/dampe/workflow/workdir
#workdir = /storage/gpfs_ams/dampe/users/dampe_prod/test
HPCsystem = condor # or lsf, sge / pbs
HPCmemory = 4000
HPCcputime = 01:00
# use HPCextra to specify the universe for condor
HPCname  = HPC_Site_A
HPCextra = site_condor_name

[watchdog]
ratio_mem = 0.95
ratio_cpu = 0.98
```

save your changes and proceed to download the client. Once done, load releavnt configuration using dampe-cli-configure -f <file/to/config>. 
```bash
dampe-cli-configure -f site_A.cfg
```

The only other part is to start the fetcher as daemon, e.g. through an infinite loop (perhaps inside a screen session):

```bash
while true; do dampe-cli-fetch-new-jobs -c 20 -m 800; sleep 20; done
```

Also, make sure to add SITE_A to the configuration file for your servers (vm4/vm6) 
