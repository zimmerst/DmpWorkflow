# DmpWorkflow
Workflow framework for DAMPE remote computing &amp; accounting

external contributor: Stephane Poss @sposs on github (lots of help in packaging & dealing with requests as well as for the initial suggestion to using flask/mongodb)

[![Code Health](https://landscape.io/github/zimmerst/DmpWorkflow/master/landscape.svg?style=flat)](https://landscape.io/github/zimmerst/DmpWorkflow/master)

Prerequisites: 
--------------
it's advisable to install virtualenv to handle the different python installations

```python
easy_install virtualenv OR
pip install virtualenv
```
NEW Installation Instructions:
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

Installation Instructions:
--------------------------
1. 	clone the repository via git: 
	```bash
	git clone https://github.com/zimmerst/DmpWorkflow.git
	```

2.	set DmpWorkflow as virtualenv:
	```bash
	virtualenv DmpWorkflow
	```

3.	activate virtualenv:
	```bash
	source DmpWorkflow/bin/activate
	```
		
4.	install missing flask packages with pip (only needs to be done once!)
	```python
	pip install flask
	pip install flask-mongoengine
	pip install flask-script
	pip install mongoengine
	pip install -U jsonpickle
	```

5.	modify config to have right server & DB address. See config/default.cfg for an example.
	(note that the framework may run on two different servers!) 
	
6.	source DmpWorkflow/setup.sh to initialize relevant environment variables

General Comments:
-----------------
to facilitate easier queries, jobs are being submitted with name JobID.JobInstanceID (so can query for that)
