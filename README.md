# DmpWorkflow
Workflow framework for DAMPE remote computing &amp; accounting


Prerequisites: 
--------------
it's advisable to install virtualenv to handle the different python installations

```python
easy_install virtualenv OR
pip install virtualenv
```

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
	```

5.	modify config to have right server & DB address. See config/default.cfg for an example.
	(note that the framework may run on two different servers!) 
	
6.	source DmpWorkflow/setup.sh to initialize relevant environment variables

General Comments:
-----------------
to facilitate easier queries, jobs are being submitted with name JobID.JobInstanceID (so can query for that)