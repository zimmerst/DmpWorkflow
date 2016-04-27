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