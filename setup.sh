#!/bin/bash

export DWF_ROOT=$(pwd)
export PYTHONPATH=${DWF_ROOT}:${PYTHONPATH}
export WorkflowConfig="$(pwd)/config/dampe.cfg"
export PYTHONPATH=$(pwd)/core:${PYTHONPATH} # is this one obsolete?
