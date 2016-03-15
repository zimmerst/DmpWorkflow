#!/bin/bash

export WorkflowConfig="$(pwd)/config/dampe.cfg"
export PYTHONPATH=$(pwd)/core:${PYTHONPATH}