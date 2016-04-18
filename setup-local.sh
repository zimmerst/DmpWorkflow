#!/bin/bash

export WorkflowConfig="$(pwd)/config/dampe-localtest.cfg"
export PYTHONPATH=$(pwd)/core:${PYTHONPATH}