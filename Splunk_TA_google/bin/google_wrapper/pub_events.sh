#!/bin/bash

curdir=`pwd`
dirn=`dirname ${curdir}`
export PYTHONPATH="${dirn}"
echo $PYTHONPATH

python pubsub_wrapper.py
