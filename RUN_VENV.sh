#!/bin/bash
## Run (and initialize) the virtual environment for the project.
## It uses conda. Alternatively you may outcomment the python/pip commands.

ENV=env_ranker
DIR=`pwd`
REQ="$DIR/requirements.txt"

## Set up virtual environment (dependency: conda)
# if [ -f "$REQ" ];
# then
#     conda create --name $ENV --file $REQ
##     python -m venv $ENV
##     python -m pip install -r requirements.txt
# else
#     conda create -n $ENV python=3.9.5
##     python -m venv $ENV
# fi

## Activate v. env.
source ~/anaconda3/etc/profile.d/conda.sh
conda activate $ENV
echo "Copy the following line to start virtual environment:"
echo "conda activate $ENV"
# echo "source $ENV/bin/activate"

## Store dependencies
if [ -f "$REQ" ];
then
    echo "$REQ exists."
else
    conda list --explicit > "$REQ"
#    pip freeze > requirements.txt
fi

## Deactivate the environment to avoid background processes.
conda deactivate
# deactivate
