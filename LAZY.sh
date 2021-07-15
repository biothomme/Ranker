#!/bin/bash
#
# Print commands to start vitrual environment and jupy lab
echo "-- Copy the lines below to start working --
- (or they are also copied automatically on mac) -"
echo "conda activate env_ranker"
echo "jupyter lab"
echo "conda activate env_ranker
jupyter lab" | pbcopy
