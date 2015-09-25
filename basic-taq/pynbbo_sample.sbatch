#!/bin/bash
#SBATCH --job-name=pynbbo_ht
#SBATCH --partition=savio
#SBATCH --account=ac_dlab
#SBATCH --qos=condo_dlab
#SBATCH --ntasks=10
#SBATCH --time=02:00:00
ht_helper.sh -t pynbbo_sample.taskfile -dvk
