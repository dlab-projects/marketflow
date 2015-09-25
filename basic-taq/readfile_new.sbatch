#!/bin/bash
#SBATCH --job-name=readfile_new
#SBATCH --partition=savio
#SBATCH --account=co_dlab
#SBATCH --qos=dlab_normal
#SBATCH --ntasks=1
#SBATCH --time=01:00:00
ht_helper.sh -t readfile_new.taskfile -n1 -dvk
