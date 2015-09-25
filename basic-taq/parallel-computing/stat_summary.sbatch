#!/bin/bash
#SBATCH --job-name=stat_summary
#SBATCH --partition=savio
#SBATCH --account=co_dlab
#SBATCH --qos=dlab_normal
#SBATCH --ntasks=20
#SBATCH --time=01:00:00
ht_helper.sh -t stat_summary.taskfile -n1 -dvk