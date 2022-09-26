#!/bin/bash
#note anything after #SBATCH is a command
#SBATCH --mail-type=ALL
#SBATCH --job-name=lob_cancel
#SBATCH --partition=standard-gpu
#SBATCH --nodelist=cholgpu01.cpu.stats.ox.ac.uk   # Node
#SBATCH --cpus-per-task=60
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --time=12:00:00
#SBATCH --mem-per-cpu=3G #Memory per cpu in megabytes
#SBATCH --output=/data/localhost/not-backed-up/pakraste/jobname_%j.txt


cd /data/cholgpu01/not-backed-up/scratch/pakraste/PriceImpactCancel    # Set directory
python Experiment4_main.py 
