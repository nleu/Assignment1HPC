#!/bin/bash
#SBATCH --ntasks-per-node=4
#SBATCH --nodes=4
#SBATCH --time=0-12:00:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.5.2-goolf-2015a

# Launch multiple process python code
time mpiexec -n 8 python3 5.py -i bigTwitter.json -l melbGrid.json
