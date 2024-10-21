#!/bin/bash
#SBATCH -p short                
#SBATCH -t 03:00:00             
#SBATCH -c 4                    
#SBATCH -N 1                    
#SBATCH --mem=5GB  
#SBATCH --mail-type=FAIL
#SBATCH --job-name=download_tcga_data

# Check if the sample sheet path is provided as a parameter
if [ -z "$1" ]; then
  echo "Error: No sample sheet provided."
  exit 1
fi

SAMPLE_SHEET_PATH="$1"

# Load necessary modules or activate environment if needed
# module load python/3.8

# Run the Python script
python /home/rav589/scratch/TCGA/scripts/download_tcga_data.py --sample-sheet "$SAMPLE_SHEET_PATH"
