#!/bin/bash
#SBATCH --job-name=post_download_processing
#SBATCH -p short                
#SBATCH -t 03:00:00             
#SBATCH -c 4                    
#SBATCH -N 1                    
#SBATCH --mem=5GB  
#SBATCH --mail-type=FAIL

if [ -z "$1" ]; then
  echo "Error: No sample sheet provided."
  exit 1
fi

SAMPLE_SHEET_PATH="$1"

# module load python/3.8

python process_tcga_data_transcriptome.py --sample-sheet "$SAMPLE_SHEET_PATH"
