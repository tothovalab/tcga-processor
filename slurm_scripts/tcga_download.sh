#!/bin/bash
#SBATCH -p short                
#SBATCH -t 03:00:00             
#SBATCH -c 4                    
#SBATCH -N 1                    
#SBATCH --mem=5GB  
#SBATCH --mail-type=FAIL
#SBATCH --job-name=download_tcga_data

if [ -z "$1" ]; then
  echo "Error: No sample sheet provided."
  exit 1
fi

SAMPLE_SHEET_PATH="$1"

if [ -n "$2" ]; then
  OUTPUT_DIRECTORY="$2"
  python download_tcga_data.py --sample-sheet "$SAMPLE_SHEET_PATH" --output-directory "$OUTPUT_DIRECTORY"
else
  python download_tcga_data.py --sample-sheet "$SAMPLE_SHEET_PATH"
fi
