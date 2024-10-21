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
  echo "Usage: sbatch script_name.sh /path/to/sample_sheet [output_directory]"
  exit 1
fi

SAMPLE_SHEET_PATH="$1"

# Check if output directory is provided as a second parameter
if [ -n "$2" ]; then
  OUTPUT_DIRECTORY="$2"
  # Run the Python script with the output directory
  python /home/rav589/tothovalab/tcga-processor/download_tcga_data.py --sample-sheet "$SAMPLE_SHEET_PATH" --output-directory "$OUTPUT_DIRECTORY"
else
  # Run the Python script without specifying output directory (will use default)
  python /home/rav589/tothovalab/tcga-processor/download_tcga_data.py --sample-sheet "$SAMPLE_SHEET_PATH"
fi
