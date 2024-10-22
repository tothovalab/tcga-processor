#!/usr/bin/env python

"""
Script Name: process_tcga_data_variantcalls.py

Description:
    This script processes and combines extracted TCGA MAF (Mutation Annotation Format) files
    based on a sample sheet obtained from the GDC portal. It concatenates the MAF files,
    retains specified columns, calculates the Variant Allele Frequency (VAF), and saves
    the combined data to a TSV file.

Author:
    Rishika Vadlamudi

Date:
    2024-10-21
"""

import os
import sys
import argparse
import logging
import pandas as pd

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process and combine extracted TCGA MAF files.')
    parser.add_argument('--sample-sheet', type=str, required=True,
                        help='Path to the sample sheet TSV file downloaded from GDC portal.')
    parser.add_argument('--file-directory', type=str, default='outputs',
                        help='Path to the directory containing extracted MAF files. Default is ./outputs.')
    parser.add_argument('--output-directory', type=str, default=os.getcwd(),
                        help='Directory where the combined MAF file will be saved. Default is the current working directory.')
    parser.add_argument('--output-file', type=str, default='combined_maf.tsv',
                        help='Name of the output combined MAF TSV file. Default is combined_maf.tsv.')
    parser.add_argument('--retain-columns', nargs='*', default=None,
                        help='List of columns to retain from the MAF files. Default is a predefined set of columns.')
    parser.add_argument('--calculate-vaf', action='store_true',
                        help='Calculate Variant Allele Frequency (VAF) and add it as a new column.')
    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s:%(name)s: %(message)s',
        handlers=[
            logging.FileHandler("process_tcga_maf.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)

    try:
        # Read the sample sheet
        sample_sheet_path = args.sample_sheet
        logger.info(f"Reading sample sheet from {sample_sheet_path}")
        sample_sheet = pd.read_csv(sample_sheet_path, sep="\t")

        # Ensure required columns are present
        required_columns = ['File ID', 'File Name', 'Project ID', 'Case ID', 'Sample ID']
        missing_columns = set(required_columns) - set(sample_sheet.columns)
        if missing_columns:
            logger.error(f"The following required columns are missing from the sample sheet: {missing_columns}")
            sys.exit(1)

        # Create a mapping from File Name to File ID
        file_name_to_file_id = dict(zip(sample_sheet['File Name'], sample_sheet['File ID']))

        # Check for duplicate File Names
        if sample_sheet['File Name'].duplicated().any():
            duplicates = sample_sheet[sample_sheet['File Name'].duplicated(keep=False)]
            logger.warning(f"Duplicate File Names detected. Ensure each File Name is unique.\n{duplicates}")

        # Initialize a list to hold individual DataFrames
        maf_dfs = []
        num_files_processed = 0

        # Define the default columns to retain if not specified
        default_desired_columns = [
            'Hugo_Symbol',
            'Chromosome',
            'Start_Position',
            'End_Position',
            'Strand',
            'Variant_Classification',
            'Variant_Type',
            'Reference_Allele',
            'Tumor_Seq_Allele1',
            'Tumor_Seq_Allele2',
            'Tumor_Sample_Barcode',
            'Matched_Norm_Sample_Barcode',
            't_depth',
            't_ref_count',
            't_alt_count',
            'n_depth',
            'n_ref_count',
            'n_alt_count',
            'Consequence',
            'IMPACT',
            'callers'
        ]

        # Use specified columns or default
        desired_columns = args.retain_columns if args.retain_columns else default_desired_columns

        # Iterate over the extracted MAF files
        file_directory = args.file_directory
        logger.info(f"Processing extracted MAF files in {file_directory}")

        for root, dirs, files in os.walk(file_directory):
            for file in files:
                # Identify MAF files (assuming they end with .maf or .maf.gz)
                if not (file.endswith('.maf') or file.endswith('.maf.gz')):
                    continue

                file_path = os.path.join(root, file)
                logger.info(f"Processing file: {file_path}")

                # Check if the File Name is in the sample sheet
                if file not in file_name_to_file_id:
                    logger.warning(f"File Name '{file}' not found in sample sheet. Skipping.")
                    continue

                # Get the corresponding File ID
                file_id = file_name_to_file_id[file]

                # Read the MAF file
                try:
                    maf = pd.read_csv(
                        file_path,
                        sep='\t',
                        compression='gzip' if file.endswith('.gz') else None,
                        comment='#',
                        low_memory=False
                    )
                except Exception as e:
                    logger.error(f"Failed to read file {file_path}: {e}")
                    continue

                # Check if desired columns are present
                missing_cols = set(desired_columns) - set(maf.columns)
                if missing_cols:
                    logger.warning(f"Missing columns {missing_cols} in file {file_path}. Skipping these columns.")
                    # Adjust the desired columns to those present
                    present_columns = [col for col in desired_columns if col in maf.columns]
                else:
                    present_columns = desired_columns

                # Select the desired columns
                maf_selected = maf[present_columns].copy()

                # Add the 'File_ID' column
                maf_selected['File_ID'] = file_id

                # Calculate VAF and add as a new column if requested
                if args.calculate_vaf:
                    # Ensure required columns are present for VAF calculation
                    if 't_alt_count' in maf_selected.columns and 't_depth' in maf_selected.columns:
                        maf_selected['VAF'] = maf_selected.apply(
                            lambda row: row['t_alt_count'] / row['t_depth'] if row['t_depth'] > 0 else float('nan'),
                            axis=1
                        )
                        # Round VAF to 4 decimal places for readability
                        maf_selected['VAF'] = maf_selected['VAF'].round(4)
                    else:
                        logger.warning(f"Columns 't_alt_count' and 't_depth' are required for VAF calculation but are missing in file {file_path}.")

                # Append to the list
                maf_dfs.append(maf_selected)
                num_files_processed += 1
                logger.info(f"Successfully processed file: {file_path}")

        if maf_dfs:
            # Concatenate all DataFrames vertically
            combined_maf = pd.concat(maf_dfs, ignore_index=True)
            logger.info(f"Combined MAF DataFrame shape: {combined_maf.shape}")

            # Define the output file path
            output_file_path = os.path.join(args.output_directory, args.output_file)

            # Save the combined DataFrame to a TSV file
            combined_maf.to_csv(output_file_path, sep='\t', index=False)
            logger.info(f"Combined MAF data saved to {output_file_path}")
            logger.info(f"Number of MAF files processed and combined: {num_files_processed}")
        else:
            logger.warning("No MAF files were processed. Please check if the extracted files are present and properly formatted.")

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
