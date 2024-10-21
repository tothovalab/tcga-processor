#!/usr/bin/env python

"""
Script Name: process_tcga_data_rnaseq.py

Description:
    This script processes and combines extracted TCGA data files based on a sample sheet
    obtained from the GDC portal. It merges the data on the 'gene_name' column and includes
    all expression columns by default. Column names are renamed to include sample identifiers
    for easy traceability.

Author:
    Rishika Vadlamudi

Date:
    2024-10-21
"""

import os
import pandas as pd
import argparse
import logging
import sys

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process and combine extracted TCGA data files.')
    parser.add_argument('--sample-sheet', type=str, required=True,
                        help='Path to the sample sheet TSV file downloaded from GDC portal.')
    parser.add_argument('--outputs-dir', type=str, default='outputs',
                        help='Path to the outputs directory containing extracted files.')
    parser.add_argument('--output-directory', type=str, default=os.getcwd(),
                        help='Path to the directory where the combined data will be saved. Default is the current working directory.')
    parser.add_argument('--output-file', type=str, default='combined_data.tsv',
                        help='Name of the output combined TSV file.')
    parser.add_argument('--expression-columns', nargs='*', default=None,
                        help='List of expression columns to extract from the TSV files. Default is all columns except gene_name.')
    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s:%(name)s: %(message)s',
        handlers=[
            logging.FileHandler("process_tcga_data.log"),
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

        # Create a sample identifier for each file to ensure uniqueness
        sample_sheet['Sample Identifier'] = sample_sheet.apply(
            lambda row: f"{row['Project ID']}_{row['Case ID']}_{row['Sample ID']}_{row['File ID']}", axis=1)
        file_name_to_sample_id = dict(zip(sample_sheet['File Name'], sample_sheet['Sample Identifier']))

        # Initialize a list to hold individual DataFrames
        data_frames = []
        num_files_processed = 0

        # Iterate over the extracted files
        outputs_dir = args.outputs_dir
        logger.info(f"Processing extracted files in {outputs_dir}")

        for root, dirs, files in os.walk(outputs_dir):
            for file in files:
                file_path = os.path.join(root, file)
                # Skip non-TSV files
                if not file.endswith('.tsv'):
                    continue

                # Get the File Name
                file_name = file
                logger.info(f"Processing file: {file_path}")

                # Check if the File Name is in the sample sheet
                if file_name not in file_name_to_sample_id:
                    logger.warning(f"File Name '{file_name}' not found in sample sheet. Skipping.")
                    continue

                # Get the sample identifier
                sample_id = file_name_to_sample_id[file_name]

                # Read the data
                try:
                    df = pd.read_csv(file_path, sep='\t', comment='#')
                except Exception as e:
                    logger.error(f"Failed to read file {file_path}: {e}")
                    continue

                # Exclude rows where 'gene_name' starts with 'N_'
                if 'gene_name' not in df.columns:
                    logger.error(f"'gene_name' column not found in file {file_path}. Skipping.")
                    continue

                # Ensure 'gene_name' is of string type
                df['gene_name'] = df['gene_name'].astype(str)
                df = df[~df['gene_name'].str.startswith('N_')]

                # Set 'gene_name' as the index
                df.set_index('gene_name', inplace=True)

                # Select expression columns
                if args.expression_columns:
                    missing_cols = set(args.expression_columns) - set(df.columns)
                    if missing_cols:
                        logger.warning(f"The following expression columns are missing in file {file_path}: {missing_cols}")
                    selected_columns = [col for col in args.expression_columns if col in df.columns]
                else:
                    # Exclude 'gene_name' and any columns that start with '__'
                    selected_columns = [col for col in df.columns if not col.startswith('__')]

                if not selected_columns:
                    logger.warning(f"No valid expression columns found in file {file_path}. Skipping.")
                    continue

                # Select the desired columns
                df_selected = df[selected_columns]

                # Rename the columns to include the sample identifier
                df_selected.columns = [f"{col}_{sample_id}" for col in df_selected.columns]

                # Append the DataFrame to the list
                data_frames.append(df_selected)
                num_files_processed += 1

        if data_frames:
            # Concatenate all DataFrames on the 'gene_name' index
            combined_df = pd.concat(data_frames, axis=1)

            # Write the combined data to a TSV file
            output_file_path = os.path.join(args.output_directory, args.output_file)
            combined_df.to_csv(output_file_path, sep='\t')
            logger.info(f"Combined data saved to {output_file_path}")
            logger.info(f"Number of files processed and combined: {num_files_processed}")
        else:
            logger.warning("No data was combined. Please check if the extracted files are present and properly formatted.")

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()