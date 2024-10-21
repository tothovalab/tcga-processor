import os
import pandas as pd
import argparse
import logging
import sys

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process and combine extracted TCGA MAF files.')
    parser.add_argument('--sample-sheet', type=str, required=True,
                        help='Path to the sample sheet TSV file.')
    parser.add_argument('--outputs-dir', type=str, default='outputs',
                        help='Path to the outputs directory containing extracted MAF files.')
    parser.add_argument('--output-file', type=str, default='combined_maf.tsv',
                        help='Name of the output combined MAF TSV file.')
    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s:%(name)s: %(message)s',
        handlers=[
            logging.FileHandler("post_download_maf_processing.log"),
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
        for col in required_columns:
            if col not in sample_sheet.columns:
                logger.error(f"Column '{col}' not found in the sample sheet.")
                sys.exit(1)

        # Create a mapping from File Name to File ID
        file_name_to_file_id = dict(zip(sample_sheet['File Name'], sample_sheet['File ID']))

        # Check for duplicate File Names
        if len(file_name_to_file_id.values()) != len(set(file_name_to_file_id.values())):
            duplicates = sample_sheet[sample_sheet.duplicated(['File Name'], keep=False)]
            logger.warning(f"Duplicate File Names detected. Ensure each File Name is unique.\n{duplicates}")
            # Proceeding under the assumption that File IDs are unique

        # Initialize a list to hold individual DataFrames
        maf_dfs = []

        # Define the columns to retain
        desired_columns = [
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

        # Iterate over the extracted MAF files
        outputs_dir = args.outputs_dir
        logger.info(f"Processing extracted MAF files in {outputs_dir}")
        num_files_processed = 0

        for root, dirs, files in os.walk(outputs_dir):
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
                missing_columns = [col for col in desired_columns if col not in maf.columns]
                if missing_columns:
                    logger.warning(f"Missing columns {missing_columns} in file {file_path}. Skipping.")
                    continue

                # Select the desired columns
                maf_selected = maf[desired_columns].copy()

                # Add the 'File ID' column
                maf_selected['File_ID'] = file_id

                # Calculate VAF and add as a new column
                # VAF = t_alt_count / t_depth
                # Handle cases where t_depth is zero to avoid division by zero
                maf_selected['VAF'] = maf_selected.apply(
                    lambda row: row['t_alt_count'] / row['t_depth'] if row['t_depth'] > 0 else float('nan'),
                    axis=1
                )

                # Optionally, round VAF to 4 decimal places for readability
                maf_selected['VAF'] = maf_selected['VAF'].round(4)

                # Append to the list
                maf_dfs.append(maf_selected)
                num_files_processed += 1
                logger.info(f"Successfully processed file: {file_path}")

        if maf_dfs:
            # Concatenate all DataFrames vertically
            combined_maf = pd.concat(maf_dfs, ignore_index=True)
            logger.info(f"Combined MAF DataFrame shape: {combined_maf.shape}")

            # Define the output file path
            output_file_path = os.path.join(outputs_dir, args.output_file)

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

