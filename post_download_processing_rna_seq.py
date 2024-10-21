import os
import pandas as pd
import argparse
import logging
import sys

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process and combine extracted TCGA data files.')
    parser.add_argument('--sample-sheet', type=str, required=True,
                        help='Path to the sample sheet TSV file.')
    parser.add_argument('--outputs-dir', type=str, default='outputs',
                        help='Path to the outputs directory containing extracted files.')
    parser.add_argument('--output-file', type=str, default='combined_data.tsv',
                        help='Name of the output combined TSV file.')
    parser.add_argument('--expression-column', type=str, default='tpm_unstranded',
                        help='Name of the expression column to extract from the TSV files.')
    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s:%(name)s: %(message)s',
        handlers=[
            logging.FileHandler("post_download_processing.log"),
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

        # Create a mapping from File Name to desired column naming format
        # Incorporate File ID to ensure uniqueness
        sample_sheet['Column Name'] = sample_sheet.apply(
            lambda row: f"{row['Project ID']}_{row['Case ID']}_{row['Sample ID']}_{row['File ID']}", axis=1)
        file_name_to_column_name = dict(zip(sample_sheet['File Name'], sample_sheet['Column Name']))

        # Check for duplicate column names
        if len(file_name_to_column_name.values()) != len(set(file_name_to_column_name.values())):
            duplicates = sample_sheet[sample_sheet.duplicated(['Project ID', 'Case ID', 'Sample ID'], keep=False)]
            logger.warning(f"Duplicate column names detected. Consider reviewing the sample sheet for uniqueness.\n{duplicates}")
            # Optionally, handle duplicates by appending a unique suffix
            # For simplicity, we're assuming File IDs make the names unique

        # Initialize an empty DataFrame to hold the combined data
        combined_df = None

        # Iterate over the extracted files
        outputs_dir = args.outputs_dir
        logger.info(f"Processing extracted files in {outputs_dir}")
        num_files_processed = 0

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
                if file_name not in file_name_to_column_name:
                    logger.warning(f"File Name '{file_name}' not found in sample sheet. Skipping.")
                    continue

                # Get the new column name
                column_name = file_name_to_column_name[file_name]

                # Read the data
                try:
                    df = pd.read_csv(file_path, sep='\t', comment='#')
                except Exception as e:
                    logger.error(f"Failed to read file {file_path}: {e}")
                    continue

                # Exclude rows starting with 'N_'
                if 'gene_name' not in df.columns:
                    logger.error(f"'gene_name' column not found in file {file_path}. Skipping.")
                    continue

                df = df[~df['gene_name'].str.startswith('N_')]

                # Set 'gene_name' as the index
                df.set_index('gene_name', inplace=True)

                # Check if the expression column exists
                expression_column = args.expression_column
                if expression_column not in df.columns:
                    logger.warning(f"Expression column '{expression_column}' not found in file {file_path}. Skipping.")
                    continue

                # Select the expression column
                df = df[[expression_column]]

                # Rename the column to the sample's column name
                df.columns = [column_name]

                # Merge the data
                if combined_df is None:
                    combined_df = df
                else:
                    try:
                        combined_df = combined_df.join(df, how='outer')
                    except ValueError as ve:
                        logger.error(f"Error merging file {file_path}: {ve}")
                        logger.debug("Attempting to resolve column name conflict by appending suffix.")
                        # Attempt to append a suffix to resolve the conflict
                        df.columns = [f"{column_name}_1"]
                        combined_df = combined_df.join(df, how='outer')

                num_files_processed += 1

        if combined_df is not None:
            # Write the combined data to a TSV file
            output_file_path = os.path.join(outputs_dir, args.output_file)
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
