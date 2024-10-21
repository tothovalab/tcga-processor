#!/usr/bin/env python

import argparse
import pandas as pd
import requests
import json
import re
import logging
import sys
import os
import tarfile
import math

def validate_file_ids(file_ids):
    logger = logging.getLogger(__name__)

    files_endpt = "https://api.gdc.cancer.gov/files"

    # Prepare parameters to check file IDs
    check_params = {
        "filters": {
            "op": "in",
            "content": {
                "field": "file_id",
                "value": file_ids
            }
        },
        "fields": "file_id",
        "format": "JSON",
        "size": len(file_ids)
    }

    try:
        # Make the POST request
        logger.info("Validating file IDs...")
        response = requests.post(
            files_endpt,
            headers={"Content-Type": "application/json"},
            data=json.dumps(check_params)
        )

        if response.status_code == 200:
            data = response.json()
            valid_file_ids = [f['file_id'] for f in data['data']['hits']]
            invalid_file_ids = set(file_ids) - set(valid_file_ids)
            if invalid_file_ids:
                logger.warning(f"Invalid File IDs found: {invalid_file_ids}")
            else:
                logger.info("All File IDs are valid.")
            return valid_file_ids
        else:
            logger.error(f"Error checking file IDs (status code {response.status_code})")
            logger.debug(f"Response: {response.text}")
            sys.exit(1)
    except Exception as e:
        logger.exception(f"An error occurred during file ID validation: {e}")
        sys.exit(1)

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Download TCGA data from a custom cohort sample sheet.')
    parser.add_argument('--sample-sheet', type=str, required=True,
                        help='Path to the sample sheet TSV file.')
    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s:%(name)s: %(message)s',
        handlers=[
            logging.FileHandler("download_tcga_data.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)

    try:
        # Create outputs directory
        outputs_dir = os.path.join(os.getcwd(), 'outputs')
        if not os.path.exists(outputs_dir):
            os.makedirs(outputs_dir)
            logger.info(f"Created outputs directory at {outputs_dir}")
        else:
            logger.info(f"Outputs directory already exists at {outputs_dir}")

        # Read the sample sheet
        sample_sheet_path = args.sample_sheet
        logger.info(f"Reading sample sheet from {sample_sheet_path}")
        sample_sheet = pd.read_csv(sample_sheet_path, sep="\t")

        # Check if 'File ID' column exists
        if 'File ID' not in sample_sheet.columns:
            logger.error("Column 'File ID' not found in the sample sheet.")
            sys.exit(1)

        # Get the list of File IDs
        file_ids = sample_sheet['File ID'].dropna().unique().tolist()
        logger.info(f"Number of file IDs in sample sheet: {len(file_ids)}")

        if not file_ids:
            logger.error("No File IDs found in the sample sheet.")
            sys.exit(1)

        # Validate File IDs
        valid_file_ids = validate_file_ids(file_ids)
        logger.info(f"Number of valid file IDs: {len(valid_file_ids)}")

        if not valid_file_ids:
            logger.error("No valid File IDs found after validation.")
            sys.exit(1)

        # GDC data endpoint
        data_endpt = "https://api.gdc.cancer.gov/data"

        # Maximum number of IDs per request
        max_ids_per_request = 500

        # Calculate the number of batches
        num_batches = math.ceil(len(valid_file_ids) / max_ids_per_request)

        for batch_num in range(num_batches):
            start_idx = batch_num * max_ids_per_request
            end_idx = start_idx + max_ids_per_request
            batch_file_ids = valid_file_ids[start_idx:end_idx]
            logger.info(f"Downloading batch {batch_num + 1}/{num_batches} with {len(batch_file_ids)} file IDs.")

            # Parameters
            params = {"ids": batch_file_ids}

            # Make the POST request
            response = requests.post(
                data_endpt,
                data=json.dumps(params),
                headers={"Content-Type": "application/json"}
            )

            # Check if the request was successful
            if response.status_code == 200:
                # Get the file name from the response headers
                response_head_cd = response.headers.get("Content-Disposition", "")
                file_name_match = re.findall('filename="*(.+?)"*$', response_head_cd)
                if file_name_match:
                    file_name = file_name_match[0]
                else:
                    file_name = f"gdc_download_batch_{batch_num + 1}.tar.gz"
                logger.debug(f"Received file name: {file_name}")

                # Save the content to a file in the outputs directory
                file_path = os.path.join(outputs_dir, file_name)
                with open(file_path, "wb") as output_file:
                    output_file.write(response.content)
                logger.info(f"Downloaded data saved to {file_path}")

                # Extract the tar.gz file into outputs directory
                if file_name.endswith(".tar.gz"):
                    extract_path = outputs_dir  # Extract into outputs directory
                    logger.debug(f"Extracting files to {extract_path}")
                    with tarfile.open(file_path, "r:gz") as tar:
                        tar.extractall(path=extract_path)
                    logger.info(f"Files extracted to {extract_path}")

            else:
                logger.error(f"Error: Unable to download files in batch {batch_num + 1} (status code {response.status_code})")
                logger.debug(f"Response headers: {response.headers}")
                logger.debug(f"Response content: {response.text}")

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()