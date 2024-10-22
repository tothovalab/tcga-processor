#!/usr/bin/env python

"""
Script Name: download_tcga_data.py

Description:
    This script downloads TCGA data files based on a sample sheet obtained from the GDC portal.
    It validates the file IDs, downloads the data in batches, and extracts the files to a specified output directory.

Author:
    Rishika Vadlamudi

Date:
    2024-10-22
"""

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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time  # Imported time module for delays

def validate_file_ids(file_ids):
    """
    Validates the list of file IDs by checking them against the GDC API.

    Args:
        file_ids (list): List of file IDs to validate.

    Returns:
        list: List of valid file IDs.
    """
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
            data=json.dumps(check_params),
            timeout=60  # Set a timeout for the request
        )

        if response.status_code == 200:
            data = response.json()
            valid_file_ids = [f['file_id'] for f in data['data']['hits']]
            invalid_file_ids = set(file_ids) - set(valid_file_ids)
            if invalid_file_ids:
                logger.warning(f"Invalid File IDs found and will be skipped: {invalid_file_ids}")
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

def create_session_with_retries():
    """
    Creates a requests Session with retry logic.

    Returns:
        requests.Session: Session object with retries configured.
    """
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Download TCGA data from a custom cohort sample sheet.')
    parser.add_argument('--sample-sheet', type=str, required=True,
                        help='Path to the sample sheet TSV file downloaded from the GDC portal.')
    parser.add_argument('--output-directory', type=str, default=os.path.join(os.getcwd(), 'outputs'),
                        help='Path to the output directory where data will be saved. Default is ./outputs')
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
        outputs_dir = args.output_directory
        if not os.path.exists(outputs_dir):
            os.makedirs(outputs_dir)
            logger.info(f"Created outputs directory at {outputs_dir}")
        else:
            logger.info(f"Using existing outputs directory at {outputs_dir}")

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
        max_ids_per_request = 100  # Reduced batch size to prevent server overload

        # Calculate the number of batches
        num_batches = math.ceil(len(valid_file_ids) / max_ids_per_request)

        # Create a session with retries
        session = create_session_with_retries()

        for batch_num in range(num_batches):
            start_idx = batch_num * max_ids_per_request
            end_idx = min(start_idx + max_ids_per_request, len(valid_file_ids))
            batch_file_ids = valid_file_ids[start_idx:end_idx]
            logger.info(f"Downloading batch {batch_num + 1}/{num_batches} with {len(batch_file_ids)} file IDs.")

            # Parameters
            params = {"ids": batch_file_ids}

            try:
                # Make the POST request with a timeout
                response = session.post(
                    data_endpt,
                    data=json.dumps(params),
                    headers={"Content-Type": "application/json"},
                    stream=True,  # Stream the content to handle large files
                    timeout=300  # Increased timeout to 5 minutes
                )

                # Check if the request was successful
                if response.status_code == 200:
                    # Get the file name from the response headers
                    response_head_cd = response.headers.get("Content-Disposition", "")
                    file_name_match = re.findall(r'filename="*(.+?)"*$', response_head_cd)
                    if file_name_match:
                        file_name = file_name_match[0]
                    else:
                        file_name = f"gdc_download_batch_{batch_num + 1}.tar.gz"
                    logger.debug(f"Received file name: {file_name}")

                    # Save the content to a file in the outputs directory
                    file_path = os.path.join(outputs_dir, file_name)
                    with open(file_path, "wb") as output_file:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                            if chunk:
                                output_file.write(chunk)
                    logger.info(f"Downloaded data saved to {file_path}")

                    # Extract the tar.gz file into outputs directory
                    if file_name.endswith(".tar.gz"):
                        extract_path = outputs_dir  # Extract into outputs directory
                        logger.debug(f"Extracting files to {extract_path}")
                        with tarfile.open(file_path, "r:gz") as tar:
                            tar.extractall(path=extract_path)
                        logger.info(f"Files extracted to {extract_path}")

                        # Optionally, remove the tar.gz file after extraction
                        os.remove(file_path)
                        logger.debug(f"Removed archive file {file_path}")

                else:
                    logger.error(f"Error: Unable to download files in batch {batch_num + 1} (status code {response.status_code})")
                    logger.debug(f"Response headers: {response.headers}")
                    logger.debug(f"Response content: {response.text}")
                    raise Exception(f"Download failed with status code {response.status_code}")

            except Exception as e:
                logger.exception(f"An error occurred while downloading batch {batch_num + 1}: {e}")
                logger.info("Retrying the failed batch...")

                # Implement a simple retry mechanism for the failed batch
                retry_attempts = 3
                for attempt in range(1, retry_attempts + 1):
                    logger.info(f"Retry attempt {attempt} for batch {batch_num + 1}")
                    try:
                        response = session.post(
                            data_endpt,
                            data=json.dumps(params),
                            headers={"Content-Type": "application/json"},
                            stream=True,
                            timeout=300
                        )
                        if response.status_code == 200:
                            # Process the response as before
                            response_head_cd = response.headers.get("Content-Disposition", "")
                            file_name_match = re.findall(r'filename="*(.+?)"*$', response_head_cd)
                            if file_name_match:
                                file_name = file_name_match[0]
                            else:
                                file_name = f"gdc_download_batch_{batch_num + 1}_retry_{attempt}.tar.gz"
                            logger.debug(f"Received file name: {file_name}")

                            # Save the content to a file in the outputs directory
                            file_path = os.path.join(outputs_dir, file_name)
                            with open(file_path, "wb") as output_file:
                                for chunk in response.iter_content(chunk_size=1024 * 1024):
                                    if chunk:
                                        output_file.write(chunk)
                            logger.info(f"Downloaded data saved to {file_path}")

                            # Extract the tar.gz file into outputs directory
                            if file_name.endswith(".tar.gz"):
                                extract_path = outputs_dir
                                logger.debug(f"Extracting files to {extract_path}")
                                with tarfile.open(file_path, "r:gz") as tar:
                                    tar.extractall(path=extract_path)
                                logger.info(f"Files extracted to {extract_path}")

                                # Remove the tar.gz file after extraction
                                os.remove(file_path)
                                logger.debug(f"Removed archive file {file_path}")

                            # Break out of retry loop on success
                            break
                        else:
                            logger.error(f"Retry {attempt} failed with status code {response.status_code}")
                            if attempt == retry_attempts:
                                logger.error(f"All retry attempts failed for batch {batch_num + 1}. Skipping this batch.")
                            else:
                                time.sleep(5)
                    except Exception as retry_exception:
                        logger.exception(f"Retry {attempt} encountered an error: {retry_exception}")
                        if attempt == retry_attempts:
                            logger.error(f"All retry attempts failed for batch {batch_num + 1}. Skipping this batch.")
                        else:
                            time.sleep(5)

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()