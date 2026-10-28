#!/usr/bin/env python3

import os
import subprocess
import traceback

import tools.catfim.catfim_shared_functions as csf


def run_flows2fim_fim_from_s3(
    hydrovis_bucket_name,
    collection_parent_folder,
    model_name,
    flows2fim_path,
    controls_csv,
    output_extent_tif,
):
    '''
    Tool for running flows2fim without downloading anything

    Arguments
    ---------
    hydrovis_bucket_name - str
        Name of hydrovis S3 bucket (example: hydrovis-ti-deployment-us-east-1)
    collection_parent_folder - str
        Name of ripple collection parent folder (example: "20260211_merged").
    model_name - str
        Name of ripple model folder (example: ble_12090301).
    flows2fim_path - str
        Filepath to the downloaded and installed flows2fim software.
    controls_csv - str
        Filepath to the controls CSV.
    output_extent_tif - str
        Filepath for the inundated output extent tif.

    Returns
    -------
    is_success - bool
        Status of whether the flows2fim command ran sucessfully.
    status_msg - list of str
        h

    is_success, status_msg = run_flows2fim_fim_from_s3(
        hydrovis_bucket_name, collection_parent_folder, model_name, flows2fim_path, controls_csv, output_extent_tif,
    )

    '''
    aws_creds_file = '/data/config/aws_credentials.env'

    is_success = False
    status_msg = []

    # Configure AWS credentials
    hv_aws_access_key, hv_aws_secret_key, hv_aws_region = csf.get_aws_credentials(aws_creds_file)

    # Define the library extent path
    library_extent_path = os.path.join(
        'vsis3',
        hydrovis_bucket_name,
        'fim',
        'ripple',
        collection_parent_folder,
        'collections',
        model_name,
        'library_extent',
    )

    flows2fim_subprocess_cmd = [
        flows2fim_path,
        'fim',
        '-lib',
        library_extent_path,
        '-c',
        controls_csv,
        '-fmt',
        'cog',
        '-o',
        output_extent_tif,
        '-type',
        'extent',
    ]
    try:
        # Define AWS credentials
        aws_env = os.environ.copy()  # Copy existing system variables
        aws_env.update(
            {
                "AWS_ACCESS_KEY_ID": hv_aws_access_key,
                "AWS_SECRET_ACCESS_KEY": hv_aws_secret_key,
                "AWS_DEFAULT_REGION": hv_aws_region,
            }
        )

        # Use subprocess to run flows2fim (with the AWS env)
        result = subprocess.run(
            flows2fim_subprocess_cmd, capture_output=True, text=True, env=aws_env, check=True
        )

        if result.stdout:
            result_list = result.stdout.strip().split('\n')
            for result in result_list:
                status_msg.append(result)
                # logging.info(f"{huc} : {ahps_site} : {magnitude} - {result}") # TODO: move outside of function for CatFIM
            is_success = True
        else:
            status_msg.append('No outputs returned for flows2fim subprocess')

    except FileNotFoundError:
        status_msg.append("A critical error occurred while attempting HEC-RAS inundation: flows2fim fim command not found")
    except subprocess.CalledProcessError as e:
        status_msg.append(f'flows2fim subprocess failed with non-zero exit code {e.returncode}: {e.stderr}')
    except Exception as e:
        status_msg.append(f"flows2fim subprocess failed: {e} - {traceback.format_exc()}")

    return is_success, status_msg

