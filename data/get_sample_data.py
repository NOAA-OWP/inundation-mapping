#!/usr/bin/env python3

import argparse
import os
import re
import shutil
import subprocess

from dotenv import load_dotenv


# from tools_shared_variables import INPUTS_DIR, OUTPUTS_DIR, TEST_CASES_DIR


load_dotenv('/foss_fim/src/bash_variables.env')

pre_clip_huc_dir = os.environ["pre_clip_huc_dir"]


def get_sample_data(huc, data_path: str, output_root_folder: str):
    """
    Create input data for the flood inundation model

    Parameters
    ----------
    huc : str
        HUC to process
    input_path : str
        Path to the input data
    output_root_folder : str
        Path to save the output data
    """

    def get_validation_hucs(org: str):
        """
        Get the list of HUCs for validation

        Parameters
        ----------
        org : str
            Organization name
        """

        return [
            d
            for d in os.listdir(f'/data/test_cases/{org}_test_cases/validation_data_{org}')
            if re.match(r'^\d{8}$', d)
        ]

    def copy_validation_data(org: str, huc: str, data_path: str, output_data_path: str):
        """
        Make the path to the validation data

        Parameters
        ----------
        org : str
            Organization name
        huc : str
            HUC
        input_path : str
            Path to the input data
        output_inputs_path : str
            Path to save the output data
        """

        validation_path = f'test_cases/{org}_test_cases/validation_data_{org}/{huc}'

        output_validation_path = os.path.join(output_data_path, validation_path)
        os.makedirs(output_validation_path, exist_ok=True)

        shutil.copytree(os.path.join(data_path, validation_path), output_validation_path, dirs_exist_ok=True)

    def copy_file(input_path, output_path, basename):
        """
        Copies a file if it doesn't already exist

        Parameters
        ----------
        input_path : str
            Path to the input data
        output_path : str
            Path to save the output data
        basename : str
            Basename of the file
        """

        if not os.path.exists(os.path.join(output_path, basename)):
            print(f"Copying {os.path.join(input_path, basename)} to {output_path}")
            os.makedirs(output_path, exist_ok=True)
            shutil.copy2(os.path.join(input_path, basename), output_path)
        else:
            print(f"{os.path.join(output_path, basename)} already exists.")

    def copy_folder(input_path, output_path):
        """
        Copies a folder if it doesn't already exist

        Parameters
        ----------
        input_path : str
            Path to the input data
        output_path : str
            Path to save the output data
        """

        if not os.path.exists(output_path):
            print(f"Copying {input_path} to {output_path}")
            shutil.copytree(input_path, output_path, dirs_exist_ok=True)
        else:
            print(f"{output_path} already exists.")

    nws_validation_hucs = get_validation_hucs('nws')
    usgs_validation_hucs = get_validation_hucs('usgs')

    input_path = os.path.join(data_path, 'inputs')

    output_inputs_path = os.path.join(output_root_folder, 'inputs')

    if isinstance(huc, str):
        huc = [huc]
    else:
        huc = huc.split(',')

    if not os.path.exists(input_path):
        raise FileNotFoundError(f'{input_path} does not exist')

    ## 3dep_dems
    dem_path = os.path.join('3dep_dems', '10m_5070')
    dem_input_path = os.path.join(input_path, dem_path)
    dem_output_path = os.path.join(output_inputs_path, dem_path)

    os.makedirs(dem_output_path, exist_ok=True)

    # dem_domain
    copy_file(dem_input_path, dem_output_path, 'HUC6_dem_domain.gpkg')

    for huc in huc:
        # dem
        copy_file(dem_input_path, dem_output_path, f'HUC6_{huc[:6]}_dem.tif')

        ## pre_clip_huc8
        pre_clip_huc_date = pre_clip_huc_dir.split('/')[-1]
        copy_folder(
            os.path.join(pre_clip_huc_dir, huc),
            os.path.join(output_inputs_path, 'pre_clip_huc8', pre_clip_huc_date, huc),
        )

        ## validation data
        if huc in nws_validation_hucs:
            copy_validation_data('nws', huc, data_path, output_root_folder)
        if huc in usgs_validation_hucs:
            copy_validation_data('usgs', huc, data_path, output_root_folder)

    # create VRT
    print('Creating VRT')
    command = ['gdalbuildvrt', os.path.join(dem_output_path, 'fim_seamless_3dep_dem_10m_5070.vrt')]
    dem_list = [os.path.join(dem_output_path, x) for x in os.listdir(dem_output_path) if x.endswith(".tif")]
    command.extend(dem_list)
    subprocess.call(command)

    ## ahps_sites
    copy_file(
        os.path.join(input_path, 'ahps_sites'), os.path.join(output_inputs_path, 'ahps_sites'), 'nws_lid.gpkg'
    )

    ## huc_lists
    copy_folder(os.path.join(input_path, 'huc_lists'), os.path.join(output_inputs_path, 'huc_lists'))

    ## nld_vectors
    copy_file(
        os.path.join(input_path, 'nld_vectors'),
        os.path.join(output_inputs_path, 'nld_vectors'),
        'Levee_protected_areas.gpkg',
    )

    ## nwm_hydrofabric (nwm_hydrofabric/nwm_lakes.gpkg)
    copy_file(
        os.path.join(input_path, 'nwm_hydrofabric'),
        os.path.join(output_inputs_path, 'nwm_hydrofabric'),
        'nwm_lakes.gpkg',
    )

    ## rating_curve
    bankfull_flows = os.path.join('rating_curve', 'bankfull_flows')
    copy_file(
        os.path.join(input_path, bankfull_flows),
        os.path.join(output_inputs_path, bankfull_flows),
        'nwm_high_water_threshold_cms.csv',
    )

    copy_folder(
        os.path.join(input_path, 'rating_curve', 'nwm_recur_flows'),
        os.path.join(output_inputs_path, 'rating_curve', 'nwm_recur_flows'),
    )

    ras2fim_exports = os.path.join('rating_curve', 'ras2fim_exports')
    copy_file(
        os.path.join(input_path, ras2fim_exports),
        os.path.join(output_inputs_path, ras2fim_exports),
        'reformat_ras_rating_curve_points_rel_101.gpkg',
    )

    variable_roughness = os.path.join('rating_curve', 'variable_roughness')
    copy_file(
        os.path.join(input_path, variable_roughness),
        os.path.join(output_inputs_path, variable_roughness),
        'mannings_global_06_12.csv',
    )

    water_edge_database = os.path.join('rating_curve', 'water_edge_database', 'calibration_points')
    copy_folder(
        os.path.join(input_path, water_edge_database), os.path.join(output_inputs_path, water_edge_database)
    )

    ## usgs_gages
    copy_file(
        os.path.join(input_path, 'usgs_gages'),
        os.path.join(output_inputs_path, 'usgs_gages'),
        'usgs_gages.gpkg',
    )
    copy_file(
        os.path.join(input_path, 'usgs_gages'),
        os.path.join(output_inputs_path, 'usgs_gages'),
        'usgs_rating_curves.csv',
    )

    ## recurr_flows
    recurr_flows = os.path.join('inundation_review', 'inundation_nwm_recurr', 'nwm_recurr_flow_data')
    copy_folder(os.path.join(data_path, recurr_flows), os.path.join(output_root_folder, recurr_flows))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create input data for the flood inundation model')
    parser.add_argument('-u', '--huc', help='HUC to process')
    parser.add_argument('-i', '--data-path', help='Path to the input data')
    parser.add_argument('-o', '--output-root-folder', help='Path to save the output data')

    args = parser.parse_args()

    get_sample_data(**vars(args))
