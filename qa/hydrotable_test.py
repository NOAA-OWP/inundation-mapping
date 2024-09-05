import argparse
import os
import re

import pandas as pd
from dotenv import load_dotenv


# from qa.qa_test import QATest


def run_hydrotable_test(fim_version: str, huc: str = None) -> dict:
    """
    This function will test the hydrotable for the required columns and positive values.
    If the hydrotable is a string, it will read the csv file.
    If the hydrotable is a dataframe, it will test the dataframe.
    If the hydrotable is not a string or dataframe, it will return an error.

    Parameters
    ----------
    fim_version : str
        Path to the FIM folder (e.g. "/outputs/fim_4_4_0_0").
    huc : str (optional)
        HUC8.

    Returns
    -------
    test : dict
        Dictionary of test results.
    """

    results = {}

    if not os.path.exists(fim_version):
        results['error'] = f"{fim_version} does not exist"

    else:
        load_dotenv(os.path.join(fim_version, 'params.env'))
        load_dotenv(os.path.join(fim_version, 'runtime_args.env'))

        if huc is None:
            # Read all HUCs in the FIM folder
            huc_list = [d for d in os.listdir(fim_version) if re.match(r'\d{8}', d)]
        else:
            huc_list = [huc]

        for huc in huc_list:
            results = hydrotable_test(fim_version, huc, results)

    print(results)

    return results


def hydrotable_test(fim_version: str, huc: str, results: dict = None) -> dict:
    """
    This function will test the hydrotable for the required columns and positive values.
    If the hydrotable is a string, it will read the csv file.
    If the hydrotable is a dataframe, it will test the dataframe.
    If the hydrotable is not a string or dataframe, it will return an error.

    Parameters
    ----------
    fim_version : str
        Path to the FIM folder (e.g. "/outputs/fim_4_4_0_0").
    huc : str (optional)
        HUC8.
    results : dict (optional)
        Dictionary of test results.

    Returns
    -------
    test : dict
        Dictionary of test results.
    """

    def check_values(df: pd.DataFrame, parameter: str, dtype, op: str = None) -> bool:
        # Check if parameter column exists and all values are >= 0
        if parameter in df.columns:
            if dtype == bool:
                if df[parameter].dtype == bool:
                    if df[parameter].all():
                        return True
                    else:
                        return f"Some values in {parameter} are False"
                else:
                    return f"{parameter} column is not of type {dtype}"
            elif dtype == int or dtype == float:
                if op is None:
                    return f"Missing operator for {parameter}"
                else:
                    if df[parameter].dtype == dtype:
                        if (eval(f'df["{parameter}"] {op}')).all():
                            return True
                        else:
                            return f"Some values in {parameter} are not {op}"
                    else:
                        return f"{parameter} column is not of type {dtype}"
        else:
            return f"{parameter} column is missing"

    results[huc] = {}

    hydrotable = os.path.join(fim_version, huc, 'hydrotable.csv')

    if isinstance(hydrotable, str):
        hydrotable = pd.read_csv(hydrotable)

    if not isinstance(hydrotable, pd.DataFrame):
        results[huc]['error'] = f"{hydrotable} is not a pandas DataFrame"
        return results

    # Check column values
    results[huc]['HUC'] = check_values(hydrotable, 'HUC', int, '> 0')
    results[huc]['HydroID'] = check_values(hydrotable, 'HydroID', int, '> 0')
    results[huc]['branch_id'] = check_values(hydrotable, 'branch_id', int, '>= 0')
    results[huc]['feature_id'] = check_values(hydrotable, 'feature_id', int, '> 0')
    results[huc]['Volume (m3)'] = check_values(hydrotable, 'Volume (m3)', float, '>= 0')
    results[huc]['stage'] = check_values(hydrotable, 'stage', float, '>= 0')
    results[huc]['discharge_cms'] = check_values(hydrotable, 'discharge_cms', float, '>= 0')

    if os.getenv('bathymetry_adjust') == 'True':
        pass

    if os.getenv('src_bankfull_toggle') == 'True':
        # Check bankfull
        pass
        if os.getenv('src_subdiv_toggle') == 'True':
            # Check subdivision
            results[huc]['subdiv_applied'] = check_values(hydrotable, 'subdiv_applied', bool)
            results[huc]['channel_n'] = check_values(hydrotable, 'channel_n', float, '>= 0')
            results[huc]['overbank_n'] = check_values(hydrotable, 'overbank_n', float, '>= 0')
            results[huc]['subdiv_discharge_cms'] = check_values(
                hydrotable, 'subdiv_discharge_cms', float, '>= 0'
            )

    # TODO: Test if 'calb_applied' is True, 'calb_coef_final' > 0
    # results[huc]['calb_applied'] = hydrotable['calb_applied'] and hydrotable['calb_coef_final'] > 0

    if os.getenv('skipcal') == '0' and os.getenv('src_bankfull_toggle') == 'True':
        if os.getenv('src_adjust_usgs') == 'True':
            pass
        if os.getenv('src_adjust_ras2fim') == 'True':
            pass
        if os.getenv('src_adjust_spatial') == 'True':
            pass

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test the hydrotable')
    parser.add_argument(
        '-f',
        '--fim-version',
        type=str,
        help='Path to the FIM folder (e.g. "/outputs/fim_4_4_0_0")',
        required=True,
    )
    parser.add_argument('-u', '--huc', type=str, help='HUC8', required=False)

    args = parser.parse_args()

    run_hydrotable_test(**vars(args))
