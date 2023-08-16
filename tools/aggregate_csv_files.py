import argparse
import fnmatch
import os

import pandas as pd


def locate(pattern, root_path):
    for path, dirs, files in os.walk(os.path.abspath(root_path)):
        for filename in fnmatch.filter(files, pattern):
            yield os.path.join(path, filename)


def read_csvs_to_df(files_to_merge, head_row):
    # df = pd.concat((pd.read_csv(f,usecols=["HUC", "feature_id", "HydroID", "last_updated", "submitter", "adjust_ManningN"],dtype={'feature_id': 'int64','HUC': str}) for f in files_to_merge), ignore_index=True)
    li = []
    for file_in in files_to_merge:
        print(file_in)
        ## Use below for merging hydroTables for calib n value data
        # df = pd.read_csv(file_in,usecols=["HUC", "feature_id", "HydroID", "last_updated", "submitter", "adjust_ManningN"],dtype={'feature_id': 'int64','HUC': str}, index_col=None, header=0)
        # df = df[df['adjust_ManningN'].notna()]
        # df.drop_duplicates(subset=['HydroID'],inplace=True)
        df = pd.read_csv(file_in, index_col=None, header=head_row)  # dtype={'feature_id': 'int64'}
        df = df[df['Unnamed: 0'] != 'HydroID']
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)
    return frame


def write_aggregate(frame, output_file):
    print('Writing new csv file: ' + output_file)
    frame.to_csv(output_file, index=False)


def concat_files(files_to_merge):
    # joining files with concat and read_csv
    print('Concatenating all matching csv files...')
    df_concat = pd.concat(map(pd.read_csv, files_to_merge), ignore_index=True)


def run_prep(fim_dir, file_search_str, head_row, output_file):
    assert os.path.isdir(fim_dir), 'ERROR: could not find the input fim_dir location: ' + str(
        fim_dir
    )

    files_to_merge = [js for js in locate('*' + file_search_str + '*.csv', fim_dir)]
    if len(files_to_merge) > 0:
        print('Found files: ' + str(len(files_to_merge)))

        aggreg_df = read_csvs_to_df(files_to_merge, head_row)
        write_aggregate(aggreg_df, output_file)

    else:
        print('Did not find any files using tag: ' + '*' + file_search_str + '*.csv')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Simple tool to search for csv files (using wildcard text) within a fim output directory and then aggregate all files into a single csv"
    )
    parser.add_argument(
        '-fim_dir',
        '--fim-dir',
        help='FIM output dir (e.g. data/outputs/xxxx/',
        required=True,
        type=str,
    )
    parser.add_argument(
        '-search_str', '--file-search-str', help='File search string', required=True, type=str
    )
    parser.add_argument(
        '-head_row',
        '--header-row',
        help='Optional: header row to parse (default=0)',
        default=0,
        required=False,
        type=int,
    )
    parser.add_argument(
        '-out_csv', '--output-csv', help='full filepath to write new csv', required=True, type=str
    )

    args = vars(parser.parse_args())
    fim_dir = args['fim_dir']
    file_search_str = args['file_search_str']
    head_row = args['header_row']
    output_file = args['output_csv']

    run_prep(fim_dir, file_search_str, head_row, output_file)
