import argparse
import pandas as pd





def collate_catchment_attributes(fim_directory,sierra_test_input,output_csv_destination):
    root_dir = '/data/outputs/calebs_run'

    aggregate_df = pd.DataFrame()

    for huc_dir in os.listdir(root_dir):
        hydroTable = pd.read_csv(os.path.join(root_dir, huc_dir, "hydroTable.csv"))
        aggregate_df.append(hydroTable)




if __name__ == '__main__':
    """
    decription of file
    TODO
    """

    parser = argparse.ArgumentParser(description='collates catchment attributes from determined source')
    
    parser.add_argument('-d','--fim-directory',help='Parent directory of FIM-required datasets.',required=True)
    parser.add_argument('-s', '--sierra-test-input', help='Optional:layer containing sierra test by hydroId',required=False)
    parser.add_argument('-o','--output-csv-destination',help='location and name for output csv',required=True)
    
    args = vars(parser.parse_args())

    fim_directory = args['fim_directory']
    sierra_test_input = args['sierra_test_input']
    output_csv_destination = args['output_csv_destination']
    
    collate_catchment_attributes(fim_directory,sierra_test_input,output_csv_destination)