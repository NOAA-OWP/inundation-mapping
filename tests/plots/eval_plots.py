#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import argparse
#from natsort import natsorted
import geopandas as gpd
from utils.shared_functions import filter_dataframe, boxplot, scatterplot, barplot
def eval_plots(metrics_csv, workspace, versions, stats = ['CSI','FAR','TPR'] , alternate_ahps_query = False, spatial_ahps = False):

    '''
    Creates plots and summary statistics for a csv from synthesize_test_cases. The only required inputs are:
        metrics_csv, evaluation_dataset, workspace. 
    Outputs include:
        aggregate_*.csv --> this csv contains the aggregated total statistics (CSI, FAR, POD) using the summed area_sq_km fields
        *_common_sites.csv --> this csv contains the base resolution data (e.g ahps: nws_lid; ble: huc08) retained when doing aggregation/plots for each magnitude. LIDs or HUCs occur in all versions supplied for analysis. For example if FIM 1, FIM 2, FIM 3.0.0.3 were versions supplied, the common sites for ble would list all huc 08 watersheds that had data for all 3 versions for the 100 year flood. The same analysis is then redone for the 500 year flood. Hucs listed in one may not be present in the other magnitude. This is especially evident in ahps. The length of the common sites is also displayed on plots as an annotation.
        *_data.csv --> This is the filtered down metrics csv. All statistics and plots are created using this csv. For example, for BLE if a huc is not found in some of the supplied versions then that huc is removed (or filtered).
        CSI_aggr_*.png --> this is a bar plot of the aggregated CSI score across all supplied versions. Sample size is annoted (see list of sites and length in corresponding column in *_common_sites.csv)
        CSI_*.png --> this is a box plot of CSI of all sites (weighted equally). Sample size is annoted (see list of sites and length in corresponding column in *_common_sites.csv)
        FAR_*.png --> this is a box plot of FAR of all sites (weighted equally). Sample size is annoted (see list of sites and length in corresponding column in *_common_sites.csv)
        TPR_*.png --> this is a box plot of TPR/POD of all sites (weighted equally). Sample size is annoted (see list of sites and length in corresponding column in *_common_sites.csv)
        ScatterPlot_*.png --> This is a scatter plot comparing the last two versions supplied. One scatterplot is created for each magnitude (e.g. ble: 100yr and 500yr)

    Parameters
    ----------
    metrics_csv : STRING
        File path to csv containing combined metrics (produced as part of synthesize_test_cases)
    workspace : STRING
        Path to the output workspace. Subdirectories will be created reflecting the evaluation dataset.
    versions: LIST
        A list of versions to be aggregated/plotted. Uses the "startswith" approach. Versions should be supplied in the order they are to be plotted. 
        For example:
            ['fim_', 'fb'] = This will evaluate all versions that start with fim_ (fim_1, fim_2, fim_3) and any feature branch that starts with "fb". To esbalish version order, the fim versions naturally sorted and then fb is appended to the order. Scatter plot is created using the last two elements of the versions (last fim_3 version and fb)
            ['fim_3_0','fb'] = this will evaluate all versions that start with fim_3_0 as well as feature branches starting with "fb". fim_3_0 versions are naturally sorted and the fb is appended. Scatter plot of the last fim_3_0 version and the fb are produced. 
        When the metric csv is input the data is filtered so that the same hucs (or nws_lid) are evaluated across all versions for each magnitude. For example: if fim_v1, fim_v2, fimv_3 are selected, then hucs that have scores for fim_v1, fim_v2, and fim_v3 are used for analysis all others are discarded (same for nws_lid for ahps). Sample size is the same for a given magnitude (100yr/500yr or Action, Minor, Moderate, Major)
    stats: LIST
        A list of statistics to be plotted. Must be identical to column field in metrics_csv.
    alternate_ahps_query : STRING, optional
        Currently the default ahps query is same as done for apg goals. If a different query is desired (e.g. different bad sites selected) then use the default query to help build a new query. If query is supplied that query will supercede the default query. The default is False.
    spatial_ahps : DICTIONARY, optional
        The default is false. Otherwise a dictionary with keys as follows: 
            'static' --> Path to AHPS point file created during creation of FIM 3 static libraries.
            'evaluated' --> Path to extent file created during the creation of the NWS/USGS AHPS preprocessing.
            'metadata' --> Path to previously created file that contains metadata about each site (feature_id, wfo, rfc and etc).
        No spatial layers will be created if set to False, if a dictionary is supplied then a spatial layer is produced.
    ble_scatterplot : BOOL, optional
        A scatter plot comparing FIM 2 and FIM 3 ble HUCs. The default is True. If FIM 2 is not available, then set to False.

    Returns
    -------
    all_datasets : DICT
        Dictionary of pandas DataFrames of all datasets used to create plots/aggregated statistics.

    '''
    
    #Import metrics csv as DataFrame and initialize all_datasets dictionary
    csv_df = pd.read_csv(metrics_csv)
    #Filter out versions based on supplied version list
    metrics = csv_df.query('version.str.startswith(tuple(@versions))')
    
    ###################################################################
    #To Play With Later, if changing this also experiment with version_order section
    #versions_joined = '|'.join(versions)
    #metrics = csv_df.query('version.str.contains(@versions_joined)')
    #Version order section mod, test this thoroughly seems to create duplicates
    #selected_versions = [sel_version for sel_version in all_versions if version in sel_version]
    ####################################################################
    
    #Group by benchmark source
    benchmark_by_source = metrics.groupby('benchmark_source')

    #Cycle through each group of data, based on the benchmark source. Perform a further filter so that all desired versions contain same instances of the base_resolution (e.g. for ble: keep all hucs that exist across all desired versions for a given magnitude; for ahps: keep all nws_lid sites that exist across all versions for a given magnitude). Write the final filtered dataset to a new dictionary with the source (key) and tuple (metrics dataframe, contributing sites).
    all_datasets = {}
    for benchmark_source, benchmark_metrics in benchmark_by_source:        
        
        #Split the benchmark source to parent source and subgroup
        source, *subgroup = benchmark_source.split('_')
        
        #If source is 'ahps' set the base resolution and additional query (use alternate query if passed). Append filtered datasets to all_datasets dictionary.
        if source == 'ahps':
            
            #Set the base processing unit for the ahps runs.
            base_resolution = 'nws_lid'
            
            #Default query (used for APG) it could be that bad_sites should be modified. If so pass an alternate query using the "evaluation_query"
            bad_sites = ['grfi2','ksdm7','hohn4','rwdn4']
            query = "not flow.isnull() & masked_perc<97 & not nws_lid in @bad_sites"

            #If alternate ahps evaluation query argument is passed, use that.
            if alternate_ahps_query:
                query = alternate_ahps_query

            #Filter the dataset based on query
            ahps_metrics = benchmark_metrics.query(query)
            
            #Filter out all instances where the base_resolution doesn't exist across all desired fim versions.
            all_datasets[benchmark_source] = filter_dataframe(ahps_metrics, base_resolution)
                     
        #If source is 'ble', set base_resolution and append ble dataset to all_datasets dictionary
        if source == 'ble':
            
            #Set the base processing unit for ble runs
            base_resolution = 'huc'
            
            #Filter out all instances where base_resolution doesn't exist across all desired fim versions.
            all_datasets[benchmark_source] = filter_dataframe(benchmark_metrics, base_resolution)
            
    #For each dataset in all_datasets, generate plots and aggregate statistics.
    for dataset_name, (dataset, sites) in all_datasets.items():
        
        #Define and create the output workspace as a subfolder within the supplied workspace
        output_workspace = Path(workspace) / dataset_name
        output_workspace.mkdir(parents = True, exist_ok = True)         
                
        #Write out the filtered dataset and common sites to file
        dataset.to_csv(output_workspace / (f'{dataset_name}_data.csv'), index = False)
        sites_pd = pd.DataFrame.from_dict(sites, orient = 'index').transpose()
        sites_pd.to_csv(output_workspace / (f'{dataset_name}_common_sites.csv'), index = False)
        
        #set the order of the magnitudes and define base resolution.
        if dataset_name == 'ble':
            magnitude_order = ['100yr', '500yr']
            base_resolution = 'huc'
        elif 'ahps' in dataset_name:
            magnitude_order = ['action','minor','moderate','major']
            base_resolution = 'nws_lid'

        #Calculate aggregated metrics based on total_sq_km fields.
        dataset_sums = dataset.groupby(['version', 'magnitude'])[['TP_area_km2','FP_area_km2','FN_area_km2']].sum()
        dataset_sums['csi'] = dataset_sums['TP_area_km2']/(dataset_sums['TP_area_km2'] + dataset_sums['FP_area_km2'] + dataset_sums['FN_area_km2'])
        dataset_sums['far'] = dataset_sums['FP_area_km2']/(dataset_sums['TP_area_km2'] + dataset_sums['FP_area_km2'])
        dataset_sums['pod'] = dataset_sums['TP_area_km2']/(dataset_sums['TP_area_km2'] + dataset_sums['FN_area_km2'])
        dataset_sums = dataset_sums.reset_index()
        
        #Write aggregated metrics to file.
        dataset_sums.to_csv(output_workspace / f'aggregate_{dataset_name}.csv', index = False )

        #Order all versions that start all elements from desired_versions and naturally sort them. This will be the hue order for the generated plots.
        all_versions = list(dataset.version.unique())        
        version_order = []
        #For each version supplied by the user
        for version in versions:
            #Select all the versions that start with the supplied version.
            selected_versions = [sel_version for sel_version in all_versions if sel_version.startswith(version)]
            #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            #Naturally sort selected_versions
            #selected_versions = natsorted(selected_versions)
            #Populate version order based on the sorted subsets.
            version_order.extend(selected_versions)

        #Define textbox which will contain the counts of each magnitude.
        textbox = []
        for magnitude in sites:
            count = len(sites[magnitude])
            line_text = f'{magnitude.title()} Sites = {count}'
            textbox.append(line_text)
        textbox = '\n'.join(textbox)

        #Create aggregate barplot
        aggregate_file = output_workspace / (f'CSI_aggr_{dataset_name}.png')        
        barplot(dataframe = dataset_sums, x_field = 'magnitude', x_order = magnitude_order, y_field = 'csi', hue_field = 'version', ordered_hue = version_order, title_text = f'Aggregate {dataset_name.upper()} FIM Scores', textbox_str = textbox, simplify_legend = True, dest_file = aggregate_file)
        
        #Create box plots for each metric in supplied stats.
        for stat in stats:
            output_file = output_workspace / (f'{stat}_{dataset_name}.png')    
            boxplot(dataframe = dataset, x_field = 'magnitude', x_order = magnitude_order, y_field = stat, hue_field = 'version', ordered_hue = version_order, title_text = f'{dataset_name.upper()} FIM Sites', textbox_str = textbox, simplify_legend = True, dest_file = output_file)
        
        #Get the last 2 versions from the version order if version order more than 1 element
        if len(version_order) > 1:            
            *discarded_versions, x_version, y_version = version_order
            for magnitude in magnitude_order:
                #Scatterplot comparison between last 2 versions in version_order variable
                x_csi = dataset.query(f'version == "{x_version}" & magnitude == "{magnitude}"')[[base_resolution, 'CSI']]
                y_csi = dataset.query(f'version == "{y_version}" & magnitude == "{magnitude}"')[[base_resolution, 'CSI']]
                plotdf = pd.merge(x_csi, y_csi, on = base_resolution, suffixes = (f"_{x_version}",f"_{y_version}"))
                #Define arguments for scatterplot function.
                title_text = f'CSI {magnitude}'
                dest_file = output_workspace / f'ScatterPlot_{magnitude}_{x_version}_{y_version}.png'
                scatterplot(dataframe = plotdf, x_field = f'CSI_{x_version}', y_field = f'CSI_{y_version}', title_text = title_text, annotate = True, dest_file = dest_file)
    

    #######################################################################
    #Create spatial layers with threshold and mapping information
    ########################################################################
    if spatial_ahps:

        #Read in supplied shapefile layers
        #Layer containing metadata for each site (feature_id, wfo, etc). Convert nws_lid to lower case.
        ahps_metadata = gpd.read_file(spatial_ahps['metadata'])
        ahps_metadata['nws_lid'] = ahps_metadata['nws_lid'].str.lower()
        metadata_crs = ahps_metadata.crs
        #Extent layer generated from preprocessing NWS/USGS datasets
        evaluated_ahps_extent = gpd.read_file(spatial_ahps['evaluated'])
        
        #Extent layer generated from static ahps library preprocessing
        static_library = gpd.read_file(spatial_ahps['static'])
        
        #Fields to keep
        #Get list of fields to keep in merge
        preserved_static_library_fields = ['nws_lid'] + [i for i in static_library.columns if i.startswith(('Q','S'))]                
        #Get list of fields to keep in merge.
        preserved_evaluated_ahps_fields = ['nws_lid', 'source', 'geometry'] + [i for i in evaluated_ahps_extent.columns if i.startswith(('action','minor','moderate','major'))]

        #Join tables to evaluated_ahps_extent
        evaluated_ahps_extent = evaluated_ahps_extent[preserved_evaluated_ahps_fields]
        evaluated_ahps_extent = evaluated_ahps_extent.merge(ahps_metadata, on = 'nws_lid')
        evaluated_ahps_extent['geometry'] = evaluated_ahps_extent['geometry_y']
        evaluated_ahps_extent.drop(columns = ['geometry_y','geometry_x'], inplace = True)
        evaluated_ahps_extent = evaluated_ahps_extent.merge(static_library[preserved_static_library_fields], on = 'nws_lid')
        
        #merge metrics 
        final_join = pd.DataFrame()
        for dataset_name, (dataset, sites) in all_datasets.items():
            if 'ahps' in dataset_name:
                subset = evaluated_ahps_extent.query(f'source == "{dataset_name}"')                        
                dataset_with_subset = dataset.merge(subset, on = 'nws_lid')
                final_join = final_join.append(dataset_with_subset)
                    
        final_join['version'] = final_join.version.str.split('_nws|_usgs').str[0]
        final_join['source'] = final_join['source'].str.split('ahps_').str[1]
        gdf = gpd.GeoDataFrame(final_join, geometry = final_join['geometry'], crs = metadata_crs)
        gdf.to_file(output_workspace.parent / 'nws_usgs_site_info.shp') 
                    


#######################################################################
if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Plot and aggregate statistics for benchmark datasets (BLE/AHPS libraries)')
    parser.add_argument('-m','--metrics_csv', help = 'Metrics csv created from synthesize test cases.', required = True)
    parser.add_argument('-w', '--workspace', help = 'Output workspace', required = True)
    parser.add_argument('-v', '--versions', help = 'List of versions to be plotted/aggregated. Versions are filtered using the "startswith" approach. For example, ["fim_","fb1"] would retain all versions that began with "fim_" (e.g. fim_1..., fim_2..., fim_3...) as well as any feature branch that began with "fb". An other example ["fim_3","fb"] would result in all fim_3 versions being plotted along with the fb.', nargs = '+', default = [], required = True)
    parser.add_argument('-s', '--stats', help = 'List of statistics (abbrev to 3 letters) to be plotted/aggregated', nargs = '+', default = ['CSI','TPR','FAR'], required = False)
    parser.add_argument('-q', '--alternate_ahps_query',help = 'Alternate filter query for AHPS. Default is: "not nws_lid.isnull() & not flow.isnull() & masked_perc<97 & not nws_lid in @bad_sites" where bad_sites are (grfi2,ksdm7,hohn4,rwdn4)', default = False, required = False)
    parser.add_argument('-sp', '--spatial_ahps', help = 'If spatial point layer is desired, supply a csv with 3 lines of the following format: metadata, path/to/metadata/shapefile\nevaluated, path/to/evaluated/shapefile\nstatic, path/to/static/shapefile.', default = False, required = False)
    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    
    #If errors occur reassign error to True
    error = False
    #Create dictionary if file specified for spatial_ahps
    if args['spatial_ahps']:
        #Create dictionary
        spatial_dict = {}
        with open(args['spatial_ahps']) as file:
            for line in file:
                key, value = line.strip('\n').split(',')
                spatial_dict[key] = Path(value)
        args['spatial_ahps'] = spatial_dict
        #Check that all required keys are present. If they are, overwrite args with spatial_dict
        required_keys = set(['metadata', 'evaluated', 'static'])
        if required_keys - spatial_dict.keys():
          print('\n Required keys are: metadata, evaluated, static')
          error = True
        else:
            args['spatial_ahps'] = spatial_dict


    #Finalize Variables
    m = args['metrics_csv']
    w = args['workspace']
    v = args['versions']
    s = args['stats']
    q = args['alternate_ahps_query']
    sp= args['spatial_ahps']

    #Run eval_plots function
    if not error:        
        eval_plots(metrics_csv = m, workspace = w, versions = v, stats = s, alternate_ahps_query = q, spatial_ahps = sp)