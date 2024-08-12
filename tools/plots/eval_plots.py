#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import argparse
from natsort import natsorted
import geopandas as gpd
from utils.shared_functions import filter_dataframe, boxplot, scatterplot, barplot
def eval_plots(metrics_csv, workspace, versions = [], stats = ['CSI','FAR','TPR'] , alternate_ahps_query = False, spatial_ahps = False, fim_1_ms = False):

    '''
    Creates plots and summary statistics using metrics compiled from 
    synthesize_test_cases. Required inputs are metrics_csv and workspace. 
    Outputs include:
        aggregate_<benchmark source>_<configuration>.csv: this csv 
            contains the aggregated total statistics (i.e. CSI, FAR, POD)
            using the summed area_sq_km fields
        <benchmark source>_<configuration>_common_sites.csv: this csv 
            contains the unique sites (e.g usgs/nws: nws_lid; ble: huc08) 
            considered for aggregation/plots for each magnitude. The selected
            sites occur in all versions analyzed. For example, if FIM 1,
            FIM 2, FIM 3.0.0.3 were versions analyzed, the common sites 
            would be those that had data for ALL versions. This 
            analysis is then redone for each magnitude. As such, the number
            of sites may vary with magnitude. The number of sites for each
            magnitude is annotated on generated plots.
        <benchmark source>_<configuration>_analyzed_data.csv: this is the 
            dataset used to create plots and aggregate statistics. It is 
            a subset of the input metrics file and consists of the common
            sites.
        csi_aggr_<benchmark source>_<configuration>.png: bar plot of the 
            aggregated CSI scores. Number of common sites is annotated
            (see list of sites listed in *_*_common_sites.csv).
        csi_<benchmark source>_<configuration>.png: box plot of CSI scores 
            (sites weighted equally). Number of common sites is annotated 
            (see list of sites listed in *_*_common_sites.csv).
        far_<benchmark source>_<configuration>*.png: box plot of FAR scores
            (sites weighted equally). Number of common sites is annotated 
            (see list of sites listed in *_*_common_sites.csv).
        tpr_<benchmark source>_<configuration>*.png: box plot of TPR/POD 
            scores (sites weighted equally). Number of common sites is 
            annotated (see list of sites listed in *_*_common_sites.csv).
        csi_scatter_<magnitude>_<configuration>*.png: scatter plot comparing 
            two versions for a given magnitude. This is only generated if
            there are exactly two versions analyzed.

    Parameters
    ----------
    metrics_csv : STRING
        Path to csv produced as part of synthesize_test_cases containing
        all metrics across all versions.
    workspace : STRING
        Path to the output workspace. Subdirectories will be created 
        reflecting the evaluation datasets.
    versions: LIST
        A list of versions to be aggregated/plotted. Uses the "startswith" 
        approach. Versions should be supplied in the order they are to 
        be plotted. For example: ['fim_', 'fb']; This will evaluate all 
        versions that start with fim_ (e.g. fim_1, fim_2, fim_3) and any
        feature branch that starts with "fb". To esbalish version order,
        the fim versions are naturally sorted and then fb versions 
        (naturally sorted) are appended. These versions are also used to 
        filter the input metric csv as only these versions are retained 
        for analysis. 
    stats: LIST
        A list of statistics to be plotted. Must be identical to column 
        field in metrics_csv. CSI, POD, TPR are currently calculated, if 
        additional statistics are desired formulas would need to be coded.
    alternate_ahps_query : STRING, optional
        The default is false. Currently the default ahps query is same 
        as done for apg goals. If a different query is desired it can be 
        supplied and it will supercede the default query. 
    spatial_ahps : DICTIONARY, optional
        The default is false. A dictionary with keys as follows: 
            'static': Path to AHPS point file created during creation of
                FIM 3 static libraries.
            'evaluated': Path to extent file created during the creation
                of the NWS/USGS AHPS preprocessing.
            'metadata': Path to previously created file that contains 
                metadata about each site (feature_id, wfo, rfc and etc).
        No spatial layers will be created if set to False, if a dictionary
        is supplied then a spatial layer is produced.
    fim_1_ms: BOOL
        Default is false. If True then fim_1 rows are duplicated with 
        extent_config set to MS. This allows for FIM 1 to be included 
        in MS plots/stats (helpful for nws/usgs ahps comparisons).

    Returns
    -------
    all_datasets : DICT
        Dictionary containing all datasets generated. 
        Keys: (benchmark_source, extent_config), 
        Values: (filtered dataframe, common sites)

    '''
    
    #Import metrics csv as DataFrame and initialize all_datasets dictionary
    csv_df = pd.read_csv(metrics_csv)

    #fim_1_ms flag enables FIM 1 to be shown on MS plots/stats
    if fim_1_ms:
        #Query FIM 1 rows based on version beginning with "fim_1"
        fim_1_rows = csv_df.query('version.str.startswith("fim_1")').copy()
        #Set extent configuration to MS (instead of FR)
        fim_1_rows['extent_config'] = 'MS'
        #Append duplicate FIM 1 rows to original dataframe
        csv_df = pd.concat([csv_df, fim_1_rows], ignore_index = True)
        
    #If versions are supplied then filter out    
    if versions:
        #Filter out versions based on supplied version list
        metrics = csv_df.query('version.str.startswith(tuple(@versions))')
    else:
        metrics = csv_df
       
    #Group by benchmark source
    benchmark_by_source = metrics.groupby(['benchmark_source', 'extent_config'])

    #Iterate through benchmark_by_source. Pre-filter metrics dataframe 
    #as needed (e.g. usgs/nws filter query). Then further filtering to 
    #discard all hucs/nws_lid that are not present across all analyzed 
    #versions for a given magnitude. The final filtered dataset is written 
    #to a dictionary with the key (benchmark source, extent config) 
    #and values (filtered dataframe, common sites).
    all_datasets = {}
    for (benchmark_source, extent_configuration), benchmark_metrics in benchmark_by_source:        
                
        #If source is usgs/nws define the base resolution and query 
        #(use alternate query if passed). Append filtered datasets to 
        #all_datasets dictionary.
        if benchmark_source in ['usgs','nws']:
            
            #Set the base processing unit for the ahps runs.
            base_resolution = 'nws_lid'
            
            #Default query (used for APG) it could be that bad_sites should be modified. If so pass an alternate query using the "alternate_ahps_query"
            bad_sites = ['grfi2','ksdm7','hohn4','rwdn4']
            query = "not flow.isnull() & masked_perc<97 & not nws_lid in @bad_sites"

            #If alternate ahps evaluation query argument is passed, use that.
            if alternate_ahps_query:
                query = alternate_ahps_query

            #Filter the dataset based on query
            ahps_metrics = benchmark_metrics.query(query)
            
            #Filter out all instances where the base_resolution doesn't 
            #exist across all desired fim versions for a given magnitude.
            all_datasets[(benchmark_source, extent_configuration)] = filter_dataframe(ahps_metrics, base_resolution)
                     
        #If source is 'ble', set base_resolution and append ble dataset 
        #to all_datasets dictionary
        elif benchmark_source == 'ble':
            
            #Set the base processing unit for ble runs
            base_resolution = 'huc'
            
            #Filter out all instances where base_resolution doesn't exist 
            #across all desired fim versions for a given magnitude.
            all_datasets[(benchmark_source, extent_configuration)] = filter_dataframe(benchmark_metrics, base_resolution)
            
    #For each dataset in all_datasets, generate plots and aggregate statistics.
    for (dataset_name,configuration), (dataset, sites) in all_datasets.items():
        
        #Define and create the output workspace as a subfolder within 
        #the supplied workspace
        output_workspace = Path(workspace) / dataset_name / configuration.lower()
        output_workspace.mkdir(parents = True, exist_ok = True)         
                
        #Write out the filtered dataset and common sites to file
        dataset.to_csv(output_workspace / (f'{dataset_name}_{configuration.lower()}_analyzed_data.csv'), index = False)
        sites_pd = pd.DataFrame.from_dict(sites, orient = 'index').transpose()
        sites_pd.to_csv(output_workspace / (f'{dataset_name}_{configuration.lower()}_common_sites.csv'), index = False)
        
        #set the order of the magnitudes and define base resolution.
        if dataset_name == 'ble':
            magnitude_order = ['100yr', '500yr']
            base_resolution = 'huc'
        elif dataset_name in ['usgs','nws']:
            magnitude_order = ['action','minor','moderate','major']
            base_resolution = 'nws_lid'

        #Calculate aggregated metrics based on total_sq_km fields.
        dataset_sums = dataset.groupby(['version', 'magnitude'])[['TP_area_km2','FP_area_km2','FN_area_km2']].sum()
        dataset_sums['csi'] = dataset_sums['TP_area_km2']/(dataset_sums['TP_area_km2'] + dataset_sums['FP_area_km2'] + dataset_sums['FN_area_km2'])
        dataset_sums['far'] = dataset_sums['FP_area_km2']/(dataset_sums['TP_area_km2'] + dataset_sums['FP_area_km2'])
        dataset_sums['pod'] = dataset_sums['TP_area_km2']/(dataset_sums['TP_area_km2'] + dataset_sums['FN_area_km2'])
        dataset_sums = dataset_sums.reset_index()
        
        #Write aggregated metrics to file.
        dataset_sums.to_csv(output_workspace / f'aggregate_{dataset_name}_{configuration.lower()}.csv', index = False )

        #This section naturally orders analyzed versions which defines 
        #the hue order for the generated plots.
        #Get all versions in dataset
        all_versions = list(dataset.version.unique())        
        version_order = []        
        #If versions are not specified then use all available versions 
        #and assign to versions_list
        if not versions:
            versions_list = all_versions
        #if versions are supplied assign to versions_list
        else:
            versions_list = versions        
        #For each version supplied by the user
        for version in versions_list:
            #Select all the versions that start with the supplied version.
            selected_versions = [sel_version for sel_version in all_versions if sel_version.startswith(version)]
            #Naturally sort selected_versions
            selected_versions = natsorted(selected_versions)
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
        aggregate_file = output_workspace / (f'csi_aggr_{dataset_name}_{configuration.lower()}.png')        
        barplot(dataframe = dataset_sums, x_field = 'magnitude', x_order = magnitude_order, y_field = 'csi', hue_field = 'version', ordered_hue = version_order, title_text = f'Aggregate {dataset_name.upper()} FIM Scores', fim_configuration = configuration, textbox_str = textbox, simplify_legend = True, dest_file = aggregate_file)
        
        #Create box plots for each metric in supplied stats.
        for stat in stats:
            output_file = output_workspace / (f'{stat.lower()}_{dataset_name}_{configuration.lower()}.png')    
            boxplot(dataframe = dataset, x_field = 'magnitude', x_order = magnitude_order, y_field = stat, hue_field = 'version', ordered_hue = version_order, title_text = f'{dataset_name.upper()} FIM Sites', fim_configuration = configuration, textbox_str = textbox, simplify_legend = True, dest_file = output_file)
        
        #Get the last 2 versions from the version order for scatter plot.
        if len(version_order) == 2:            
            x_version, y_version = version_order
            for magnitude in magnitude_order:
                #Scatterplot comparison between last 2 versions.
                x_csi = dataset.query(f'version == "{x_version}" & magnitude == "{magnitude}"')[[base_resolution, 'CSI']]
                y_csi = dataset.query(f'version == "{y_version}" & magnitude == "{magnitude}"')[[base_resolution, 'CSI']]
                plotdf = pd.merge(x_csi, y_csi, on = base_resolution, suffixes = (f"_{x_version}",f"_{y_version}"))
                #Define arguments for scatterplot function.
                title_text = f'CSI {magnitude}'
                dest_file = output_workspace / f'csi_scatter_{magnitude}_{configuration.lower()}.png'
                scatterplot(dataframe = plotdf, x_field = f'CSI_{x_version}', y_field = f'CSI_{y_version}', title_text = title_text, annotate = False, dest_file = dest_file)
    

    #######################################################################
    #Create spatial layers with threshold and mapping information
    ########################################################################
    if spatial_ahps:

        #Read in supplied shapefile layers
        #Layer containing metadata for each site (feature_id, wfo, etc). 
        #Convert nws_lid to lower case.
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
        
        #Join dataset metrics to evaluated_ahps_extent data. 
        final_join = pd.DataFrame()
        for (dataset_name, configuration), (dataset, sites) in all_datasets.items():
            #Only select ahps from dataset if config is MS
            if dataset_name in ['usgs','nws'] and configuration == 'MS':
                #Select records from evaluated_ahps_extent that match the dataset name
                subset = evaluated_ahps_extent.query(f'source == "{dataset_name}"')                        
                #Join to dataset
                dataset_with_subset = dataset.merge(subset, on = 'nws_lid')
                #Append rows to final_join dataframe
                final_join = pd.concat([final_join, dataset_with_subset])
        
        #Modify version field
        final_join['version'] = final_join.version.str.split('_nws|_usgs').str[0]
        
        #Write geodataframe to file
        gdf = gpd.GeoDataFrame(final_join, geometry = final_join['geometry'], crs = metadata_crs)
        output_shapefile = Path(workspace) / 'nws_usgs_site_info.shp'
        gdf.to_file(output_shapefile) 
                    


#######################################################################
if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Plot and aggregate statistics for benchmark datasets (BLE/AHPS libraries)')
    parser.add_argument('-m','--metrics_csv', help = 'Metrics csv created from synthesize test cases.', required = True)
    parser.add_argument('-w', '--workspace', help = 'Output workspace', required = True)
    parser.add_argument('-v', '--versions', help = 'List of versions to be plotted/aggregated. Versions are filtered using the "startswith" approach. For example, ["fim_","fb1"] would retain all versions that began with "fim_" (e.g. fim_1..., fim_2..., fim_3...) as well as any feature branch that began with "fb". An other example ["fim_3","fb"] would result in all fim_3 versions being plotted along with the fb.', nargs = '+', default = [])
    parser.add_argument('-s', '--stats', help = 'List of statistics (abbrev to 3 letters) to be plotted/aggregated', nargs = '+', default = ['CSI','TPR','FAR'], required = False)
    parser.add_argument('-q', '--alternate_ahps_query',help = 'Alternate filter query for AHPS. Default is: "not nws_lid.isnull() & not flow.isnull() & masked_perc<97 & not nws_lid in @bad_sites" where bad_sites are (grfi2,ksdm7,hohn4,rwdn4)', default = False, required = False)
    parser.add_argument('-sp', '--spatial_ahps', help = 'If spatial point layer is desired, supply a csv with 3 lines of the following format: metadata, path/to/metadata/shapefile\nevaluated, path/to/evaluated/shapefile\nstatic, path/to/static/shapefile.', default = False, required = False)
    parser.add_argument('-f', '--fim_1_ms', help = 'If enabled fim_1 rows will be duplicated and extent config assigned "ms" so that fim_1 can be shown on mainstems plots/stats', action = 'store_true', required = False)
    
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
        #Check that all required keys are present and overwrite args with spatial_dict
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
    f = args['fim_1_ms']

    #Run eval_plots function
    if not error:        
        eval_plots(metrics_csv = m, workspace = w, versions = v, stats = s, alternate_ahps_query = q, spatial_ahps = sp, fim_1_ms = f)