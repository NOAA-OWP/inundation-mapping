#!/usr/bin/env python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
#########################################################################
#Create boxplot
#########################################################################
def boxplot(dataframe, x_field, x_order, y_field, hue_field, ordered_hue, title_text, fim_configuration, textbox_str = False, simplify_legend = False, dest_file = False):      
    '''
    Create boxplots. 

    Parameters
    ----------
    dataframe : DataFrame
        Pandas dataframe data to be plotted.
    x_field : STR
        Field to use for x-axis
    x_order : List
        Order to arrange the x-axis.
    y_field : STR
        Field to use for the y-axis
    hue_field : STR
        Field to use for hue (typically FIM version)
    title_text : STR
        Text for plot title.
    fim_configuration: STR
        Configuration of FIM (FR or MS or Composite).
    simplify_legend : BOOL, optional
        If True, it will simplify legend to FIM 1, FIM 2, FIM 3. 
        The default is False.
    dest_file : STR or BOOL, optional
        If STR provide the full path to the figure to be saved. If False 
        no plot is saved to disk. The default is False.

    Returns
    -------
    fig : MATPLOTLIB
        Plot.

    '''

    #initialize plot
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(15, 10))
    #Use seaborn to plot the boxplot
    axes=sns.boxplot(x=x_field, y=y_field, order=x_order, hue=hue_field, hue_order = ordered_hue, data=dataframe, palette='bright')
    #set title of plot
    axes.set_title(f'{title_text} ({y_field})',fontsize=20, weight = 'bold')
    #Set yticks and background horizontal line.
    axes.set(ylim=(0.0,1.0),yticks = np.arange(0,1.1,0.1))
    for index,ytick in enumerate(axes.get_yticks()):
        plt.axhline(y=ytick,color='black',linestyle = '--',linewidth = 1,alpha = 0.1)
    #Define y axis label and x axis label.
    axes.set_ylabel(f'{y_field}',fontsize='xx-large',weight = 'bold')
    axes.set_xlabel('',fontsize=0,weight = 'bold')
    #Set sizes of ticks and legend.
    axes.tick_params(labelsize = 'xx-large')
    axes.legend(markerscale = 2, fontsize =20, loc = 'lower left')
    
    #If simple legend desired
    if simplify_legend:
        #trim labels to FIM 1, FIM 2, and the FIM 3 version    
        handles, org_labels = axes.get_legend_handles_labels()
        label_dict = {}
        for label in org_labels:
            if 'fim_1' in label:
                label_dict[label] = 'FIM 1'
            elif 'fim_2' in label:
                label_dict[label] = 'FIM 2' + ' ' + fim_configuration.lower()                  
            elif 'fim_3' in label:
                label_dict[label] = re.split('_fr|_ms', label)[0].replace('_','.').replace('fim.','FIM ') + ' ' + fim_configuration.lower()
                if label.endswith('_c'):
                    label_dict[label] = label_dict[label] + ' c'
            else:
                label_dict[label] = label + ' ' + fim_configuration.lower()
        #Define simplified labels as a list.
        new_labels = [label_dict[label] for label in org_labels]
        #Define legend location. FAR needs to be in different location than CSI/POD.
        if y_field == 'FAR':
            legend_location = 'upper right'
        else:
            legend_location = 'lower left' 
        #rename legend labels to the simplified labels.
        axes.legend(handles, new_labels, markerscale = 2, fontsize = 20, loc = legend_location, ncol = int(np.ceil(len(new_labels)/7)))
    #Print textbox if supplied
    if textbox_str:
        box_props = dict(boxstyle='round', facecolor='white', alpha=0.5)
        axes.text(0.01, 0.99, textbox_str, transform=axes.transAxes, fontsize=14, verticalalignment='top', bbox=box_props)

    #If figure to be saved to disk, then do so, otherwise return figure
    if dest_file:
        fig.savefig(dest_file)
        plt.close(fig)
    else:
        return fig

#########################################################################
#Create scatter plot
#########################################################################
def scatterplot(dataframe, x_field, y_field, title_text, stats_text=False, annotate = False, dest_file = False):      
    '''
    Create boxplots. 

    Parameters
    ----------
    dataframe : DataFrame
        Pandas dataframe data to be plotted.
    x_field : STR
        Field to use for x-axis (Assumes FIM 2)
    y_field : STR
        Field to use for the y-axis (Assumes FIM 3)
    title_text : STR
        Text for plot title.    
    stats_text : STR or BOOL
        Text for stats to place on chart. Default is false (no stats printed)
    dest_file : STR or BOOL, optional
        If STR provide the full path to the figure to be saved. If False 
        no plot is saved to disk. The default is False.

    Returnsy
    -------
    fig : MATPLOTLIB
        Plot.

    '''

    #initialize plot
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(15, 10))
    
    #Use seaborn to plot the boxplot
    axes=sns.scatterplot(data=dataframe, x=x_field, y=y_field, color = 'black', s = 150)
    
    #Set xticks and yticks and background horizontal line.
    axes.set(ylim=(0.0,1.0),yticks = np.arange(0,1.1,0.1))
    axes.set(xlim=(0.0,1.0),xticks = np.arange(0,1.1,0.1))
    axes.grid(b=True, which='major', axis='both')    
    
    #Set sizes of ticks and legend.
    axes.tick_params(labelsize = 'xx-large')

    #Define y axis label and x axis label.
    axes.set_ylabel(f'{y_field.replace("_"," ")}',fontsize='xx-large',weight = 'bold')
    axes.set_xlabel(f'{x_field.replace("_"," ")}',fontsize='xx-large',weight = 'bold')

    #Plot diagonal line
    diag_range = [0,1]
    axes.plot(diag_range, diag_range, color='gray', transform=axes.transAxes)


    #set title of plot
    axes.set_title(f'{title_text}',fontsize=20, weight = 'bold')
   
    if annotate:
        #Set text for labels
        box_props = dict(boxstyle='round', facecolor='white', alpha=0.5)
        textbox_str = 'Target Better'
        axes.text(0.3, 0.6, textbox_str, transform=axes.transAxes, fontsize=32, color = 'gray', fontweight = 'bold', verticalalignment='top', bbox=box_props, rotation = 35, rotation_mode = 'anchor')
        textbox_str = 'Baseline Better'
        axes.text(0.5, 0.2, textbox_str, transform=axes.transAxes, fontsize=32, color = 'gray', fontweight = 'bold', verticalalignment='top', bbox=box_props, rotation = 35, rotation_mode = 'anchor')

    if stats_text:
        #Add statistics textbox
        axes.text(0.01, 0.80, stats_text, transform=axes.transAxes, fontsize=24, verticalalignment='top', bbox=box_props)

    #If figure to be saved to disk, then do so, otherwise return fig
    if dest_file:
        fig.savefig(dest_file)
        plt.close(fig)
    else:
        return fig
#########################################################################
#Create barplot
#########################################################################
def barplot(dataframe, x_field, x_order, y_field, hue_field, ordered_hue, title_text, fim_configuration, textbox_str = False, simplify_legend = False, display_values = False, dest_file = False):      
    '''
    Create barplots. 

    Parameters
    ----------
    dataframe : DataFrame
        Pandas dataframe data to be plotted.
    x_field : STR
        Field to use for x-axis
    x_order : List
        Order to arrange the x-axis.
    y_field : STR
        Field to use for the y-axis
    hue_field : STR
        Field to use for hue (typically FIM version)
    title_text : STR
        Text for plot title.
    fim_configuration: STR
        Configuration of FIM (FR or MS or Composite).
    simplify_legend : BOOL, optional
        If True, it will simplify legend to FIM 1, FIM 2, FIM 3. 
        Default is False.
    display_values : BOOL, optional
        If True, Y values will be displayed above bars. 
        Default is False.
    dest_file : STR or BOOL, optional
        If STR provide the full path to the figure to be saved. If False 
        no plot is saved to disk. Default is False.

    Returns
    -------
    fig : MATPLOTLIB
        Plot.

    '''

    #initialize plot
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(15, 10))
    #Use seaborn to plot the boxplot
    axes=sns.barplot(x=x_field, y=y_field, order=x_order, hue=hue_field, hue_order = ordered_hue, data=dataframe, palette='bright')
    #set title of plot
    axes.set_title(f'{title_text}',fontsize=20, weight = 'bold')
    #Set yticks and background horizontal line.
    axes.set(ylim=(0.0,1.0),yticks = np.arange(0,1.1,0.1))
    for index,ytick in enumerate(axes.get_yticks()):
        plt.axhline(y=ytick,color='black',linestyle = '--',linewidth = 1,alpha = 0.1)
    #Define y axis label and x axis label.
    axes.set_ylabel(f'{y_field.upper()}',fontsize='xx-large',weight = 'bold')
    axes.set_xlabel('',fontsize=0,weight = 'bold')
    #Set sizes of ticks and legend.
    axes.tick_params(labelsize = 'xx-large')
    axes.legend(markerscale = 2, fontsize =20, loc = 'upper right')
    #If simple legend desired
    if simplify_legend:
        #trim labels to FIM 1, FIM 2, FIM 3    
        handles, org_labels = axes.get_legend_handles_labels()
        label_dict = {}
        for label in org_labels:
            if 'fim_1' in label:
                label_dict[label] = 'FIM 1'
            elif 'fim_2' in label:
                label_dict[label] = 'FIM 2' + ' ' + fim_configuration.lower()
            elif 'fim_3' in label:
                label_dict[label] = re.split('_fr|_ms', label)[0].replace('_','.').replace('fim.','FIM ') + ' ' + fim_configuration.lower()
                if label.endswith('_c'):
                    label_dict[label] = label_dict[label] + ' c'
            else:
                label_dict[label] = label + ' ' + fim_configuration.lower()
        #Define simplified labels as a list.
        new_labels = [label_dict[label] for label in org_labels]
        #rename legend labels to the simplified labels.
        axes.legend(handles, new_labels, markerscale = 2, fontsize = 20, loc = 'upper right', ncol = int(np.ceil(len(new_labels)/7)))
    #Add Textbox
    if textbox_str:
        box_props = dict(boxstyle='round', facecolor='white', alpha=0.5)
        axes.text(0.01, 0.99, textbox_str, transform=axes.transAxes, fontsize=18, verticalalignment='top', bbox=box_props)

    #Display Y values above bars
    if display_values:
        #Add values of bars directly above bar.
        for patch in axes.patches:
            value = round(patch.get_height(),3)
            axes.text(patch.get_x()+patch.get_width()/2.,
                    patch.get_height(),
                    '{:1.3f}'.format(value),
                    ha="center", fontsize=18)  
    
    #If figure to be saved to disk, then do so, otherwise return fig
    if dest_file:
        fig.savefig(dest_file)
        plt.close(fig)
    else:
        return fig
#######################################################################
#Filter dataframe generated from csv file from run_test_case aggregation
########################################################################
def filter_dataframe(dataframe, unique_field):
    '''

    This script will filter out the sites (or hucs) which are not consistently 
    found for all versions for a given magnitude. For example, an AHPS 
    lid site must have output for all 3 versions (fim1, fim2, fim3) for 
    a given magnitude (eg action) otherwise that lid is filtered out. 
    Likewise for a BLE a huc must have output for all 3 versions 
    (fim1, fim2, fim3) for a given magnitude (eg 100yr) otherwise it is 
    filtered out.

    Parameters
    ----------
    dataframe : Pandas DataFrame
        Containing the input metrics originating from synthesize_test_cases
    unique_field : STR
        base resolution for each benchmark source: 'nws'/'usgs' (nws_lid)
        ble (huc).

    Returns
    -------
    final_filtered_dataframe : Pandas Dataframe
        Filtered dataframe that contains only common sites (lids or hucs) between versions for each magnitude. For example, for AHPS all sites which were run for each version for a given magnitude will be kept or for ble, all hucs which ran for all versions for a given magnitude. 
    unique_sites: DICT
        The sites that were included in the dataframe for each magnitude.

    '''
    
    #Get lists of sites for each magnitude/version
    unique_sites = dataframe.groupby(['magnitude','version'])[unique_field].agg('unique')
    #Get unique magnitudes
    magnitudes = dataframe.magnitude.unique()
    #Create new dataframe to hold metrics for the common sites as well as the actual lists of common sites.
    final_filtered_dataframe = pd.DataFrame()
    all_unique_sites = {}
    #Cycle through each magnitude
    for magnitude in magnitudes:
        #Compile a list of sets containing unique lids pertaining to each threshold. List contains 3 unique sets [{fim1:unique lids},{fim2: unique lids},{fim3: unique lids}]
        sites_per_magnitude=[set(a) for a in unique_sites[magnitude]]
        #Intersect the sets to get the common lids per threshold then convert to list.
        common_sites_per_magnitude = list(set.intersection(*sites_per_magnitude))
        #Write common sites to dataframe
        all_unique_sites[magnitude] = common_sites_per_magnitude
        #Query filtered dataframe and only include data associated with the common sites for that magnitude
        filtered_common_sites = dataframe.query(f'magnitude == "{magnitude}" & {unique_field} in @common_sites_per_magnitude')
        #Append the data for each magnitude to a final dataframe that will contain data for all common sites for all magnitudes.
        final_filtered_dataframe = pd.concat([final_filtered_dataframe, filtered_common_sites], ignore_index = True)            
    
    return final_filtered_dataframe, all_unique_sites

