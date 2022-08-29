#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import argparse
from foss_fim.tools.consolidate_metrics import Consolidate_metrics
from matplotlib.ticker import FixedLocator, FixedFormatter, NullFormatter, FormatStrFormatter
from statsmodels.robust.robust_linear_model import RLM
#from foss_fim.tools.gms_tools.plots import preparing_data_for_plotting


def Make_results_plot( benchmarks=['all'],versions=['all'],
                       zones=['total_area'],matching_hucs_only=True,
                       hucs_of_interest=None,
                       metrics_output_csv=None,
                       impute_missing_ms=False,
                       output_fig=None,
                       model=None,
                       nhd_v_3dep=False,
                       quiet=False
                     ):

    consolidated_metrics_df,consolidated_secondary_metrics = Consolidate_metrics( 
                         benchmarks,
                         versions, 
                         zones,
                         matching_hucs_only,
                         hucs_of_interest,
                         metrics_output_csv,
                         impute_missing_ms,
                         quiet=True
                        )

    consolidated_metrics_df = __preparing_data_for_plotting(consolidated_metrics_df,nhd_v_3dep)
    violin_plots(consolidated_metrics_df,nhd_v_3dep,model=model,output_fig=output_fig)


def __preparing_data_for_plotting(metrics_table,nhd_v_3dep):

    metrics_table.rename(columns= {'extent_config':'Model',
                                   'magnitude': 'Magnitude',
                                   'TPR' : 'POD',
                                   'huc' : 'HUC'
                                  },
                         inplace=True)

    set_mannings = lambda df : 0.06 if 'n_6' in df['version'] else 0.12

    metrics_table['Mannings N'] = metrics_table.apply(set_mannings,axis=1)

    
    def __set_resolution(metrics_table):
        
        if '10m' in metrics_table['version']:
            resolution = 10
        elif '15m' in metrics_table['version']:
            resolution = 15
        elif '5m' in metrics_table['version']:
            resolution = 5
        elif '20m' in metrics_table['version']:
            resolution = 20
        else:
            resolution = 10


        return(resolution)
    
    metrics_table['DEM Resolution (m)'] = metrics_table.apply(__set_resolution,axis=1)
    
    
    def __set_dem_source(metrics_table):
        
        if '3dep' in metrics_table['version']:
            source = '3DEP'
        else:
            source = 'NHD'

        return(source)
    
    metrics_table['DEM Source'] = metrics_table.apply(__set_dem_source, axis=1)

    metrics_table = pd.melt(metrics_table, id_vars=('HUC','Model','Magnitude','Mannings N', 'DEM Resolution (m)','DEM Source'),
                            value_vars=('CSI','POD','FAR'),
                            var_name='Metric',
                            value_name='Metric Value'
                            )

    def __set_source_resolution_combo(metrics_table):
        
        return(str(metrics_table['DEM Source']) + ' - ' +str(metrics_table['DEM Resolution (m)']) ) + 'm'

    metrics_table.loc[:,'DEM Label'] = metrics_table.apply(__set_source_resolution_combo,axis=1)
    
    dem_label_to_value_dict = {'NHD - 10m' : -1, '3DEP - 1m': 0,
                               '3DEP - 5m': 1,
                               '3DEP - 10m': 2,
                               '3DEP - 15m': 3,
                               '3DEP - 20m': 4 }

    if nhd_v_3dep:
        dem_label_to_value_dict = {'NHD - 10m' : 0, '3DEP - 10m': 1,
                                   '3DEP - 5m': -1,
                                   '3DEP - 1m': -1,
                                   '3DEP - 15m': -1,
                                   '3DEP - 20m': -1 }

    set_dem_label_integer_encodings = lambda df : dem_label_to_value_dict[ df['DEM Label'] ]

    metrics_table['dem_label_integer_encodings'] = metrics_table.apply(set_dem_label_integer_encodings, axis=1)

    
    return(metrics_table)


def violin_plots(metrics_table,nhd_v_3dep=False,model=None,output_fig=None):

    if isinstance(metrics_table,pd.DataFrame):
        pass
    elif isinstance(metrics_table,str):
        metrics_table = pd.read_csv(metrics_table)
    elif isinstance(metrics_table,list):
        metrics_table = pd.concat([pd.read_csv(mt) for mt in metrics_table],ignore_index=True)
    else: 
        ValueError("Pass metrics_table as DataFrame or path to CSV")

    #metrics_table = preparing_data_for_plotting(metrics_table)
    
    if (model is None) | (model == 'FR'):
        range_by_row = ( (0.45,0.70,0.05),
                         (0.55,0.85,0.05),
                         (0.05,0.40,0.05)
                       )
    if (model is None) | (model == 'MS'):
        range_by_row = ( (0.45,0.70,0.05),
                         (0.55,0.85,0.05),
                         (0.05,0.40,0.05)
                       )
    if (model is None) | (model == 'GMS'):
        range_by_row = ( (0.45,0.70,0.05),
                         (0.55,0.85,0.05),
                         (0.05,0.40,0.05)
                       )
    
    if model is not None:
        metrics_table = metrics_table.loc[metrics_table.loc[:,'Model'] == model,:]
    #metrics_table = metrics_table.loc[metrics_table.loc[:,'Model'] == model,:]

    if nhd_v_3dep:
        metrics_table = metrics_table.loc[metrics_table.loc[:,'DEM Resolution (m)'] == 10,:]
        order=['NHD - 10m','3DEP - 10m']
    else:
        order=['3DEP - 5m','3DEP - 10m','3DEP - 15m','3DEP - 20m']

    metrics_table.reset_index(drop=True,inplace=True)
    
    facetgrid = sns.catplot( 
                    data=metrics_table,x='DEM Label',y='Metric Value',
                    hue='Magnitude',inner='quartile',split=True,
                    hue_order=['100yr','500yr'],
                    order=order,
                    cut=2,
                    kind='violin',
                    #col='Mannings N',
                    row='Metric',
                    margin_titles=False,
                    sharex=False,
                    sharey=False,
                    despine=True,
                    scale='width',
                    height=3,
                    aspect=2,
                    linewidth=1.5,
                    legend=False,
                    saturation=0.55
                  )
    
    facetgrid.map_dataframe( sns.regplot,
                             data=metrics_table.loc[metrics_table.loc[:,'DEM Source'] == '3DEP',:],
                             x='dem_label_integer_encodings',
                             y='Metric Value',
                             scatter=False,
                             ci=None,
                             robust=True,
                             color= 'darkgreen',
                             truncate=False,
                             line_kws= {'linewidth' : 2},
                             label='Trend Line'
                           )
    
    #facetgrid.fig.set_size_inches(10,15)

    # legend
    handles, labels = facetgrid.axes[0,0].get_legend_handles_labels()
    facetgrid.fig.legend(loc='lower left',ncol=3,bbox_to_anchor=(0.25,-0.09),handles=handles,
                         title='Magnitude',
                         labels=labels)

    # xlabel
    plt.annotate('DEM Source - Resolution',xy=(0.39,0.08),xycoords='figure fraction')

    # override axes params
    facetgrid = set_axes(facetgrid, range_by_row, model)

    facetgrid.fig.subplots_adjust( wspace=0,
                                   hspace=0.1,
                                 )
    
    # set margins to tight
    #plt.tight_layout()
    
    # set rlm metrics
    metrics_table = robust_linear_model(metrics_table)
    facetgrid = annotate_rlm(facetgrid,metrics_table)

    # set text sizes
    facetgrid = set_text_sizes(facetgrid)

    if output_fig is not None:
        plt.savefig(output_fig,dpi=300,format='jpg',bbox_inches='tight')

    plt.show()


def robust_linear_model(metrics_table):

    # REMOVE ALL BUT 3DEP DATA

    metrics = metrics_table.loc[:,'Metric'].unique()
    #mannings = metrics_table.loc[:,'Mannings N'].unique()

    metric_indices = { m : metrics_table.loc[:,'Metric'] == m for m in metrics }
    #mannings_indices = { m : metrics_table.loc[:,'Mannings N'] == m for m in mannings }

    #metrics_table.set_index(['Metric','Mannings N'],inplace=True,drop=False)
    metrics_table.set_index(['Metric'],inplace=True,drop=False)
    metrics_table.sort_index(inplace=True)
    
    metrics_table.loc[:,'beta1'] = None
    metrics_table.loc[:,'beta1_pvalue'] = None
    
    #for met,man in product(metrics,mannings):
    for met in metrics:
        
        y = metrics_table.loc[met,'Metric Value'].to_numpy()

        X = metrics_table.loc[met,'DEM Resolution (m)'].to_numpy()

        X = np.stack((np.ones(X.shape),X),axis=1)
        
        model = RLM(y,X)
        results = model.fit()

        beta1 = results.params[1]
        pval_beta1=results.pvalues[1]/2 # two tailed to one tail

        metrics_table.loc[met,'beta1'] = beta1
        metrics_table.loc[met,'beta1_pvalue'] = pval_beta1
    
    metrics_table.reset_index(drop=True,inplace=True)
    
    return(metrics_table)


def annotate_rlm(facetgrid,metrics_table):

    metrics_table.set_index(['Metric'],inplace=True,drop=False)
    metrics_table.sort_index(inplace=True)

    metric_index_dict = {'CSI':0,'POD':1,'FAR':2}
    mannings_index_dict = {0.06:0 , 0.12:1}
    
    for met in metrics_table.index.unique():

        rowIdx = metric_index_dict[met]
        #colIdx = mannings_index_dict[man]
        colIdx = 0

        beta1 = metrics_table.loc[met,'beta1'].unique()[0]
        beta1_pvalue = metrics_table.loc[met,'beta1_pvalue'].unique()[0]

        facetgrid.axes[rowIdx,colIdx].annotate( r'$\beta_1$  =  {:.4f}  |  p-value  =  {:.3f}'.format(beta1,beta1_pvalue),
                                                xy=(0.24,1.025),
                                                xycoords='axes fraction',
                                                color='darkgreen')
    
    return(facetgrid)
    

def set_axes(facetgrid, range_by_row, model):

    nrows,ncols = facetgrid.axes.shape

    title_dict = {'FR' : 'Full Resolution (FR)', 'MS' : 'Mainstems (MS)', 'GMS' : 'Generalized Mainstems (GMS)' }
    title = title_dict[model]

    for rowIdx in range(nrows):
        
        all_ticks = np.arange(*range_by_row[rowIdx])
        major_ticks = all_ticks[1:]
        #minor_ticks = (all_ticks[0] , all_ticks[-1]+range_by_row[rowIdx][-1])
        minor_ticks = np.arange( range_by_row[rowIdx][0],
                                 range_by_row[rowIdx][1]+range_by_row[rowIdx][2],
                                 range_by_row[rowIdx][2]*2
                               )
        major_ticks = np.arange( range_by_row[rowIdx][0]+range_by_row[rowIdx][2],
                                 range_by_row[rowIdx][1],
                                 range_by_row[rowIdx][2]*2
                               )
        
        
        for colIdx in range(ncols):
            
            facetgrid.axes[rowIdx,colIdx].set_ylim(range_by_row[rowIdx][:-1])
            facetgrid.axes[rowIdx,colIdx].yaxis.set_major_locator(FixedLocator(major_ticks))
            facetgrid.axes[rowIdx,colIdx].yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
            facetgrid.axes[rowIdx,colIdx].yaxis.set_minor_locator(FixedLocator(minor_ticks))
            facetgrid.axes[rowIdx,colIdx].yaxis.set_minor_formatter(NullFormatter())
            facetgrid.axes[rowIdx,colIdx].tick_params(labelrotation=45,axis='y')
            
            # set axis titles
            current_title = facetgrid.axes[rowIdx,colIdx].get_title()
            facetgrid.axes[rowIdx,colIdx].set_title('')
            values = [val.strip() for val in current_title.split('|')]
            metric = values[0][-3:]
            #print(values);exit()
            #mannings = values[1][-4:]

            # axis border
            facetgrid.axes[rowIdx,colIdx].axhline(range_by_row[rowIdx][1],color='black')
            
            if colIdx == 1:
                facetgrid.axes[rowIdx,colIdx].axvline(x=facetgrid.axes[rowIdx,colIdx].get_xlim()[1],color='black')

            if rowIdx == 0:
                facetgrid.axes[rowIdx,colIdx].set_title(title,pad=25)
            #    facetgrid.axes[rowIdx,colIdx].set_title(f'Manning\'s N = {mannings}',pad=25)

            if colIdx == 0:
                facetgrid.axes[rowIdx,colIdx].set_ylabel(metric)

            # removes x ticks from everything but bottom row
            if rowIdx < (nrows - 1):
                facetgrid.axes[rowIdx,colIdx].set_xticks([])
        
        # remove ticks from 2nd column
        #facetgrid.axes[rowIdx,1].set(yticklabels=[]) 

    return(facetgrid)


def set_text_sizes(facetgrid):

    plt.rc('font',size=18)

    return(facetgrid)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Makes plots for DEM skill results')
    parser.add_argument('-b','--benchmarks',help='Allowed benchmarks', required=False, default='all', nargs="+")
    parser.add_argument('-u','--hucs-of-interest',help='HUC8s of interest', required=False, default='None', nargs="+")
    parser.add_argument('-v','--versions',help='Allowed versions', required=False, default='all', nargs="+")
    parser.add_argument('-z','--zones',help='Allowed zones', required=False, default='total_area', nargs="+")
    parser.add_argument('-o','--metrics-output-csv',help='File path to outputs csv', required=False, default=None)
    parser.add_argument('-f','--output-fig',help='Output Figure Filename', required=False, default=None)
    parser.add_argument('-m','--model',help='Model to use (FR, MS, GMS)', required=False, default=None)
    parser.add_argument('-n','--nhd-v-3dep',help='Make NHD vs 3DEP version of plot', required=False, default=False,action='store_true')
    parser.add_argument('-i','--impute-missing_ms',help='Imputes FR metrics in HUCS with no MS. Only supports one version per extent config', required=False, action='store_true',default=False)
    parser.add_argument('-q','--quiet',help='Quiet print output', required=False, action='store_true',default=False)

    args = vars(parser.parse_args())

    Make_results_plot(**args)
