#!/usr/bin/env python3

import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, FixedFormatter, NullFormatter, FormatStrFormatter
import argparse
import pandas as pd
import numpy as np
from statsmodels.robust.robust_linear_model import RLM
from itertools import product



def gms_box_plots(metrics_table,output_fig=None):

    if isinstance(metrics_table,pd.DataFrame):
        pass
    elif isinstance(metrics_table,str):
        metrics_table = pd.read_csv(metrics_table)
    elif isinstance(metrics_table,list):
        metrics_table = pd.concat([pd.read_csv(mt) for mt in metrics_table],ignore_index=True)
    else: 
        ValueError("Pass metrics_table as DataFrame or path to CSV")

    metrics_table = preparing_data_for_plotting(metrics_table)
    
    facetgrid = sns.catplot( 
                    data=metrics_table,x='Model',y='Metric Value',
                    hue='Magnitude',inner='quartile',split=True,
                    order=['FR','MS','GMS'],
                    cut=2,
                    kind='violin',
                    col='Mannings N',
                    row='Metric',
                    margin_titles=False,
                    sharex=False,
                    sharey=False,
                    despine=True,
                    scale='width',
                    height=2.75,
                    aspect=1,
                    linewidth=1.5,
                    legend=False,
                    saturation=0.55
                  )
    
    facetgrid.map_dataframe( sns.regplot,
                             data=metrics_table,
                             x='model_integer_encodings',
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
    facetgrid.fig.legend(loc='lower left',ncol=3,bbox_to_anchor=(0.22,-0.06),handles=handles,labels=labels)

    # xlabel
    plt.annotate('Model',xy=(0.52,0.054),xycoords='figure fraction')

    # override axes params
    facetgrid = set_axes(facetgrid)

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
    

def set_axes(facetgrid):

    range_by_row = ( (0.45,0.75,0.05),
                     (0.50,0.90,0.05),
                     (0.00,0.40,0.05)
                   )
    
    nrows,ncols = facetgrid.axes.shape

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
            mannings = values[1][-4:]

            # axis border
            facetgrid.axes[rowIdx,colIdx].axhline(range_by_row[rowIdx][1],color='black')
            
            if colIdx == 1:
                facetgrid.axes[rowIdx,colIdx].axvline(x=facetgrid.axes[rowIdx,colIdx].get_xlim()[1],color='black')

            if rowIdx == 0:
                facetgrid.axes[rowIdx,colIdx].set_title(f'Manning\'s N = {mannings}',pad=25)

            if colIdx == 0:
                facetgrid.axes[rowIdx,colIdx].set_ylabel(metric)

            # removes x ticks from everything but bottom row
            if rowIdx < (nrows - 1):
                facetgrid.axes[rowIdx,colIdx].set_xticks([])
        
        # remove ticks from 2nd column
        facetgrid.axes[rowIdx,1].set(yticklabels=[]) 

    return(facetgrid)


def preparing_data_for_plotting(metrics_table):

    metrics_table.rename(columns= {'extent_config':'Model',
                                   'magnitude': 'Magnitude',
                                   'TPR' : 'POD'
                                  },
                         inplace=True)

    set_mannings = lambda df : 0.06 if 'n_6' in df['version'] else 0.12

    metrics_table['Mannings N'] = metrics_table.apply(set_mannings,axis=1)

    metrics_table = pd.melt(metrics_table, id_vars=('Model','Magnitude','Mannings N'),
                            value_vars=('CSI','POD','FAR'),
                            var_name='Metric',
                            value_name='Metric Value'
                            )

    model_to_value_dict = {'FR' : 0, 'MS': 1, 'GMS': 2}

    set_model_integer_encodings = lambda df : model_to_value_dict[ df['Model'] ]

    metrics_table['model_integer_encodings'] = metrics_table.apply(set_model_integer_encodings, axis=1)
    
    return(metrics_table)


def robust_linear_model(metrics_table):

    metrics = metrics_table.loc[:,'Metric'].unique()
    mannings = metrics_table.loc[:,'Mannings N'].unique()

    metric_indices = { m : metrics_table.loc[:,'Metric'] == m for m in metrics }
    mannings_indices = { m : metrics_table.loc[:,'Mannings N'] == m for m in mannings }

    metrics_table.set_index(['Metric','Mannings N'],inplace=True,drop=False)
    metrics_table.sort_index(inplace=True)
    for met,man in product(metrics,mannings):
        
        y = metrics_table.loc[(met,man),'Metric Value'].to_numpy()

        X = metrics_table.loc[(met,man),'model_integer_encodings'].to_numpy()

        X = np.stack((np.ones(X.shape),X),axis=1)
        
        model = RLM(y,X)
        results = model.fit()

        beta1 = results.params[1]
        pval_beta1=results.pvalues[1]/2 # two tailed to one tail

        metrics_table.loc[(met,man),'beta1'] = beta1
        metrics_table.loc[(met,man),'beta1_pvalue'] = pval_beta1
    
    metrics_table.reset_index(drop=True,inplace=True)

    return(metrics_table)


def annotate_rlm(facetgrid,metrics_table):

    metrics_table.set_index(['Metric','Mannings N'],inplace=True,drop=False)
    metrics_table.sort_index(inplace=True)

    metric_index_dict = {'CSI':0,'POD':1,'FAR':2}
    mannings_index_dict = {0.06:0 , 0.12:1}
    
    for met,man in metrics_table.index.unique():

        rowIdx = metric_index_dict[met]
        colIdx = mannings_index_dict[man]

        beta1 = metrics_table.loc[(met,man),'beta1'].unique()[0]
        beta1_pvalue = metrics_table.loc[(met,man),'beta1_pvalue'].unique()[0]

        facetgrid.axes[rowIdx,colIdx].annotate( r'$\beta_1$  =  {:.4f}  |  p-value  =  {:.3f}'.format(beta1,beta1_pvalue),
                                                xy=(0.01,1.025),
                                                xycoords='axes fraction',
                                                color='darkgreen')
    
    return(facetgrid)


def set_text_sizes(facetgrid):

    plt.rc('font',size=12)

    return(facetgrid)



if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument('-m','--metrics-table',help='Metrics table',required=True,nargs='+')
    parser.add_argument('-o','--output-fig',help='Output figure',required=False,default=None)

    args = vars(parser.parse_args())

    gms_box_plots(**args)
