#!/usr/bin/env python3

from shapely.geometry import box
import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import argparse
from datetime import datetime
from make_boxes_from_bounds import find_hucs_of_bounding_boxes
import requests
from concurrent.futures import ThreadPoolExecutor,as_completed
import os
from tqdm import tqdm
from foss_fim.tools.inundation import read_nwm_forecast_file


##################################
##
## Likely Deprecated: File appears to be no longer used. Noticed Jan 16, 2023
## Might want to be kept for possible re-use at a later time?
##
##################################


def cygnss_preprocessing(bounding_boxes_file,wbd=None,projection_of_boxes='EPSG:4329',wbd_layer='WBDHU8',forecast_output_file=None,retrieve=True,workers=6,download_directory=None, daily_mean_forecast_files=None):

    _, bounding_boxes = find_hucs_of_bounding_boxes(bounding_boxes_file,wbd=wbd,projection_of_boxes=projection_of_boxes,wbd_layer=wbd_layer)
    
    # load bounding box file
    bounding_boxes['event_date'] = pd.to_datetime(bounding_boxes['event_date'],utc=True)
    bounding_boxes.reset_index(drop=True,inplace=True)
    
    wbdcol_name = 'HUC'+wbd_layer[-1]

    # expand dates
    datetime_indices = bounding_boxes.apply(lambda df:pd.date_range(df['event_date'],periods=24,closed=None,freq='H',tz='UTC'),axis=1)

    datetime_indices.name = 'date_time'
    datetime_indices=pd.DataFrame(datetime_indices)
    datetime_indices = datetime_indices.join(bounding_boxes[['Name',wbdcol_name]])

    # append columns to expanded dates
    forecast_df = pd.DataFrame()
    for idx,row in datetime_indices.iterrows():
        dt_df = row['date_time'].to_frame(index=False,name='date_time')
        
        row = row.drop('date_time')

        dt_df = dt_df.join(pd.concat([pd.DataFrame(row).T]*len(dt_df),ignore_index=True))

        forecast_df = pd.concat((forecast_df,dt_df),ignore_index=True)

    # add extra dry date 
    #additional_date_df = forecast_df[['Name',wbdcol_name]].merge(bounding_boxes[['additional_date',wbdcol_name]],left_on=wbdcol_name,right_on=wbdcol_name).drop_duplicates(ignore_index=True)

    #forecast_df = pd.concat((forecast_df,additional_date_df.rename(columns={'additional_date':'date_time'})),ignore_index=True)

    forecast_df = forecast_df.sort_values(['Name',wbdcol_name],ignore_index=True)


    #forecast_df['date_time'] = forecast_df.apply(lambda df : df['date_time'].replace(hour=18,minute=0),axis=1)

    forecast_df = forecast_df.rename(columns={wbdcol_name:'huc'})

    forecast_df = construct_nwm_forecast_filenames_and_retrieve(forecast_df,download_directory,retrieve=retrieve,workers=workers)
   

    # take daily means
    def get_forecast(forecast_row):
        
        try:
            forecast_table = read_nwm_forecast_file(forecast_row['forecast_file'])
        except FileNotFoundError:
            print(f"Skipping file {forecast_row['forecast_file']}")
            return None

        return(forecast_table)

    # filter out 2021
    years = forecast_df['date_time'].dt.year < 2021
    forecast_df = forecast_df.loc[years,:]

    # unique dates and hourly samples
    unique_forecast_df = forecast_df.drop(columns='huc').drop_duplicates()
    unique_forecast_df.reset_index(inplace=True,drop=True)

    dates = unique_forecast_df["date_time"].map(lambda t: t.date())
    unique_dates, hourly_samples_per_date = np.unique(dates,return_counts=True)

    unique_dict = dict(zip(unique_dates,hourly_samples_per_date))
    
    final_forecast_df = forecast_df.groupby(pd.Grouper(key='date_time',freq='d')).first().dropna()
    final_forecast_df.set_index(pd.to_datetime(final_forecast_df.index.date,utc=True),drop=True,inplace=True)
    forecast_df_dates = forecast_df.copy()
    forecast_df_dates.date_time = forecast_df.date_time.dt.date
    final_forecast_df = final_forecast_df.merge(forecast_df,left_index=True, right_on='date_time')

    final_forecast_df.reset_index(drop=True,inplace=True)
    final_forecast_df['date_time'] = final_forecast_df.date_time.dt.date

    final_forecast_df.drop(columns={'huc_x','Name_x','forecast_file_x','forecast_url_x','forecast_url_y'},inplace=True)

    final_forecast_df.rename(columns={'Name_y':'Name','huc_y':'huc','forecast_file_y':'forecast_file'},inplace=True)
    
    def update_daily(daily_nwm_forecast_df,current_date,daily_mean_forecast_files,final_forecast_df):
        daily_mean = daily_nwm_forecast_df.mean(axis=1).rename('discharge')
        
        current_date_string = current_date.strftime(format='%Y%m%d')
        filename,ext = os.path.basename(daily_mean_forecast_files).split('.')
        outfile = os.path.join(os.path.dirname(daily_mean_forecast_files),filename+'_'+ current_date_string + '.'+ext)
        daily_mean.to_csv(outfile,index=True)
        
        final_forecast_df.loc[final_forecast_df['date_time'] == current_date,'forecast_file'] = outfile
        
        daily_nwm_forecast_df = None

        return(daily_nwm_forecast_df,current_date,daily_mean_forecast_files,final_forecast_df)


    daily_nwm_forecast_df = None
    ii = 0
    current_date_time = unique_forecast_df.loc[0,'date_time']
    current_date = current_date_time.date()
    for i,row in tqdm(unique_forecast_df.iterrows(),total=len(unique_forecast_df),desc='Daily Means'):
        
        current_nwm_forecast_df = get_forecast(row)
        
        if ii == unique_dict[current_date]:

            daily_nwm_forecast_df,current_date,daily_mean_forecast_files,final_forecast_df = update_daily(daily_nwm_forecast_df,current_date,daily_mean_forecast_files,final_forecast_df)

        if current_nwm_forecast_df is not None:
            
            if daily_nwm_forecast_df is None:
                ii = 0
                daily_nwm_forecast_df = pd.DataFrame(np.empty((len(current_nwm_forecast_df),unique_dict[current_date])))
                daily_nwm_forecast_df.set_index(current_nwm_forecast_df.index,inplace=True,drop=True)
            
            daily_nwm_forecast_df.loc[:,ii] = current_nwm_forecast_df.discharge
            #daily_nwm_forecast_df.rename(columns={ii:current_date_time})
            
            ii += 1
        
        current_date_time = row['date_time']
        current_date = current_date_time.date()

    daily_nwm_forecast_df,current_date,daily_mean_forecast_files,final_forecast_df = update_daily(daily_nwm_forecast_df,current_date,daily_mean_forecast_files,final_forecast_df)
       

    if forecast_output_file is not None:
        #final_forecast_df.to_csv(forecast_output_file,index=False,date_format='%Y-%m-%d %H:%M:%S%Z')
        final_forecast_df.to_csv(forecast_output_file,index=False,date_format='%Y-%m-%d')



def construct_nwm_forecast_filenames_and_retrieve(forecast_df,download_directory,retrieve=True,workers=1):
    # make forecast file names for NWM and retrieve

    #construct url
    #url = f'{year}/{year}{month}{day}{time}.CHRTOUT_DOMAIN1.comp'

    make_url = lambda df:  "https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/model_output/" + \
                                          str(df['date_time'].year) + '/' + str(df['date_time'].year) + \
                                           str(df['date_time'].month).zfill(2) + \
                                           str(df['date_time'].day).zfill(2) + \
                                           str(df['date_time'].hour).zfill(2) + \
                                           str(df['date_time'].minute).zfill(2) +\
                                           ".CHRTOUT_DOMAIN1.comp"
        
    def make_file_names(df,download_directory):
        # assumes that the last segment after the / represents the file name
        url = df['forecast_url']
        file_name_start_pos = url.rfind("/") + 1
        file_name = url[file_name_start_pos:]

        file_name = os.path.join(download_directory,file_name)

        return(file_name)


    urls = forecast_df.apply(make_url,axis=1)
    forecast_df['forecast_url'] = urls

    file_names = forecast_df.apply(lambda df: make_file_names(df,download_directory),axis=1)
    forecast_df['forecast_file'] = file_names

    if not retrieve:
        return(forecast_df)

    download_df = forecast_df[['forecast_url','forecast_file']].drop_duplicates()


    def download_url(url,file_name):
        r = requests.get(url, stream=True)
        if r.status_code == requests.codes.ok:
             with open(file_name, 'wb') as f:
                for data in r:
                    f.write(data)
        return url

    pool = ThreadPoolExecutor(max_workers=workers)
    
    results = { pool.submit(download_url,*(url,file_name)) : (url,file_name) for idx,(url,file_name) in download_df.iterrows() }

    for future in tqdm(as_completed(results),total=len(download_df),desc='Acquiring NWM forecasts'):

        url,file_name = results[future]

        try:
            future.result()
        except Exception as exc:
            print('error',exc,url)

    pool.shutdown(wait=True)
    
    return(forecast_df)


if __name__ == '__main__':


    ##################################
    ##
    ## Likely Deprecated: File appears to be no longer used. Noticed Jan 16, 2023
    ## Might want to be kept for possible re-use at a later time?
    ##
    ##################################

    # parse arguments
    parser = argparse.ArgumentParser(description='Find hucs for bounding boxes')
    parser.add_argument('-b','--bounding-boxes-file', help='Bounding box file', required=True)
    parser.add_argument('-w','--wbd', help='WBD file', required=True)
    parser.add_argument('-f','--forecast-output-file', help='Forecast file', required=False,default=None)
    parser.add_argument('-r','--retrieve', help='Forecast file', required=False,default=False,action='store_true')
    parser.add_argument('-j','--workers', help='Forecast file', required=False,default=1,type=int)
    parser.add_argument('-d','--download_directory', help='Forecast file', required=False,default=1)
    parser.add_argument('-m','--daily-mean-forecast-files', help='Daily Mean Forecast file', required=False,default=None)

    args=vars(parser.parse_args())

    cygnss_preprocessing(**args)
