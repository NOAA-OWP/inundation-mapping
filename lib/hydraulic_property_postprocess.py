#!/usr/bin/env python3

# calculate extended hydraulic properties from the basic properties
# derived from CatchHydroGeo
# Yan Y. Liu <yanliu@illinois.edu>
# 10/31/2016

import pandas as pd
import sys

# usage: python thisscript hydroprop.csv 0.05 hydropropfull.csv
def main():
    hydropropotxt = str(sys.argv[1])
    manning_n = str(sys.argv[2])
    handpropotxt = str(sys.argv[3])
    df_result = pd.read_csv(hydropropotxt, dtype= object )
    # {'CatchId': int, ' Stage': float, ' Number of Cells': int,
                                                   #' SurfaceArea (m2)' : float, ' BedArea (m2)' : float, ' Volume (m3)' : float,
                                                   #' SLOPE' : float, ' LENGTHKM' : float, ' AREASQKM' : float}
    # CatchId, Stage, Number of Cells, SurfaceArea (m2), BedArea (m2), Volume (m3), SLOPE, LENGTHKM, AREASQKM
    #{'a': int, 'b': float, 'c': int, 'd' : float, 'e' : float, 'f' : float, 'g' : float, 'h' : float, 'i' : float}
    df_result['Roughness'] = float(manning_n)
    #df_result = df_result.drop('COMID', 1)
    df_result = df_result.rename(columns=lambda x: x.strip(" "))
    # print(df_result.iloc[59200,:])
    # print(df_result.iloc[59262,:])
    # exit()
    df_result = df_result.apply(pd.to_numeric,**{'errors' : 'coerce'})
    df_result['TopWidth (m)'] = df_result['SurfaceArea (m2)']/df_result['LENGTHKM']/1000
    df_result['WettedPerimeter (m)'] = df_result['BedArea (m2)']/df_result['LENGTHKM']/1000
    df_result['WetArea (m2)'] = df_result['Volume (m3)']/df_result['LENGTHKM']/1000
    df_result['HydraulicRadius (m)'] = df_result['WetArea (m2)']/df_result['WettedPerimeter (m)']
    df_result['HydraulicRadius (m)'].fillna(0, inplace=True)
    df_result['Discharge (m3s-1)'] = df_result['WetArea (m2)']* \
    pow(df_result['HydraulicRadius (m)'],2.0/3)* \
    pow(df_result['SLOPE'],0.5)/df_result['Roughness']

    # set nans to 0
    df_result.loc[df_result['Stage']==0,['Discharge (m3s-1)']] = 0

    df_result.to_csv(handpropotxt,index=False)


if __name__ == "__main__":
    main()
