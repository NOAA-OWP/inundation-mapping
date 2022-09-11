#!/usr/bin/env python3

import argparse
import os

from osgeo import gdal

def create_vrt_file(vrt_file_name, src_dir, tif_files = ''):
    
    '''
    Process
    
    Parameters
    '''
    
    #vrt_options = gdal.BuildVRTOptions(resampleAlg='cubic', addAlpha=True)
    #gdal.BuildVRT('my.vrt', ['one.tif', 'two.tif'], options=vrt_options)
    
    input_files = ['/data/inputs/usgs/3dep_dems/10m_huc8/HUC8_01040001_dem.tif',
                   '/data/inputs/usgs/3dep_dems/10m_huc8/HUC8_01040002_dem.tif']
    output_vrt_name = "/data/inputs/usgs/3dep_dems/10m_huc8/FIM_Seamless_DEM_10m.vrt"
    
    result = gdal.BuildVRT(output_vrt_name, input_files)
    print(result)

    print("done")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='AAAA')

    # TODO: Add param to allow for multiples
    '''
    parser.add_argument('-f','--tif_files', help='a list of just .tif files names.'\
                        'If empty, then all files in a stated directory will be used', 
                        required=False)
    
    parser.add_argument('-n','--vrt_file_name', help='Name of the vrt file to be created. ' \
                        'Note: it will be created in the source directory', 
                        required=True)
    
    parser.add_argument('-s','--src_directory', help='A directory of where the .tif files '\
                        'files exist. If the -f (tif-file) param is empty then all .tif files '\
                        'in this directory will be used.', 
                        required=True)
    '''
    create_vrt_file('','')
