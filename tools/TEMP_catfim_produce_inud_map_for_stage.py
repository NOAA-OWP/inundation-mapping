#!/usr/bin/env python3

import numpy as np
import os
import rasterio

def produce_inundation_map_with_stage_and_feature_ids(
    rem_path, catchments_path, hydroid_list, hand_stage, lid_directory, category, huc, lid, branch
):
    # Open rem_path and catchment_path using rasterio.
    
    print()
    print("+++++++++++++++++++++++")
    print(f"At the start of producing inundation maps for {huc}")
    print(locals())
    print("+++++++++++++++++++++++")    
    print()
    
    rem_src = rasterio.open(rem_path)
    catchments_src = rasterio.open(catchments_path)
    rem_array = rem_src.read(1)
    catchments_array = catchments_src.read(1)

    # Use numpy.where operation to reclassify rem_path on the condition that the pixel values
    #   are <= to hand_stage and the catchments value is in the hydroid_list.
    reclass_rem_array = np.where((rem_array <= hand_stage) & (rem_array != rem_src.nodata), 1, 0).astype(
        'uint8'
    )
    hydroid_mask = np.isin(catchments_array, hydroid_list)
    target_catchments_array = np.where(
        (hydroid_mask is True) & (catchments_array != catchments_src.nodata), 1, 0
    ).astype('uint8')
    masked_reclass_rem_array = np.where(
        (reclass_rem_array == 1) & (target_catchments_array == 1), 1, 0
    ).astype('uint8')

    # Save resulting array to new tif with appropriate name. brdc1_record_extent_18060005.tif
    is_all_zero = np.all((masked_reclass_rem_array == 0))
    
    # TODO: How can we get all zeros??
    
    print(f"is_all_zero is {is_all_zero}")

    if not is_all_zero:
        output_tif = os.path.join(
            lid_directory, lid + '_' + category + '_extent_' + huc + '_' + branch + '.tif'
        )
        print(f" +++ Output_Tif is {output_tif}")
        with rasterio.Env():
            profile = rem_src.profile
            profile.update(dtype=rasterio.uint8)
            profile.update(nodata=10)

            with rasterio.open(output_tif, 'w', **profile) as dst:
                dst.write(masked_reclass_rem_array, 1)

if __name__ == '__main__':
    
    rem_path = '/outputs/dev-bridge-pnts/02040101/branches/0/rem_zeroed_masked_0.tif'
    catchments_path = '/outputs/dev-bridge-pnts/02040101/branches/0/gw_catchments_reaches_filtered_addedAttributes_0.tif'
    hydroid_list =[10740168, 10740169, 10740181, 10740180, 10740170, 10740167, 10740179, 10740165, 10740166, 10740172, 10740174, 10740178, 10740175, 10740177, 10740171, 10740176]
    hand_stage = 7.669046000000037
    lid_directory ='/data/catfim/rob_test/test_3_stage_based/mapping/02040101/waln6'
    category = 'major_19p5ft'
    huc ='02040101' 
    lid ='waln6' 
    branch ='0'
    
    produce_inundation_map_with_stage_and_feature_ids(
    rem_path, catchments_path, hydroid_list, hand_stage, lid_directory, category, huc, lid, branch
)