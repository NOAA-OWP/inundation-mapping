import pandas as pd
import rasterio
import numpy as np

def htable_comparison(huc):
    # Read CSVs
    df1 = pd.read_csv(r"%s/%s/hydrotable.csv"%(run1_path,huc), dtype={"branch_id": str, 'HydroID':str}, usecols=['branch_id','HydroID','discharge_cms','stage'])
    df2 = pd.read_csv(r"%s/%s/hydrotable.csv"%(run2_path,huc),dtype={"branch_id": str, 'HydroID':str}, usecols=['branch_id','HydroID','discharge_cms','stage'])

    merged = pd.merge(
        df1[["HydroID", "discharge_cms", "stage","branch_id"]],
        df2[["HydroID", "discharge_cms", "stage","branch_id"]],
        on=["HydroID","branch_id", "stage"],
        suffixes=("_file1", "_file2")
    )


    # Compute % difference in discharge
    merged["flow_diff_pct"] = (
        (merged["discharge_cms_file1"] - merged["discharge_cms_file2"]).abs()
        / merged["discharge_cms_file1"].replace(0, pd.NA) * 100
    )

    # Filter where > 5%
    diff_gt5 = merged[merged["flow_diff_pct"] > 0.1]
    if not diff_gt5.empty:
        print("HydroIDs with >5 flow difference:")
        # print("branch %s: HydroIDs with >5 flow difference:"%branch)
        print(diff_gt5[["HydroID","branch_id", "discharge_cms_file1", "discharge_cms_file2", "stage", "flow_diff_pct"]])
        print('branches with error:',diff_gt5["branch_id"].unique())
    else:
        print("all good")
        # print("all good in branch %s"%branch)

def compare_fims(huc):
    r1 = "%s/post/fim_%s.tif"%(run1_path,huc)
    r2 = "%s/post/fim_%s.tif"%(run2_path,huc)

    # Open both rasters
    with rasterio.open(r1) as src1, rasterio.open(r2) as src2:
        # 1. Compare metadata (extent, resolution, CRS, etc.)
        if src1.crs != src2.crs:
            print("Different CRS")
        if src1.transform != src2.transform:
            print("Different geotransform")
        if src1.width != src2.width or src1.height != src2.height:
            print("Different dimensions")
        
        # 2. Read data into arrays
        arr1 = src1.read(1)  # first band
        arr2 = src2.read(1)

        # 3. Compare values
        if np.array_equal(arr1, arr2):
            print("Rasters are identical")
        else:
            # Optional: allow tolerance for floats
            if np.allclose(arr1, arr2, atol=1e-6, equal_nan=True):
                print("Rasters match within tolerance")
            else:
                diff = np.count_nonzero(arr1 != arr2)
                print(f"Rasters differ in {diff} cells")


run1_path='outputs/ZahraPR/Run5/pipeline/'
run2_path='outputs/ZahraPR/Run5/only_postprocessing/'


for huc in ['05100205','05030106','01070006','12030101']:
# for huc in ['05030106']:
    print(huc)
    htable_comparison(huc)
    # compare_fims(huc)



