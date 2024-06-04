## Update BLE benchmark data

Downloads and preprocesses FEMA [Base Level Engineering](https://webapps.usgs.gov/infrm/estbfe/) (BLE)
benchmark datasets for purposes of evaluating FIM output. A benchmark dataset will be transformed using
properties (CRS, resolution) from an input reference dataset. The outputs will be a CSV table of
flows with [`feature_id`, `discharge`] columns and a boolean (True/False) TIF inundation extent raster
with inundated areas (True or 1) and dry areas (False or 0).

As the `reference_raster` is required for preprocessing, it is assumed that `fim_pipeline.py` has previously
been run for the HUCs being processed.

## Installation

1. The Docker image can be run using the following command:
```
docker run --rm -it -v ~/git/inundation-mapping:/foss_fim -v ~/efs/fim_data/:/data \
-v ~/efs/fim_data/fim_4_4_x_x/outputs:/outputs --name ble_benchmark dev:ble_benchmark {your docker image name}
```

## Usage
```
Create BLE benchmark files

options:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input-file INPUT_FILE
                        Input file (.xlsx or .csv) a version resides in inputs/ble/ble_benchmark
  -s SAVE_FOLDER, --save-folder SAVE_FOLDER
                        Output folder
  -r REFERENCE_FOLDER, --reference-folder REFERENCE_FOLDER
                        Reference folder
  -o BENCHMARK_FOLDER, --benchmark-folder BENCHMARK_FOLDER
                        Benchmark folder
  -n NWM_GEOPACKAGE, --nwm-geopackage NWM_GEOPACKAGE
                        NWM flows geopackage
  -u HUC, --huc HUC     Run a single HUC. If not supplied, it will run all HUCs in the input file.
  -xs BLE_XS_LAYER_NAME, --ble-xs-layer-name BLE_XS_LAYER_NAME
                        BLE cross section layer. Default layer is "XS" (sometimes it is "XS_1D").
  -l NWM_STREAM_LAYER_NAME, --nwm-stream-layer-name NWM_STREAM_LAYER_NAME
                        NWM streams layer. Default layer is "streams".
  -id NWM_FEATURE_ID_FIELD, --nwm-feature-id-field NWM_FEATURE_ID_FIELD
                        ID field for NWM streams. Not required if NWM v2.1 is used (default id field is "ID").
```

## Example
```
python /foss_fim/data/ble/ble_benchmark/create_ble_benchmark.py \
-i /data/inputs/ble/ble_benchmark/EBFE_urls_20230608.xlsx \
-s /data/temp/ble_benchmark \
-r /data/outputs/fim_4_3_12_0 \
-o /data/test_cases/ble_test_cases/validation_data_ble \
-n /data/inputs/nwm_hydrofabric/nwm_flows.gpkg \
-l streams \
-u 12090301
```

## Notes

If this is run without the `-u` option, it will run all HUCs in the input file and download nearly
1TB of data. If this is done, please notify Florence Thompson (fethomps@usgs.gov) so that the download usage
can be attributed.
