#!/bin/bash -e
:
usage ()
{
    echo 'Produce FIM datasets'
    echo 'Usage : fim_run.sh [REQ: -d <inputdir> -o <outputdir> -n <paramfile> -n <paramfolder>] [OPT: -h]'
    echo ''
    echo 'REQUIRED:'
    echo '  -d/--indir    : initial run directory with default mannings values'
    echo '  -t/--hucnumber    : '
    echo '  -g/--outdir     : directory for output mannings parameter adjustment runs'
    echo '  -n/--paramfile    : parameter set file'
    echo '  -f/--paramfolder    : parameter set file path'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    exit
}

if [ "$#" -lt 4 ]
then
  usage
fi

while [ "$1" != "" ]; do
case $1
in
    -d|--indir)
        shift
        indir="$1"
        ;;
    -g|--outdir )
        shift
        outdir=$1
        ;;
    -n|--paramfile)
        shift
        paramfile=$1
        ;;
    -f|--paramfolder)
        shift
        paramfolder=$1
        ;;
    -h|--help)
        shift
        usage
        ;;
    -o|--overwrite)
        overwrite=1
        ;;
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$indir" = "" ]
then
    usage
fi
# runtestcase_outfolder=mannings_test_calib
pathdir=$indir/$hucnumber
count=1
while read p; do
  echo "$p"
  # Generate mannings_table.json
  python3 foss_fim/config/create_mannings_table.py -d "$p" -f $paramfolder/mannings_template.json

  subdir=$outdir"_"$count
  mkdir -p $subdir

  $libDir/add_crosswalk.py $indir/gw_catchments_reaches_clipped_addedAttributes.gpkg $indir/demDerived_reaches_split_clipped.gpkg $indir/src_base.csv $indir/majority.geojson $subdir/gw_catchments_reaches_clipped_addedAttributes_crosswalked.gpkg $subdir/demDerived_reaches_split_clipped_addedAttributes_crosswalked.gpkg $subdir/src_full_crosswalked.csv $subdir/src.json $subdir/crosswalk_table.csv $subdir/hydroTable.csv $indir/wbd8_clp.gpkg $indir/nwm_subset_streams.gpkg $paramfolder/mannings_table.json
  count=`expr $count + 1`

  python3 foss_fim/tests/run_test_case.py -r $subdir -t $p"_ble" -b $subdir -c

  python3 foss_fim/tests/calc_obj_func.py -d data/test_cases/performance_archive/development_versions/$subdir -o 'data/temp/mannings_test.txt' -p $count

done <$paramfolder/$paramfile
