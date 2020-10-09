#!/bin/bash -e
:
usage ()
{
    echo 'Produce FIM datasets'
    echo 'Usage : fim_run.sh [REQ: -d <inputdir> -o <outputdir> -n <paramfile> -n <paramfolder>] [OPT: -h]'
    echo ''
    echo 'REQUIRED:'
    echo '  -d/--indir    : initial run directory with default mannings values'
    echo '  -t/--huclist    : huc or list of hucs'
    echo '  -g/--outdir     : directory for output mannings parameter adjustment runs'
    echo '  -n/--paramfile    : parameter set file'
    echo '  -f/--paramfolder    : parameter set file path'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    exit
}

if [ "$#" -lt 8 ]
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
    -t|--huclist )
        shift
        huclist=$1
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
    -j|--jobLimit)
        shift
        jobLimit=$1
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

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=1
fi


export paramfolder=$paramfolder
export outdir=$outdir
mkdir -p $outdir/logs

for huc in $huclist
do

  # hucdir="/data/outputs/"$indir/$huc
  export huc=$huc
  # export hucdir=$hucdir
  export indir=$indir
  export hucdir="/data/outputs/"$indir/$huc
  # count=1

  ## RUN ##
  if [ -f "$paramfile" ]; then
      if [ "$jobLimit" -eq 1 ]; then
          parallel --verbose --lb  -j $jobLimit -- $libDir/mannings_calibration.sh :::: $paramfile
      else
          parallel --eta -j $jobLimit -- $libDir/mannings_calibration.sh :::: $paramfile
      fi
  else
      if [ "$jobLimit" -eq 1 ]; then
          parallel --verbose --lb -j $jobLimit -- $libDir/mannings_calibration.sh ::: $paramfile
      else
          parallel --eta -j $jobLimit -- $libDir/mannings_calibration.sh ::: $paramfile
      fi
  fi


  # while read p; do
  #
  #   # Generate mannings_table.json
  #   python3 foss_fim/config/create_mannings_table.py -d "$p" -f $paramfolder/mannings_template.json
  #
  #   str_ord=$((($count-1)/17 +1))
  #   echo "$str_ord"
  #   subdir=$paramfolder/$outdir/$huc"_""$str_ord""_""$count"
  #   mkdir -p $subdir
  #   echo "$subdir"
  #   $libDir/add_crosswalk.py $hucdir/gw_catchments_reaches_filtered_addedAttributes.gpkg $hucdir/demDerived_reaches_split_filtered.gpkg $hucdir/src_base.csv $hucdir/majority.geojson $subdir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg $subdir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg $subdir/src_full_crosswalked.csv $subdir/src.json $subdir/crosswalk_table.csv $subdir/hydroTable.csv $hucdir/wbd8_clp.gpkg $hucdir/nwm_subset_streams.gpkg $paramfolder/mannings_template.json
  #
  #   count=`expr $count + 1`
  #
  #   python3 foss_fim/tests/run_test_case_calibration.py -r $indir/$huc -d $subdir -t $huc"_ble" -b $subdir

    # python3 foss_fim/tests/calc_obj_func.py -d data/test_cases/performance_archive/development_versions/$subdir -o 'data/temp/mannings_test.txt' -p $count

  # done <$paramfolder/$paramfile
done
