#!/bin/bash -e
:
usage ()
{
    echo "Calibrate FIM based on Manning's n values"
    echo 'Usage : fim_run.sh [REQ: -d <fimdir> -t <huclist> -g <outdir> -n <paramfile>] [OPT: -h]'
    echo ''
    echo 'REQUIRED:'
    echo '  -d/--fimdir    : initial run directory with default mannings values'
    echo '  -t/--huclist    : huc or list of hucs'
    echo '  -g/--outdir     : output directory for mannings parameter adjustment files'
    echo '  -n/--paramfile    : parameter set file'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    exit
}

if [ "$#" -lt 7 ]
then
  usage
fi

while [ "$1" != "" ]; do
case $1
in
    -d|--fimdir)
        shift
        fimdir="$1"
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
if [ "$fimdir" = "" ]
then
    usage
fi

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=1
fi

export input_NWM_Catchments=$inputDataDir/nwm_hydrofabric/nwm_catchments.gpkg
export outdir=$outdir
export toolsdir="/foss_fim/tools"

if [ -f "$huclist" ]; then

  while read huc; do

    export huc=$huc
    export fimdir=$fimdir
    export hucdir="/data/outputs/"$fimdir/$huc

    ## RUN ##
    if [ -f "$paramfile" ]; then
        if [ "$jobLimit" -eq 1 ]; then
            parallel --verbose --lb  -j $jobLimit -- $toolsdir/time_and_tee_mannings_calibration.sh :::: $paramfile
        else
            parallel --eta -j $jobLimit -- $toolsdir/time_and_tee_mannings_calibration.sh :::: $paramfile
        fi
    else
        if [ "$jobLimit" -eq 1 ]; then
            parallel --verbose --lb -j $jobLimit -- $toolsdir/time_and_tee_mannings_calibration.sh ::: $paramfile
        else
            parallel --eta -j $jobLimit -- $toolsdir/time_and_tee_mannings_calibration.sh ::: $paramfile
        fi
    fi
  done <$huclist

else

  for huc in $huclist
  do
    export huc=$huc
    export fimdir=$fimdir
    export hucdir="/data/outputs/"$fimdir/$huc

    ## RUN ##
    if [ -f "$paramfile" ]; then
        if [ "$jobLimit" -eq 1 ]; then
            parallel --verbose --lb  -j $jobLimit -- $toolsdir/time_and_tee_mannings_calibration.sh :::: $paramfile
        else
            parallel --eta -j $jobLimit -- $toolsdir/time_and_tee_mannings_calibration.sh :::: $paramfile
        fi
    else
        if [ "$jobLimit" -eq 1 ]; then
            parallel --verbose --lb -j $jobLimit -- $toolsdir/time_and_tee_mannings_calibration.sh ::: $paramfile
        else
            parallel --eta -j $jobLimit -- $toolsdir/time_and_tee_mannings_calibration.sh ::: $paramfile
        fi
    fi
  done
fi
