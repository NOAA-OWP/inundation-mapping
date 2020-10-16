#!/bin/bash -e
:
usage ()
{
    echo 'Produce FIM datasets'
    echo 'Usage : fim_run.sh [REQ: -d <inputdir> -o <outputdir> -n <paramfile>] [OPT: -h]'
    echo ''
    echo 'REQUIRED:'
    echo '  -d/--indir    : initial run directory with default mannings values'
    echo '  -t/--huclist    : huc or list of hucs'
    echo '  -g/--outdir     : directory for output mannings parameter adjustment runs'
    echo '  -n/--paramfile    : parameter set file'
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

export outdir=$outdir
export testdir="/foss_fim/tests"

while read huc; do

  export huc=$huc
  export indir=$indir
  export hucdir="/data/outputs/"$indir/$huc

  ## RUN ##
  if [ -f "$paramfile" ]; then
      if [ "$jobLimit" -eq 1 ]; then
          parallel --verbose --lb  -j $jobLimit -- $testdir/time_and_tee_mannings_calibration.sh :::: $paramfile
      else
          parallel --eta -j $jobLimit -- $testdir/time_and_tee_mannings_calibration.sh :::: $paramfile
      fi
  else
      if [ "$jobLimit" -eq 1 ]; then
          parallel --verbose --lb -j $jobLimit -- $testdir/time_and_tee_mannings_calibration.sh ::: $paramfile
      else
          parallel --eta -j $jobLimit -- $testdir/time_and_tee_mannings_calibration.sh ::: $paramfile
      fi
  fi
done <$huclist
