#!/bin/bash -e
:
usage ()
{
    echo 'Produce FIM datasets'
    echo 'Usage : ble_auto_eval.sh [REQ: -f <fim outfolder(s)> -b <ble list> -d <current dev> -s <outfolder> -v <version>] [OPT: -h -j <job limit>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -f/--fim-outfolder     : fim output directory(s)'
    echo '  -b/--ble-list          : list of ble sites to evaluate'
    echo '  -d/--current-dev       : current archived dev stats column name'
    echo '  -s/--outfolder         : outfolder name'
    echo '  -v/--version           : version eval results. options are Options: "DEV" or "PREV"'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    exit
}

if [ "$#" -lt 7 ]
then
  usage
fi

while [ "$1" != "" ]; do
case $1
in
    -f|--fim_outfolder)
        shift
        fim_outfolder="$1"
        ;;
    -b|--ble_list)
        shift
        ble_list="$1"
        ;;
    -d|--current_dev)
        shift
        current_dev="$1"
        ;;
    -s|--outfolder)
        shift
        outfolder="$1"
        ;;
    -v|--version)
        shift
        version="$1"
        ;;
    -j|--jobLimit)
        shift
        jobLimit=$1
        ;;
    -h|--help)
        shift
        usage
        ;;
    esac
    shift
done

export testDir='foss_fim/tests'


for branch in $fim_outfolder
do
  echo "processing feature branch: $branch"

  while read p; do
      # Run Eval
      if [ -d "data/outputs/$branch/$p" ]
      then
        echo "processing ble for $branch/$p"
        python3 $testDir/run_test_case.py -r $branch/$p -t $p"_ble" -b $branch -c
      fi

      if [ -d "data/outputs/$branch/$(echo $p| cut -b 1-6)" ]
      then
        echo "processing ble for $branch/$(echo $p| cut -b 1-6)"
        python3 $testDir/run_test_case.py -r $branch/$(echo $p| cut -b 1-6) -t $p"_ble" -b $branch -c
      fi
  done <$ble_list
done

echo "combining ble metrics"
python3 $testDir/all_ble_stats_comparison.py -b $ble_list -e "$fim_outfolder" -d $current_dev -f $outfolder

echo "calculating aggregate metrics"
python3 $testDir/aggregate_metrics.py -c $version -b "$fim_outfolder" -u $ble_list -f $outfolder
