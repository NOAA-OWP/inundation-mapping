#!/bin/bash -e

#This files pushs all support files required for a HV production release,

#When running this script, ensure the docker container is mounted to the dev_fim_share folders

#Note: This files is rather unpolished and as it is Git, it must not included the
#base HV S3 folder name or AWS credentials.

usage ()
{
    echo 'Copying key FIM production run output files to HV.'
    echo 'Does not included the filtered HAND files from FIM.'
    echo 'Does included key files such as catfim, fim performance and sierra test files.'
    echo 'Note: Please look at the code as there are assumed directories and pathing.'
    echo '      Also.. for now, please pre-run the aws configure script for creds.'    
    echo
    echo 'Usage : push-hv-data-support-files.sh [REQ: -n <run name> ]'
    echo '                                      [REQ: -hv <HV S3 bucket name>'
    echo ''
    echo 'REQUIRED:'
    echo '  -n/--runName       : A name of HAND output release.'
    echo '  -hv/--HV_S3_bucket : The root HV S3 bucket name. ie) s3://{hv....}' 
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo
    exit
}

while [ "$1" != "" ]; do
case $1
in
    -n|--runName)
        shift
        runName=$1
        ;;
    -hv|--HV_S3_bucket)
        shift
        HV_S3_bucket=$1
        ;;
    -h|--help)
        shift
        usage
        ;;
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$runName" = "" ]
then
    echo "ERROR: Missing -n  -- run time name (fim version / folder name) argument"
    usage
fi

if [ "$HV_S3_bucket" = "" ]
then
    echo "ERROR: Missing -hv  -- the HV s3 bucket name (ie: s3://{hv...})"
    usage
fi

# FOR NOW, run the aws configure script by hand outside this tool
read -p "Did you run aws configure and load creds already (y/n): " resp

case $resp in
    y ) echo ok, we wil proceed;;
    n ) echo exiting...;
         exit;;
    * ) echo invalid response;
         exit 1;;

esac

echo "Loading files to $HV_S3_bucket"

echo "Copying catfim files"

aws_cmd_cp="aws s3 cp"

# ----------------------
# Catfim Stage Based
aws_src="/data/catfim/"$runName"_stage_based/mapping/stage_based_catfim.csv"
aws_target=" "$HV_S3_bucket"/"$runName"/qa_datasets/"

aws_cmd="${aws_cmd_cp} ${aws_src} ${aws_target}"
echo "command is '"$aws_cmd"'"
eval "${aws_cmd}"
echo 


aws_src="/data/catfim/"$runName"_stage_based/mapping/stage_based_catfim_sites.csv"
aws_target=" "$HV_S3_bucket"/"$runName"/qa_datasets/"

aws_cmd="${aws_cmd_cp} ${aws_src} ${aws_target}"
echo "command is '"$aws_cmd"'"
eval "${aws_cmd}"
echo 

# ----------------------
# Catfim Flow Based
aws_src="/data/catfim/"$runName"_flow_based/mapping/flow_based_catfim.csv"
aws_target=" "$HV_S3_bucket"/"$runName"/qa_datasets/"

aws_cmd="${aws_cmd_cp} ${aws_src} ${aws_target}"
echo "command is '"$aws_cmd"'"
eval "${aws_cmd}"
echo 


aws_src="/data/catfim/"$runName"_flow_based/mapping/flow_based_catfim_sites.csv"
aws_target=" "$HV_S3_bucket"/"$runName"/qa_datasets/"

aws_cmd="${aws_cmd_cp} ${aws_src} ${aws_target}"
echo "command is '"$aws_cmd"'"
eval "${aws_cmd}"
echo 

# ----------------------
# Fim Performance
aws_src="/data/fim_performance/"$runName"/fim_performance_polys.csv"
aws_target=" "$HV_S3_bucket"/"$runName"/qa_datasets/"

aws_cmd="${aws_cmd_cp} ${aws_src} ${aws_target}"
echo "command is '"$aws_cmd"'"
eval "${aws_cmd}"
echo 


aws_src="/data/fim_performance/"$runName"/fim_performance_points.csv"
aws_target=" "$HV_S3_bucket"/"$runName"/qa_datasets/"

aws_cmd="${aws_cmd_cp} ${aws_src} ${aws_target}"
echo "command is '"$aws_cmd"'"
eval "${aws_cmd}"
echo 


aws_src="/data/fim_performance/"$runName"/fim_performance_catchments.csv"
aws_target=" "$HV_S3_bucket"/"$runName"/qa_datasets/"

aws_cmd="${aws_cmd_cp} ${aws_src} ${aws_target}"
echo "command is '"$aws_cmd"'"
eval "${aws_cmd}"
echo 

# ----------------------
# Sierra Test (Rating Curve Comparison Metrics)
aws_src="/data/fim_performance/"$runName"/rating_curve_comparison/agg_nwm_recurr_flow_elev_stats_location_id.csv"
aws_target=" "$HV_S3_bucket"/"$runName"/qa_datasets/"

aws_cmd="${aws_cmd_cp} ${aws_src} ${aws_target}"
echo "command is '"$aws_cmd"'"
eval "${aws_cmd}"
echo 


aws_src="/data/inputs/usgs_gages/usgs_rating_curves.csv"
aws_target=" "$HV_S3_bucket"/"$runName"/qa_datasets/"

aws_cmd="${aws_cmd_cp} ${aws_src} ${aws_target}"
echo "command is '"$aws_cmd"'"
eval "${aws_cmd}"
echo 


echo "done coping files and folders to S3"
echo
