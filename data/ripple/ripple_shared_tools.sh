#!/bin/bash -e

function calc_Duration() {
    local start_time=$1
    local end_time=`date +%s`

    local total_sec=$(( $end_time - $start_time ))
    local dur_min=$((total_sec / 60))
    local dur_remainder_sec=$((total_sec %60))
    local dur_sec_percent=$((100*$dur_remainder_sec/60))

    printf -v dur "${dur_min}.${dur_sec_percent}"
    echo $dur
    # echo -e "${dur_min}.${dur_sec_percent}"
    # return "${dur_min}.${dur_sec_percent}"
    # printf -v dur '${dur_min}.${dur_sec_percent}'
}

function Setup_logging() {

    local log_file_path="$1"
    # echo "the setup log path is ${log_file_path}"
    if [ ! -f $log_file_path ]; then
        touch $log_file_path
    else
        rm -f $log_file_path
    fi
}

## Simple logging system
# This will print to screen and add to log file. Just use echo if you want screen only.
#   July 2025, basic pattern copied from /src/bash_functions for now due to 
#   pathing and encapsulation, but upgraded
function l_echo() {
    local msg="$1"
    local log_file_path=$2

    printf -v current_datetime '%(%Y%m%d-%H:%M:%S)T' -1
    adj_msg="$current_datetime - ${msg}"
    echo -e "$adj_msg" ; echo -e "$adj_msg" >> $log_file_path
}

# trims spaces both sides plus an ending slash if one exists
function Trim_spaces() {
    local orig_value="$1"

    # trim spaces (end first, then front)
    rtn_value="${orig_value##*[[:space:]]}"
    rtn_value="${rtn_value%%*([[:space:]])}"
    rtn_value="${rtn_value%/}"
    echo -e "$rtn_value"
}