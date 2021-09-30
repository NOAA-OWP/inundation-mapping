#!/bin/bash -e

# ============================
function __Validate_Manditory_Args {
	
	# ------------------------------------
	if [ "$hucList" = "" ]
	then
		Show_Error 'missing -u/--hucList argument'
		usage
	fi

	# ------------------------------------
	if [ "$extent" = "" ]
	then
		Show_Error 'missing -e|--extent argument'
		usage
	fi

	# ------------------------------------
	if [ "$envFile" = "" ]
	then
		Show_Error 'missing -c|--configFile argument'
		usage
	fi

	# ------------------------------------
	if [ "$runName" = "" ]
	then
		Show_Error 'missing -n|--runName argument'
		usage
	fi

}

# ============================
# Validating step_start_number and step_end_number
function __Validate_Step_Numbers() {

	# -----------------
	Check_IsNumber $step_start_number
    if [ $value_Is_Number -eq 1 ] ; then
		step_start_number=0
    fi

	# -----------------
	Check_IsNumber $step_end_number	
    if [ $value_Is_Number -eq 1 ] ; then
		step_end_number=99
    fi
	
	# -----------------
    # Check if start number is a greater than end date, change end date to 99
    if [ $step_start_number -gt $step_end_number ] ; then
		step_end_number=99
    fi

}


# ============================
function __Validate_ArgValues_HucList {

	Check_IsNumber $hucList
    if [ $value_Is_Number -eq 1 ]
	then  #false
		# check to see if the path exists
		echo 'check path'
		
		
	else
		echo 'check length'
		if [[ $hucList -lt 4 ] || [ $hucList -gt 8 ]]
		then
			Show_Error '-u|--hucList argument: A HUC number was submitted but it needs to be 4 to 8 digits.'
			exit
		fi		
    fi	
}

# ============================
# This is where the function calls are made. 
# If an error is found, it will take care of messaging and exit if/as applicable.

source $srcDir/bash_functions.env

__Validate_Manditory_Args
__Validate_ArgValues
__Validate_Step_Numbers
