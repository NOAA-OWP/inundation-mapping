#!/bin/bash -e

# ============================
function __Validate_Manditory_Args_Exist {
	
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
    if [ $value_Is_Number -eq 1 ]
	then # false
		step_start_number=0
    fi

	# -----------------
	Check_IsNumber $step_end_number	
    if [ $value_Is_Number -eq 1 ]
	then # false	
		step_end_number=99
    fi
	
	# -----------------
    # Check if start number is a greater than end date, change end date to 99
    if [ $step_start_number -gt $step_end_number ] 
	then # false	
		step_end_number=99
    fi

}


# ============================
 function __Validate_ArgValues {

	#--------------------------------------------
	# extent (-e/--extent)
	#     check to see if the value of 'MS' or 'FS' (we will correct for case)
	if [ $extent != "" ] && [ ! Check_IsNumber $extent ]
	then
		$extent=$extent^^
		if [ $extent != "MR" ] && [ $extent != "FS" ] ; then
			Show_Error '-e/--extent must be the value of MS or FR.'
			usage
		fi
	fi


	#--------------------------------------------
	# envFile (c/--config)
	#     check to see if the path exists
	Check_IsFileExists $envFile
	if [ $value_Is_Number -eq 1 ] # false
	then
		Show_Error 'c/--config argument: The file name does not appear to exist. Check path, spelling and path.'
		usage
	fi
	
	
}

# ============================


# ============================
# This is where the function calls are made. 
# If an error is found, it will take care of messaging and exit if/as applicable.

source $srcDir/bash_functions.env

__Validate_Manditory_Args_Exist

# huc inputs are handled by another script
huc_input_validation_output=$( python3  $srcDir/check_huc_inputs.py -u "$hucList")
if [ "$huc_input_validation_output" != "" ] ; then
	Show_Error
	echo "$huc_input_validation_output"
	usage
fi

__Validate_ArgValues
__Validate_Step_Numbers

exit
