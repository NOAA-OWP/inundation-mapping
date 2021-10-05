#!/bin/bash -e

# ============================
# Also checks the first char of the value. If you submit somethign like this -e -n "some_name"...
#    notice the arg value is missing from -e... it will make the value of extent become "-n" (hence, invalid)
function __Validate_Manditory_Args_Exist {
	
	# ------------------------------------
	if [ "$hucList" = "" ] || [ "${hucList::1}" = "-" ]
	then
		Show_Error 'missing -u/--hucList argument'
		usageMessage
	fi

	# ------------------------------------
	if [ "$envFile" = "" ] || [ "${envFile::1}" = "-" ]
	then
		Show_Error 'missing -c|--configFile argument'
		usageMessage
	fi

	# ------------------------------------
	if [ "$extent" = "" ] || [ "${extent::1}" = "-" ]
	then
		Show_Error 'missing -e|--extent argument'
		usageMessage
	fi

	# ------------------------------------
	if [ "$runName" = "" ] || [ "${runName::1}" = "-" ]
	then
		Show_Error 'missing -n|--runName argument'
		usageMessage
	fi

}

# ============================
# Validating step_start_number and step_end_number
function __Validate_Step_Numbers() {

	# -----------------
	Check_IsInteger $step_start_number
    if [ $value_Is_Number -eq 1 ]
	then # false
		step_start_number=0
    fi

	# -----------------
	Check_IsInteger $step_end_number	
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
	Check_IsInteger $extent  # We check so the change to uppercase doesn't fail
	if [ "$extent" != "" ] && [ $value_Is_Number -eq 0 ]  # not empty and is not a number, then we can uppercase it
	then
		extent=${extent^^}  # Change to uppercase
		if [ "$extent" != "MS" ] && [ "$extent" != "FR" ] ; then
			Show_Error '-e/--extent must be the value of MS or FR.'
			usageMessage
		fi
	else
		Show_Error '-e/--extent must be the value of MS or FR.'
		usageMessage
	fi


	#--------------------------------------------
	# envFile (c/--config)
	#     check to see if the path exists
	Check_IsFileExists $envFile
	if [ $file_Exists -eq 0 ] # false
	then
		Show_Error 'c/--config argument: The file name does not appear to exist. Check path, spelling and path.'
		usageMessage
	fi

	#--------------------------------------------
	# -n/--runName
	Check_File_Folder_Name_Characters $runName
	if [ $is_Valid -eq 0 ] 
	then
		Show_Error '-n/--runName: Please use alpha-numeric or underscores only for the run name.'
		usageMessage
	fi
	
	#--------------------------------------------
	# -j/--jobLimit


	#--------------------------------------------
	# -h/--help (with added args)


	#--------------------------------------------
	# -o|--overwrite


	#--------------------------------------------
	# w|--whitelist


	#--------------------------------------------
	# -v|--viz (with added args)


	#--------------------------------------------
	# -m|--mem (with added args)

	
	
}

# ============================


# ============================
# This is where the function calls are made. 
# If an error is found, it will take care of messaging and exit if/as applicable.

source $srcDir/bash_functions.env

__Validate_Manditory_Args_Exist

# huc inputs are handled by another script
huc_input_validation_output=$( python3  $srcDir/check_huc_inputs.py -u "$hucList")
if [ "$huc_input_validation_output" != "" ] 
then
	Show_Error "$huc_input_validation_output"
	usageMessage
fi

# validate other args
__Validate_ArgValues
__Validate_Step_Numbers


