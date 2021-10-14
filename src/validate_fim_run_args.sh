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
		Show_Error 'missing -n|--runName argument (or other arguments may be missing expected values). Please recheck your parameters.'
		usageMessage
	fi

}

# ============================
# Validating step_start_number and step_end_number
function __Validate_Step_Numbers() {

    # -----------------
	if ! [ "$step_start_number" == "" ]
	then
		Check_IsInteger $step_start_number
		if [ $value_Is_Number -eq 0 ]  
		then # false
			Show_Error '-ssn ... Step Start Number must be a number if argument is used.'
			usageMessage
		fi
	else
		step_start_number=1
	fi

    # -----------------
	if ! [ "$step_end_number" == "" ]
	then
		Check_IsInteger $step_end_number
		if [ $value_Is_Number -eq 0 ]  
		then # false
			Show_Error '-sen ... Step End Number must be a number if argument is used.'
			usageMessage
		fi
	else
		step_end_number=99
	fi
	
	# -----------------
    # Check if start number is a greater than end date, change end date to 99
    if [[ $step_start_number -gt $step_end_number ]]
	then # false
		Show_Error '-ssn and -sen ... Step Start Number can not be greater than the Step End Number.'
		usageMessage
    fi
}

# ============================
 function __Validate_ArgValues {

	#--------------------------------------------
	# extent (-e/--extent)
	#     check to see if the value of 'MS' or 'FS' (we will correct for case)
	Check_IsInteger $extent  # We check so the change to uppercase doesn't fail
	if [ ! -z "$extent" ] && [ $value_Is_Number -eq 0 ]  # not empty and is not a number, then we can uppercase it
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
	# Also ensured first char is not a dash (meaning they missed a value for -j
	if [ -z "$jobLimit" ] || [ "${jobLimit::1}" = "-" ]
	then
		jobLimit=$default_max_jobs
	else
		Check_IsInteger $jobLimit
		if [ $value_Is_Number -eq 0 ]  
		then
			Show_Error '-j/--jobLimit: (Optional) argument value may be missing or is not a number.'
			usageMessage
		else
			# strip leading zeros if any
			jobLimit=$(echo $jobLimit | sed 's/^0*//')
			
			# if it was all zeros, we change it to 1
			if [ -z "$jobLimit" ]; then
				jobLimit=$default_max_jobs
			fi
		fi
	fi

	#-h/--help = no need for validation
	#-o/--overwrite = no need for validation
	# -p/--production  = no need for validation
	# -v/--viz  = no need for validation
	# -m/--mem  = no need for validation	

}

# ============================
# Validate that all of the file names that have been submitted (if any) are propertly formatted file names
# with extensions.
# The variable whitelist will be replaced at the end as it will trim whitespaces around the commas and each name
 function __Validate_Whitelist_Args {

	
	if [ ! -z "$whitelist" ]
	then
		# see if it is one file name or more than one seperated by comma'script
	
		IFS="," read -a fileNames <<< $whitelist
		
		ctr=0
		Adjusted_List_Files_Names=""  #rebuilt as trimming has been done
		for file_name in "${fileNames[@]}"
		do
			trimmed_file_name=$(echo $file_name | xargs)
			Check_Valid_File_Name_Pattern $trimmed_file_name
			if [ $is_Valid -eq 0 ] 
			then
				Show_Error '-w|--whitelist: One or more of the file names appears to be invalid. File Name:'$file_name
				usageMessage
			fi
			
			let "ctr+=1"
			Adjusted_List_Files_Names+=$trimmed_file_name
			
			if [ ${#fileNames[@]} -gt $ctr ]; then
				Adjusted_List_Files_Names+=","
			fi
		done
		
		# assign it back to the original variable, but it is now cleaned
		whitelist=$Adjusted_List_Files_Names
	fi
}

# ============================
# This is where the function calls are made. 
# If an error is found, it will take care of messaging and exit if/as applicable.

#source $srcDir/bash_functions.env

__Validate_Manditory_Args_Exist

huc_input_validation_output=$( python3  $srcDir/check_huc_inputs.py -u "$hucList")

if [ "$huc_input_validation_output" != "" ] 
then
	# Oct 2021: Yes.. this is ugly picking up the first chars of the StdOut from python (which is really StnOut)
	# but there is no other good way for now.
	if [[ $huc_input_validation_output == err:* ]]
	then
		Show_Error "$huc_input_validation_output"
		usageMessage
	elif [[ $huc_input_validation_output == HUCS:* ]]
	then
		# We will remove the string of "HUCS:" from the start of the string, 
		# then parse what is left over into an array of huc codes (comma seperated if more than one)
		str_huc_codes=$(echo $huc_input_validation_output | sed 's/HUCS://')
			
		# split to an array based on the comma (remember, might only be one item and no comma)
		IFS="," read -a hucCodes <<< $str_huc_codes
	fi
fi

# validate other args
__Validate_ArgValues
__Validate_Whitelist_Args
__Validate_Step_Numbers


