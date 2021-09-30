#!/bin/bash -e

# ============================
function Show_Error() {
	echo -e '\n*** ERROR: '$1'  ***'
}

# ============================
function __Validate_Manditory_Args {
	# print usage if arguments empty
	if [ "$hucList" = "" ]
	then
		Show_Error 'missing -u/--hucList argument'
		usage
	fi

	if [ "$extent" = "" ]
	then
		Show_Error 'missing -e|--extent argument'
		usage
	fi

	if [ "$envFile" = "" ]
	then
		Show_Error 'missing -c|--configFile argument'
		usage
	fi

	if [ "$runName" = "" ]
	then
		Show_Error 'missing -c|--configFile argument'
		usage
	fi
}


# ============================
# Validating step_start_number and step_end_number
function __Validate_Step_Numbers() {
	echo '-----------------------------'
	echo '-- __Validate_Step_Numbers'
	echo 'params: $@'
	echo '-----------------------------'
	usage
}

# ============================
# This is where the function calls are made. 
# If an error is found, it will take care of messaging and exit if/as applicable.

__Validate_Manditory_Args
__Validate_Step_Numbers
